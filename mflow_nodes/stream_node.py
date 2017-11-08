from logging import getLogger

import os
from time import sleep

from mflow import mflow, Stream, zmq
from mflow.tools import ThroughputStatistics
from mflow_nodes.node_manager import NodeManager, NodeManagerProxy
from mflow_nodes.rest_api.rest_server import start_web_interface
from mflow_nodes import config
from mflow_nodes.stream_tools.mflow_message import get_mflow_message, get_raw_mflow_message

_logger = getLogger(__name__)


def start_stream_node(instance_name, processor, processor_parameters=None,
                      connection_address=None, control_host=None, control_port=None,
                      start_node_immediately=False, receive_raw=False):
    """
    Start the ZMQ processing node.
    :param instance_name: Name of the processor instance. Used for the REST api path.
    :param processor: Stream mflow_processor that does the actual work on the stream data.
    :type processor: StreamProcessor
    :param connection_address: Fully qualified ZMQ stream connection address. Default: "tcp://127.0.0.1:40000"
    :param control_host: Binding host for the control REST API.
    :param control_port: Binding port for the control REST API.
    :param start_node_immediately: If true, the external mflow_processor will be started at node startup.
    :param processor_parameters: List of arguments to pass to the string mflow_processor start command.
    :param receive_raw: Pass the raw ZMQ messages to the mflow_processor.
    :return: None
    """
    connection_address = connection_address or config.DEFAULT_CONNECT_ADDRESS
    control_host = control_host or config.DEFAULT_REST_HOST
    control_port = control_port or config.DEFAULT_REST_PORT

    _logger.debug("Node set to connect to '%s', with control address '%s:%s'." % (connection_address,
                                                                                  control_host,
                                                                                  control_port))

    _logger.debug("To start a client for this instance:\n\t%s"
                  % config.DEFAULT_CLIENT_INSTANCE.format(variable_name=instance_name,
                                                          address="%s:%s" % (control_host, control_port),
                                                          instance_name=instance_name))

    node_manager = NodeManager(processor_function=get_processor_function(processor=processor,
                                                                         connection_address=connection_address,
                                                                         receive_raw=receive_raw),
                               receiver_function=get_receiver_function(
                                   connection_address=connection_address,
                                   receive_raw=receive_raw),
                               initial_parameters=processor_parameters,
                               processor_instance=processor)

    # node_manager_proxy = NodeManagerProxy(node_manager)

    if start_node_immediately:
        # We on purpose do not catch the possible exceptions here.
        # If the node is started with start immediately, it should also immediately throw an exception.
        node_manager.start()

    # Attach web interface
    start_web_interface(instance_name=instance_name, process=node_manager,
                        host=control_host, port=control_port)


def get_receiver_function(connection_address, receive_timeout=None, queue_size=None, receive_raw=False):
    """
    Generate and return the function for running the mflow receiver.
    :param connection_address: Fully qualified ZMQ stream connection address.
    :param receive_timeout: ZMQ read timeout in milliseconds.
    :param queue_size: ZMQ queue size.
    :param receive_raw: Read the mflow socket in raw mode. Default: False.
    :return: Function to be executed in an external thread.
    """
    receive_timeout = receive_timeout or config.DEFAULT_RECEIVE_TIMEOUT
    queue_size = queue_size or config.DEFAULT_ZMQ_QUEUE_LENGTH

    def receiver_function(running_event, data_queue):
        try:

            # Setup the ZMQ listener and the stream mflow_processor.
            context = zmq.Context(io_threads=config.ZMQ_IO_THREADS)

            stream = Stream()
            stream.connect(address=connection_address,
                           conn_type=mflow.CONNECT,
                           mode=mflow.PULL,
                           receive_timeout=receive_timeout,
                           queue_size=queue_size,
                           context=context)

            # Setup the receive and converter function according to the raw parameter.
            receive_function = stream.receive_raw if receive_raw else stream.receive
            mflow_message_function = get_raw_mflow_message if receive_raw else get_mflow_message

            # The running event is used to signal that mflow has successfully started.
            running_event.set()
            while running_event.is_set():
                message = mflow_message_function(receive_function())

                # Process only valid messages.
                if message is not None:
                    data_queue.append(message)

            stream.disconnect()
        except Exception as e:
            _logger.error(e)
            running_event.clear()

    return receiver_function


def get_processor_function(processor, connection_address, receive_timeout=None, queue_size=None, receive_raw=False):
    receive_timeout = receive_timeout or config.DEFAULT_RECEIVE_TIMEOUT
    queue_size = queue_size or config.DEFAULT_ZMQ_QUEUE_LENGTH
    n_messages = None

    def process_parameters_queue(parameter_queue):
        # Set each parameter individually (either to the process or to the processor).
        while not parameter_queue.empty():
            parameter_to_set = parameter_queue.get()

            if not isinstance(parameter_to_set, tuple) or len(parameter_to_set) != 2:
                raise ValueError("Invalid parameter to set. Expected tuple of length 2, but received %s."
                                 % parameter_to_set)

            parameter_name = parameter_to_set[0]
            parameter_value = parameter_to_set[1]

            process_parameters_to_set = {}
            # The parameter is for this process.
            if parameter_name in config.PROCESS_PARAMETERS:
                process_parameters_to_set[parameter_name] = parameter_value

            # The parameter is for the processor.
            else:
                processor.set_parameter(parameter_to_set)

            if process_parameters_to_set:
                set_process_parameters(process_parameters_to_set)

    def set_process_parameters(parameters_to_set):

        # Set process GID. Always before UID.
        if config.PARAMETER_PROCESS_GID in parameters_to_set:
            gid_to_set = parameters_to_set.pop(config.PARAMETER_PROCESS_GID)
            os.setgid(gid_to_set)

        # Set process UID.
        if config.PARAMETER_PROCESS_UID in parameters_to_set:
            uid_to_set = parameters_to_set.pop(config.PARAMETER_PROCESS_UID)
            os.setgid(uid_to_set)

        # Set n_frames.
        if config.PARAMETER_N_MESSAGES in parameters_to_set:
            nonlocal n_messages
            n_messages = parameters_to_set.pop(config.PARAMETER_N_MESSAGES)

        if parameters_to_set:
            raise ValueError("Unknown process parameters. %s." % parameters_to_set)

    def processor_function(running_event, statistics_buffer, statistics_namespace, parameter_queue, data_queue):
        try:
            # Pass all the queued parameters before starting the mflow_processor.
            process_parameters_queue(parameter_queue)

            statistics = ThroughputStatistics(statistics_buffer, statistics_namespace)
            total_messages = 0
            processor.start()

            try:
                # Setup the ZMQ listener and the stream mflow_processor.
                context = zmq.Context(io_threads=config.ZMQ_IO_THREADS)

                stream = Stream()
                stream.connect(address=connection_address,
                               conn_type=mflow.CONNECT,
                               mode=mflow.PULL,
                               receive_timeout=receive_timeout,
                               queue_size=queue_size,
                               context=context)

                # Setup the receive and converter function according to the raw parameter.
                receive_function = stream.receive_raw if receive_raw else stream.receive
                mflow_message_function = get_raw_mflow_message if receive_raw else get_mflow_message

                # The running event is used to signal that mflow has successfully started.
                running_event.set()
                while running_event.is_set():
                    message = mflow_message_function(receive_function())

                    # Process only valid messages.
                    if message is not None:
                        processor.process_message(message)

                        total_messages += 1
                        if n_messages and total_messages >= n_messages:
                            _logger.info("Received %d frames. Stopping.", total_messages)
                            running_event.clear()

                        statistics.save_statistics(message.get_statistics())

                    # If available, pass parameters to the mflow_processor.
                    process_parameters_queue(parameter_queue)

                stream.disconnect()
            except Exception as e:
                _logger.error(e)
                running_event.clear()

            # Save the last statistics events even if the sampling interval was not reached.
            statistics.flush()
            processor.stop()

        except Exception as e:
            _logger.error(e)
            running_event.clear()

    return processor_function
