from logging import getLogger
from queue import Empty

from mflow import mflow
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

    node_manager = NodeManager(processor_function=get_processor_function(processor=processor),
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
            stream = mflow.connect(address=connection_address,
                                   conn_type=mflow.CONNECT,
                                   mode=mflow.PULL,
                                   receive_timeout=receive_timeout,
                                   queue_size=queue_size)

            # Setup the receive and converter function according to the raw parameter.
            receive_function = stream.receive_raw if receive_raw else stream.receive
            mflow_message_function = get_raw_mflow_message if receive_raw else get_mflow_message

            # The running event is used to signal that mflow has successfully started.
            running_event.set()
            while running_event.is_set():
                message = mflow_message_function(receive_function())

                # Process only valid messages.
                if message is not None:
                    data_queue.put(message, timeout=receive_timeout)

            stream.disconnect()
        except Exception as e:
            _logger.error(e)
            running_event.clear()

    return receiver_function


def get_processor_function(processor, read_timeout=None):
    """
    Generate and return the function for running the processor.
    :param processor: Stream mflow_processor to be used in this instance.
    :type processor: StreamProcessor
    :param read_timeout: Timeout to read the data queue, in milliseconds.
    :return: Function to be executed in an external thread.
    """
    read_timeout = read_timeout or config.DEFAULT_QUEUE_READ_TIMEOUT

    def processor_function(running_event, statistics_buffer, statistics_namespace, parameter_queue, data_queue):
        try:
            # Pass all the queued parameters before starting the mflow_processor.
            while not parameter_queue.empty():
                processor.set_parameter(parameter_queue.get())

            statistics = ThroughputStatistics(statistics_buffer, statistics_namespace)

            processor.start()

            # Queue accepts the timeout in seconds, but zmq accepts milliseconds.
            # We are trying to have a common (millisecond) value for timeouts.
            queue_timeout = read_timeout / 1000

            # The running event is used to signal that the processor has successfully started.
            running_event.set()
            while running_event.is_set():
                try:
                    message = data_queue.get(timeout=queue_timeout)
                    processor.process_message(message)
                    statistics.save_statistics(message.get_statistics())
                except Empty:
                    pass

                # If available, pass parameters to the mflow_processor.
                while not parameter_queue.empty():
                    processor.set_parameter(parameter_queue.get())

            # Save the last statistics events even if the sampling interval was not reached.
            statistics.flush()
            processor.stop()
        except Exception as e:
            _logger.error(e)
            running_event.clear()

    return processor_function
