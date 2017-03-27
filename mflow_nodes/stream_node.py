from logging import getLogger
from queue import Empty

from mflow import mflow
from mflow.tools import ThroughputStatistics
from mflow_nodes.node_manager import ExternalProcessWrapper
from mflow_nodes.rest_api.rest_server import start_web_interface
from mflow_nodes.stream_tools.mflow_message import get_mflow_message

_logger = getLogger(__name__)


def start_stream_node(instance_name, processor, processor_parameters=None,
                      connection_address="tcp://127.0.0.1:40000", control_host="0.0.0.0", control_port=8080,
                      start_listener=False, receive_raw=False):
    """
    Start the ZMQ processing node.
    :param instance_name: Name of the processor instance. Used for the REST api path.
    :param processor: Stream mflow_processor that does the actual work on the stream data.
    :type processor: StreamProcessor
    :param connection_address: Fully qualified ZMQ stream connection address. Default: "tcp://127.0.0.1:40000"
    :param control_host: Binding host for the control REST API. Default: "0.0.0.0"
    :param control_port: Binding port for the control REST API. Default: 8080
    :param control_port: Binding port for the control REST API. Default: 8080
    :param start_listener: If true, the external mflow_processor will be started at node startup.
    :param processor_parameters: List of arguments to pass to the string mflow_processor start command.
    :param receive_raw: Pass the raw ZMQ messages to the mflow_processor.
    :return: None
    """
    _logger.debug("Node set to connect to '%s', with control address '%s:%s'." % (connection_address,
                                                                                  control_host,
                                                                                  control_port))

    # Start the ZMQ listener
    zmq_listener_process = ExternalProcessWrapper(processor_function=get_processor_function(processor=processor),
                                                  receiver_function=get_receiver_function(
                                                      connection_address=connection_address,
                                                      receive_raw=receive_raw),
                                                  initial_parameters=processor_parameters,
                                                  processor_instance=processor)

    if start_listener:
        zmq_listener_process.start()

    # Attach web interface
    start_web_interface(instance_name=instance_name, process=zmq_listener_process,
                        host=control_host, port=control_port)


def get_receiver_function(connection_address, receive_timeout=1000, queue_size=32, receive_raw=False):
    """
    Generate and return the function for running the mflow receiver.
    :param connection_address: Fully qualified ZMQ stream connection address.
    :param receive_timeout: ZMQ read timeout in milliseconds. Default: 1000.
    :param queue_size: ZMQ queue size. Default: 32.
    :param receive_raw: Read the mflow socket in raw mode. Default: False.
    :return: Function to be executed in an external thread.
    """

    def receiver_function(stop_event, data_queue):
        # Setup the ZMQ listener and the stream mflow_processor.
        stream = mflow.connect(address=connection_address,
                               conn_type=mflow.CONNECT,
                               mode=mflow.PULL,
                               receive_timeout=receive_timeout,
                               queue_size=queue_size)

        # Setup the receive function according to the raw parameter.
        receive_function = stream.receive_raw if receive_raw else stream.receive

        while not stop_event.is_set():
            message = get_mflow_message(receive_function())

            # Process only valid messages.
            if message is not None:
                data_queue.put(message, timeout=receive_timeout)

        stream.disconnect()

    return receiver_function


def get_processor_function(processor, read_timeout=1000):
    """
    Generate and return the function for running the processor.
    :param processor: Stream mflow_processor to be used in this instance.
    :type processor: StreamProcessor
    :param read_timeout: Timeout to read the data queue, in milliseconds. Default: 1000.
    :return: Function to be executed in an external thread.
    """
    def processor_function(stop_event, statistics_buffer, statistics_namespace, parameter_queue, data_queue):

        # Pass all the queued parameters before starting the mflow_processor.
        while not parameter_queue.empty():
            processor.set_parameter(parameter_queue.get())

        # Start the statistics class.
        statistics = ThroughputStatistics(statistics_buffer, statistics_namespace)

        processor.start()

        # Queue accepts the timeout in seconds, but zmq accepts milliseconds.
        # We are trying to have a common (millisecond) value for timeouts.
        queue_timeout = read_timeout/1000
        while not stop_event.is_set():
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

    return processor_function
