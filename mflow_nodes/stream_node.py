import time
from collections import OrderedDict
from collections import deque
from logging import getLogger

from mflow import mflow
from mflow_nodes.rest_api.rest_server import start_web_interface, RestInterfacedProcess
from mflow_nodes.stream_tools.mflow_message import get_mflow_message

USE_MULTIPROCESSING = False

if USE_MULTIPROCESSING:
    from multiprocessing import Queue
    from multiprocessing import Event
    from multiprocessing import Process as Runner
else:
    from queue import Queue, Empty
    from threading import Event
    from threading import Thread as Runner


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
    :param start_listener: If true, the external mflow_processor will be started at node startup.
    :param processor_parameters: List of arguments to pass to the string mflow_processor start command.
    :param receive_raw: Pass the raw ZMQ messages to the mflow_processor.
    :return: None
    """
    _logger.debug("Node set to connect to '%s', with control address '%s:%s'." % (connection_address,
                                                                                  control_host,
                                                                                  control_port))

    # Start the ZMQ listener
    zmq_listener_process = ExternalProcessWrapper(get_zmq_listener(processor=processor,
                                                                   connection_address=connection_address,
                                                                   receive_raw=receive_raw),
                                                  initial_parameters=processor_parameters,
                                                  processor_instance=processor)

    if start_listener:
        zmq_listener_process.start()

    # Attach web interface
    start_web_interface(instance_name=instance_name, process=zmq_listener_process,
                        host=control_host, port=control_port)


def get_zmq_listener(processor, connection_address, receive_timeout=1000, queue_size=32, receive_raw=False):
    """
    Generate and return the function for listening to the ZMQ stream and process it in the provider mflow_processor.
    :param processor: Stream mflow_processor to be used in this instance.
    :type processor: StreamProcessor
    :param connection_address: Fully qualified ZMQ stream connection address.
    :param receive_timeout: ZMQ read timeout. Default: 1000.
    :param queue_size: ZMQ queue size. Default: 10.
    :param receive_raw: Return the raw ZMQ message.
    :return: Function to be executed in an external process.
    """

    def zmq_listener(stop_event, statistics_buffer, parameter_queue):

        # Pass all the queued parameters before starting the mflow_processor.
        while not parameter_queue.empty():
            processor.set_parameter(parameter_queue.get())

        # Start the statistics class.
        statistics = BasicStatistics(statistics_buffer)

        processor.start()

        def receiver(stop_event, data_queue):
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

        data_queue = Queue(maxsize=16)
        receiver_loop = Runner(target=receiver, args=(stop_event, data_queue))
        receiver_loop.start()

        while not stop_event.is_set():
            try:
                message = data_queue.get(timeout=receive_timeout)

                start_time = time.time()
                processor.process_message(message)
                stop_time = time.time()

                statistics.save_statistics(time_delta=stop_time - start_time,
                                           message=message)
            except Empty:
                pass

            # If available, pass parameters to the mflow_processor.
            while not parameter_queue.empty():
                processor.set_parameter(parameter_queue.get())

        # Wait for the receiver to quit.
        receiver_loop.join()

        # Clean up after yourself.
        stop_event.clear()
        processor.stop()

    return zmq_listener


class ExternalProcessWrapper(RestInterfacedProcess):
    """
    Wrap the processing function to allow for inter process communication.
    """

    def __init__(self, process_function, initial_parameters=None, processor_instance=None):
        """
        Constructor.
        :param process_function: Function to start in a new process.
        :param initial_parameters: Parameters to pass to the function at instantiation.
        """
        self.process_function = process_function
        self.processor_instance = processor_instance
        self.process = None

        self.stop_event = Event()
        self.parameter_queue = Queue()

        self.statistics_buffer = deque(maxlen=1000)
        self.statistics = BasicStatistics(self.statistics_buffer)

        self.current_parameters = initial_parameters or {}

        # Pre-process static attributes.
        self._process_name = getattr(self.processor_instance, "__name__",
                                     self.processor_instance.__class__.__name__) \
            if self.processor_instance else "Unknown processor"

    def is_running(self):
        """
        Return the status of the process function (running or not).
        :return: True if running, otherwise False.
        """
        return self.process and self.process.is_alive()

    def start(self):
        """
        Start the processing function in a new process.
        :return: None or Exception if the function is already running.
        """
        _logger.debug("Starting node.")

        if self.is_running():
            raise Exception("External process is already running.")

        self.process = Runner(target=self.process_function,
                              args=(self.stop_event, self.statistics_buffer, self.parameter_queue))

        self._set_current_parameters()
        self.process.start()

    def stop(self):
        """
        Stop the processing function process.
        :return: None or Exception if the function is not running.
        """
        _logger.debug("Stopping node.")
        if not self.is_running():
            raise Exception("External process is already stopped.")

        self.stop_event.set()
        # Wait maximum of 10 seconds for process to stop
        for i in range(100):
            time.sleep(0.1)
            if not self.process.is_alive():
                break

        self.process = None

    def wait(self):
        self.process.join()

    def set_parameter(self, parameter):
        """
        Pass a parameter to the processing function. It needs to be in tuple format: (name, value).
        :param parameter: Tuple of (parameter_name, parameter_value).
        :return: None.
        """
        self.current_parameters[parameter[0]] = parameter[1]
        self.parameter_queue.put(parameter)

    def _set_current_parameters(self):
        for parameter in self.current_parameters.items():
            self.set_parameter(parameter)

    def get_process_name(self):
        return self._process_name

    def get_process_help(self):
        return RestInterfacedProcess.get_process_help(self.processor_instance)

    def get_parameters(self):
        # Collect default mflow_processor parameters and update them with the user set.
        all_parameters = RestInterfacedProcess.get_parameters(self.processor_instance) \
            if self.processor_instance else {}
        all_parameters.update(self.current_parameters)

        return all_parameters

    def get_statistics(self):
        return self.statistics.get_statistics()

    def get_statistics_raw(self):
        return self.statistics.get_statistics_raw()


class BasicStatistics(object):
    """
    Basic statistics implementation for mflow node.
    """

    def __init__(self, buffer):
        """
        Initialize the class.
        :param buffer: Statistics buffer length. Default 1000.
        """
        self._buffer = buffer

    def save_statistics(self, time_delta, message):
        """
        Add statistics point to the buffer.
        :param time_delta: Time needed to process the message.
        :param message: Message that was processed.
        """
        self._buffer.append({"message_length": message.get_data_length(),
                             "processing_time": time_delta,
                             "frame": message.get_frame_index()})

    def get_statistics_raw(self):
        """
        Return the raw statistics data.
        :return: List of statistic events.
        """
        return self._buffer

    def get_statistics(self):
        """
        Get the processed statistics. Aggregate them together and display averages.
        Dictionary of statistic values.
        """
        raw_data = self.get_statistics_raw()
        # Check if there is any statistics at all.
        if not raw_data:
            return {}

        total_number_frames = len(raw_data)
        total_time = sum((x["processing_time"] for x in raw_data))
        total_bytes = sum((x["message_length"] for x in raw_data))

        frame_rate = total_number_frames / total_time
        bytes_rate = total_bytes / total_time

        statistics = {"total_time_seconds": total_time,
                      "total_frames": total_number_frames,
                      "frame_per_second": frame_rate,
                      "total_bytes": total_bytes,
                      "bytes_per_second": bytes_rate}

        return OrderedDict(sorted(statistics.items()))
