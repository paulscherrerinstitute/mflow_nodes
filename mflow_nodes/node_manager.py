from collections import deque, OrderedDict
from logging import getLogger

USE_MULTIPROCESSING = False

if USE_MULTIPROCESSING:
    from multiprocessing import Queue
    from multiprocessing import Event
    from multiprocessing import Process as Runner
else:
    from queue import Queue
    from threading import Event
    from threading import Thread as Runner

from mflow_nodes.rest_api.rest_server import RestInterfacedProcess

_logger = getLogger(__name__)


class ExternalProcessWrapper(RestInterfacedProcess):
    """
    Wrap the processing function to allow for inter process communication.
    """

    def __init__(self, processor_function, receiver_function, initial_parameters=None, processor_instance=None,
                 thread_queue_size=16):
        """
        Constructor.
        :param processor_function: Function to run the processor in a thread.
        :param receiver_function: Function to run the receiver in a thread.
        :param initial_parameters: Parameters to pass to the function at instantiation.
        :param processor_instance: Instance of the processor (for help and parameters)
        :param thread_queue_size: Size of the data queue between the processor and receiver thread.
        """
        self.thread_queue_size = thread_queue_size
        self.process_function = processor_function
        self.processor_instance = processor_instance
        self.process_thread = None

        self.receiver_function = receiver_function
        self.receiver_thread = None

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
        return (self.process_thread and self.process_thread.is_alive()) and \
               (self.receiver_thread and self.receiver_thread.is_alive())

    def start(self):
        """
        Start the processing function in a new process.
        :return: None or Exception if the function is already running.
        """
        _logger.debug("Starting node.")

        if self.is_running():
            raise Exception("External process is already running.")

        data_queue = Queue(maxsize=16)

        self.process_thread = Runner(target=self.process_function,
                                     args=(self.stop_event, self.statistics_buffer,
                                           self.parameter_queue, data_queue))

        self.receiver_thread = Runner(target=self.receiver_function,
                                      args=(self.stop_event, data_queue))

        self._set_current_parameters()
        self.process_thread.start()
        self.receiver_thread.start()

    def stop(self):
        """
        Stop the processing function process.
        :return: None or Exception if the function is not running.
        """
        _logger.debug("Stopping node.")
        if not self.is_running():
            raise Exception("External process is already stopped.")

        self.stop_event.set()

        # Wait for both threads to stop.
        self.process_thread.join()
        self.receiver_thread.join()

        self.stop_event.clear()

        self.process_thread = None
        self.receiver_thread = None

    def wait(self):
        self.process_thread.join()

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
