import uuid
from argparse import Namespace
from collections import deque
from logging import getLogger

import multiprocessing

from mflow.tools import ThroughputStatistics
from multiprocessing import Process, Event, Queue

from mflow_nodes import config
from mflow_nodes.rest_api.rest_server import RestInterfacedProcess

_logger = getLogger(__name__)


class NodeManager(RestInterfacedProcess):
    """
    Wrap the processing function to allow for inter process communication.
    """

    def __init__(self, processor_function, receiver_function, initial_parameters=None, processor_instance=None,
                 data_queue_size=None, n_receiving_threads=None):
        """
        Constructor.
        :param processor_function: Function to run the processor in a thread.
        :param receiver_function: Function to run the receiver in a thread.
        :param initial_parameters: Parameters to pass to the function at instantiation.
        :param processor_instance: Instance of the processor (for help and parameters)
        :param data_queue_size: Size of the data queue between the processor and receiver thread.
        :param n_receiving_threads: Number of receiving threads.
        """
        self.processor_instance = processor_instance
        self.data_queue_size = data_queue_size or config.DEFAULT_DATA_QUEUE_LENGTH
        self.current_parameters = initial_parameters or {}
        self.n_receiving_threads = n_receiving_threads or config.DEFAULT_N_RECEIVING_THREADS

        _logger.debug("Using %d receiving threads." % self.n_receiving_threads)

        self.processor_function = processor_function
        self.processor_process = None
        self.processor_running = Event()
        self.parameter_queue = Queue()

        self.receiver_function = receiver_function

        self.statistics_buffer = deque(maxlen=config.DEFAULT_STATISTICS_BUFFER_LENGTH)
        self.statistics_namespace = Namespace()
        self.statistics = ThroughputStatistics(self.statistics_buffer, self.statistics_namespace)

        # Pre-process static attributes.
        self._process_name = getattr(self.processor_instance, "__name__",
                                     self.processor_instance.__class__.__name__) \
            if self.processor_instance else "Unknown processor"

    def is_running(self):
        """
        Return the status of the process function (running or not).
        :return: True if running, otherwise False.
        """
        return (self.processor_process is not None and self.processor_process.is_alive()
                and self.processor_running.is_set())
        # and (all(thr and thr.is_alive() and running.is_set()
        #      for thr, running in zip(self.receiver_threads, self.receivers_running))
        #      )

    def start(self):
        """
        Start the processing function in a new process.
        """
        # It is either restart (so, first stop) or clean the current situation up (in case one of the threads died).
        self.stop()

        _logger.debug("Starting node.")

        data_queue = deque(maxlen=self.data_queue_size)

        self.processor_process = Process(target=self.processor_function,
                                         args=(
                                             self.processor_running, self.statistics_buffer, self.statistics_namespace,
                                             self.parameter_queue, data_queue))

        self._set_current_parameters()
        self.processor_process.start()

        # Both thread need to set the running event. If not, something went wrong.
        if not self.processor_running.wait(config.DEFAULT_STARTUP_TIMEOUT):
            error = "An exception occurred during the startup."
            _logger.error(error)
            raise ValueError(error)

    def stop(self):
        """
        Stop the processing function process.
        """
        _logger.debug("Stopping node.")

        self.processor_running.clear()

        if self.processor_process is not None:
            self.processor_process.join()
            self.processor_process = None

    def set_parameters(self, parameters):
        """
        Pass a parameter to the processing function. It needs to be in tuple format: (name, value).
        :param parameters: Dictionary of parameters.
        :return: None.
        """
        for parameter_name, parameter_value in parameters.items():
            # Update current parameters.
            self.current_parameters[parameter_name] = parameter_value

            # Set the current parameters to the queue.
            self.parameter_queue.put((parameter_name, parameter_value))

    def _set_current_parameters(self):
        self.set_parameters(self.current_parameters)

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
        return list(self.statistics.get_statistics_raw())


def external_process_wrapper(node_manager, communication_pipe, stop_event):
    _logger.debug("External process wrapper started.")

    try:
        while not stop_event.is_set():
            if not communication_pipe.poll(config.DEFAULT_IPC_POLL_TIMEOUT):
                continue

            ipc_call = communication_pipe.recv()
            ipc_method = ipc_call["method"]

            _logger.debug("Executing call_id %s for method %s on node_manager.", ipc_call, ipc_method)

            args = ipc_call["args"] or []
            kwargs = ipc_call["kwargs"] or {}

            method_return = getattr(node_manager, ipc_method)(*args, **kwargs)

            ipc_return = {"call_id": ipc_call["call_id"],
                          "return": method_return}

            communication_pipe.send(ipc_return)

    except KeyboardInterrupt:
        node_manager.stop()

    _logger.debug("External process wrapper stopped.")


class NodeManagerProxy(object):
    def __init__(self, node_manager, ipc_timeout=None):
        if not ipc_timeout:
            ipc_timeout = config.DEFAULT_IPC_TIMEOUT

        self.node_manager = node_manager
        self.stop_event = multiprocessing.Event()
        self.ipc_timeout = ipc_timeout

        self.external_process = None
        self.communication_pipe = None

        self.current_parameters = self.node_manager.get_parameters()

    def start(self):
        _logger.debug("Starting external process.")

        if self.is_process_running():
            _logger.info("Process already running.")
            return

        self.stop_event.clear()
        external_pipe, self.communication_pipe = multiprocessing.Pipe()
        self.external_process = multiprocessing.Process(target=external_process_wrapper,
                                                        args=(self.node_manager, external_pipe, self.stop_event))

        self.external_process.start()
        self._set_current_parameters()
        self._execute_call("start")

    def stop(self):
        _logger.debug("Stopping external process wrapper")

        if self.is_process_running():
            self._execute_call("stop")
            self.stop_event.set()
            self.external_process.join(config.DEFAULT_SHUTDOWN_TIMEOUT)

            # The join didn't happen.
            if self.external_process.is_alive():
                self.external_process.terminate()
        else:
            _logger.info("External process already stopped.")

        self.external_process = None

    def is_running(self):
        if self.is_process_running():
            # If the external process is running, we have to ask him about his status.
            return self._execute_call("is_running")
        else:
            return False

    def is_process_running(self):
        return self.external_process is not None and self.external_process.is_alive()

    def kill(self):
        _logger.debug("Killing external process wrapper.")
        if self.is_process_running():
            self.external_process.terminate()
        else:
            _logger.warning("External process already stopped.")

        self.external_process = None

    def _execute_call(self, method_name, args=None, kwargs=None):
        if not self.is_process_running():
            return None
            # raise ValueError("Cannot execute %s call because external process is not running." % method_name)

        # Discard any messages from the previous exchange.
        while self.communication_pipe.poll():
            self.communication_pipe.recv()

        _logger.debug("Executing method '%s' with args '%s' and kwargs '%s'.", method_name, args, kwargs)

        call_id = uuid.uuid4()
        ipc_call = {"call_id": call_id,
                    "method": method_name,
                    "args": args,
                    "kwargs": kwargs}

        self.communication_pipe.send(ipc_call)

        # The other end did not reply in timely fashion.
        if not self.communication_pipe.poll(timeout=self.ipc_timeout):
            raise TimeoutError("Execution of method '%s' timeout." % method_name)

        response = self.communication_pipe.recv()

        if response["call_id"] != call_id:
            raise ValueError("Request call_id %s but response call_id %s." % (call_id, response["call_id"]))

        return response["return"]

    def get_process_name(self):
        return self.node_manager.get_process_name()

    def get_process_help(self):
        return self.node_manager.get_process_help()

    def _set_current_parameters(self):
        if self.is_process_running():
            for parameter in self.current_parameters.items():
                self._execute_call("set_parameter", (parameter,))
        else:
            _logger.info("External processor not running. Parameter set added to queue.")

    def set_parameters(self, parameters):
        """
        Pass a parameter to the processing function. It needs to be in tuple format: (name, value).
        :param parameters: Dictionary of parameters.
        :return: None.
        """
        for parameter_name, parameter_value in parameters.items():
            # Update current parameters.
            self.current_parameters[parameter_name] = parameter_value

            # Set the current parameters to the queue.
            self.parameter_queue.put((parameter_name, parameter_value))

    def get_parameters(self):
        if self.is_process_running():
            return self._execute_call("get_parameters")
        else:
            return self.current_parameters

    def __getattr__(self, method_name):
        def call_function(*args, **kwargs):
            self._execute_call(method_name, args, kwargs)

        return call_function
