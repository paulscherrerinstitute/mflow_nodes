from logging import getLogger
from mflow import mflow, json


class StreamProcessor(object):
    """
    Just a stub, for actual stream processors to extend.
    """
    _logger = getLogger(__name__)

    def start(self):
        """
        Start the stream mflow_processor.
        :return: None.
        """
        self._logger.debug("Starting mflow_processor.")

    def process_message(self, message):
        """
        Process the message received over ZMQ.
        :param message: Message received from the ZMQ stream.
        :return: None
        """
        self._logger.debug("Received message.")

    def set_parameter(self, parameter):
        """
        Set the parameter received from the REST API.
        :param parameter: Tuple of format (parameter_name, parameter_value).
        :return: None
        """
        # Parameters can only be received in tuple format: (parameter_name, parameter_value)
        if not isinstance(parameter, tuple):
            error = "Received parameter '%s' value is not in tuple format. " \
                    "All parameters must be in format: (name, value)."
            self._logger.error(error)
            raise ValueError(error)

        name = parameter[0]
        value = parameter[1]
        self._logger.debug("Update parameter '%s'='%s'" % (name, value))

        # Overwrite current attribute value.
        setattr(self, name, value)

    def stop(self):
        """
        Stop the mflow_processor. Called after the processing node stop command has been invoked.
        :return: None.
        """
        self._logger.debug("Stopping mflow_processor.")


class MFlowForwarder(object):
    """
    MFlow forwarder. Forwards the mflow stream to the next node.
    """
    _logger = getLogger(__name__)

    def __init__(self, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=1000, queue_size=16):
        """
        Constructor.
        :param conn_type: Type of mflow connection to use.
        :param mode: Socket type.
        :param receive_timeout: Receive timeout.
        :param queue_size: Queue size to use for mflow.
        """
        self.conn_type = conn_type
        self.mode = mode
        self.receive_timeout = receive_timeout
        self.queue_size = queue_size
        self.stream = None

    def start(self, address):
        """
        Start the mflow connection on the provided address.
        :param address: Address to forward to.
        :return: None.
        """
        self.stream = mflow.connect(address,
                                    conn_type=self.conn_type,
                                    mode=self.mode,
                                    receive_timeout=self.receive_timeout,
                                    queue_size=self.queue_size)

    def send(self, header, data):
        """
        Forward the provided data.
        :param header: Header to forward.
        :param data: Data to forward.
        :return: None.
        """
        self._logger.debug("Forwarding frame '%d'." % header["frame"])
        self.stream.send(json.dumps(header).encode(), send_more=True, block=True)

        self.stream.send(data , block=True)

    def stop(self):
        """
        Disconnect the forwarder.
        :return: None.
        """
        self.stream.disconnect()
