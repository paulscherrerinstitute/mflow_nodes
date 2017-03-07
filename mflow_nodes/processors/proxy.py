from logging import getLogger

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.stream_tools.mflow_forwarder import MFlowForwarder


class ProxyProcessor(BaseProcessor):
    """
    MFlow Proxy

    Executes the provided function on the received packages and forwards the original
    package to the next node.

    Proxy commands:
        start                          Starts the proxy.
        stop                           Stop the proxy.

    Proxy parameters:
        forwarding_address             Address to forward the stream to.
    """
    _logger = getLogger(__name__)

    def __init__(self, proxy_function, name="Proxy node"):
        """
        Initialize the proxy node.
        :param proxy_function: Proxy function to be executed on message.
        :param name: Name of the proxy.
        """
        self._zmq_forwarder = None
        self._proxy_function = proxy_function
        self.__name__ = name

        # Parameters to set.
        self.binding_address = None

    def _validate_parameters(self):
        error_message = ""

        if not self.binding_address:
            error_message += "Parameter 'binding_address' not set.\n"

        if not callable(self._proxy_function):
            error_message += "Parameter 'proxy_function' is not a valid function\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def start(self):
        self._logger.debug("Proxy started.")
        # Check if all the needed input parameters are available.
        self._validate_parameters()

        self._logger.debug("Stream forwarding address='%s'." % self.binding_address)
        self._zmq_forwarder = MFlowForwarder()
        self._zmq_forwarder.start(self.binding_address)

    def process_message(self, message):
        self._logger.debug("Received frame '%d'. Passing to proxy function." % message.get_frame_index())
        forward_message = self._proxy_function(message)

        if forward_message:
            self._zmq_forwarder.forward(message.raw_message)

    def stop(self):
        self._zmq_forwarder.stop()
