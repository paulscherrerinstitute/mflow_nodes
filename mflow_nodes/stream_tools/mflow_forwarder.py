from logging import getLogger
from mflow import mflow

from mflow_nodes import config


class MFlowForwarder(object):
    """
    MFlow forwarder. Forwards the mflow stream to the next node.
    """
    _logger = getLogger(__name__)

    def __init__(self, conn_type=mflow.BIND, mode=mflow.PUSH, receive_timeout=None, queue_size=None):
        """
        Constructor.
        :param conn_type: Type of mflow connection to use.
        :param mode: Socket type.
        :param receive_timeout: Receive timeout.
        :param queue_size: Queue size to use for mflow.
        """
        self.conn_type = conn_type
        self.mode = mode
        self.receive_timeout = receive_timeout or config.DEFAULT_RECEIVE_TIMEOUT
        self.queue_size = queue_size or config.DEFAULT_ZMQ_QUEUE_LENGTH
        self.stream = None

    def start(self, address):
        """
        Start the mflow connection on the provided address.
        :param address: Address to use for connection.
        :return: None.
        """
        self.stream = mflow.connect(address,
                                    conn_type=self.conn_type,
                                    mode=self.mode,
                                    receive_timeout=self.receive_timeout,
                                    queue_size=self.queue_size)

    def forward(self, message):
        """
        Forward the provided data.
        :param message: Message to be forwarded.
        :return: None.
        """
        self._logger.debug("Forwarding message with header:\n%s" % message.data["header"])
        self.stream.forward(message.data, block=True)

    def stop(self):
        """
        Disconnect the forwarder.
        :return: None.
        """
        self.stream.disconnect()
