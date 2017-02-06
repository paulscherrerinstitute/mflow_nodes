from logging import getLogger

from mflow_rest_api.rest_interface import start_web_interface

_logger = getLogger(__name__)


def start_rest_node(processor, control_host="0.0.0.0", control_port=8080):
    _logger.debug("Node control address '%s:%s'." % (control_host, control_port))

    start_web_interface(process=processor, host=control_host, port=control_port)
