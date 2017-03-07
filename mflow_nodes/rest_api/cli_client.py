import json

from mflow_nodes.rest_api.rest_client import start, stop, get_status, get_parameters, set_parameters, get_help, \
    get_statistics, get_statistics_raw
from mflow_nodes.rest_api.rest_server import API_PATH_FORMAT


class ConsoleClient(object):
    """
    Console client for mflow node rest interface.
    """
    def __init__(self, address, instance_name):
        """
        Setup connection to the mflow node you want to control.
        :param address: REST Api address of the node. Example: 127.0.0.1:8080
        :param instance_name: Name of the node instance. Example: writer
        """

        self._address = address.rstrip("/") + API_PATH_FORMAT.format(instance_name=instance_name).format(url="")

        self.start = lambda: start(self._address)
        self.stop = lambda: stop(self._address)
        self.get_status = lambda: get_status(self._address)
        self.get_parameters = lambda: get_parameters(self._address)
        self.set_parameters = lambda parameters: set_parameters(self._address, parameters)
        self.get_help = lambda: get_help(self._address)
        self.get_statistics = lambda: get_statistics(self._address)
        self.get_statistics_raw = lambda: get_statistics_raw(self._address)