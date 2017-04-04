import json
import requests

from mflow_nodes.config import API_PATH_FORMAT


class NodeClient(object):
    """
    Client for mflow node rest interface.
    """
    def __init__(self, address, instance_name):
        """
        Setup connection to the mflow node you want to control.
        :param address: REST Api address of the node. Example: 127.0.0.1:8080
        :param instance_name: Name of the node instance. Example: writer
        """
        self._api_address = address.rstrip("/") + API_PATH_FORMAT.format(instance_name=instance_name)

    def set_parameters(self, parameters):
        """
        Set parameters on the node.
        :param parameters: Parameters to set.
        :return: Response message.
        """
        headers = {'content-type': 'application/json'}
        set_parameters_url = self._api_address.format(url="parameters")
        response = requests.post(set_parameters_url,
                                 data=json.dumps(parameters),
                                 headers=headers).json()

        if response["status"] != "ok":
            raise ValueError("Cannot set node parameters. Original error:%s\n" % response["message"])

        return response["message"]

    def start(self):
        """
        Start the node.
        :return: Response message.
        """
        start_command_url = self._api_address.format(url="start")
        response = requests.get(start_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot start node. Original error:%s\n" % response["message"])

        return response["message"]

    def stop(self):
        """
        Stop the node.
        :return: Response message.
        """
        stop_command_url = self._api_address.format(url="stop")
        response = requests.get(stop_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot stop node. Original error:%s\n" % response["message"])

        return response["message"]

    def get_status(self):
        """
        Get node status.
        :return: Response data.
        """
        status_command_url = self._api_address.format(url="status")
        response = requests.get(status_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get status. Original error:%s\n" % response["message"])

        return response["data"]

    def get_statistics(self):
        """
        Get node statistics.
        :return: Response data.
        """
        parameters_command_url = self._api_address.format(url="statistics")
        response = requests.get(parameters_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get statistics. Original error:%s\n" % response["message"])

        return response["data"]

    def get_statistics_raw(self):
        """
        Get node raw statistics.
        :return: Response data (in JSON string).
        """
        parameters_command_url = self._api_address.format(url="statistics_raw")
        response = requests.get(parameters_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get raw statistics. Original error:%s\n" % response["message"])

        return response["data"]

    def get_parameters(self):
        """
        Get node parameters.
        :return: Response data.
        """
        parameters_command_url = self._api_address.format(url="parameters")
        response = requests.get(parameters_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get parameters. Original error:%s\n" % response["message"])

        return response["data"]

    def get_help(self):
        """
        Get node help.
        :return: Response data.
        """
        parameters_command_url = self._api_address.format(url="help")
        response = requests.get(parameters_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get help. Original error:%s\n" % response["message"])

        return response["data"]

    def kill(self):
        kill_command_url = self._api_address.format(url="kill")
        requests.delete(kill_command_url).json()
