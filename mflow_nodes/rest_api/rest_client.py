import json
import requests

from mflow_nodes import config


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
        self._api_address = address.rstrip("/") + config.API_PATH_FORMAT.format(instance_name=instance_name)

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

    def set_process_user(self, uid=None, gid=None):
        """
        Set the user the writer process should run under.
        :param uid: User id.
        :param gid: Group id.
        :return: Response from the server.
        """
        parameters = {}

        if uid is not None:
            parameters[config.PARAMETER_PROCESS_UID] = uid

        if gid is not None:
            parameters[config.PARAMETER_PROCESS_GID] = gid

        if parameters:
            return self.set_parameters(parameters)

        raise ValueError("UID and/or GID must be specified.")

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

    def reset(self):
        """
        Reset the node.
        :return: Response message.
        """
        reset_command_url = self._api_address.format(url="reset")
        response = requests.post(reset_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot reset node. Original error:%s\n" % response["message"])

        return response["data"]

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

    def get_log_level(self):
        """
        Get log levels.
        :return: Response data.
        """
        log_command_url = self._api_address.format(url="logging")
        response = requests.get(log_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot get log levels. Original error:%s\n" % response["message"])

        return response["data"]

    def set_log_level(self, parameters):
        """
        Set parameters on the node.
        :param parameters: Parameters set the logs to: {"logger_name": "level"}.
        :return: Response message.
        """
        headers = {'content-type': 'application/json'}
        set_log_url = self._api_address.format(url="logging")
        response = requests.post(set_log_url,
                                 data=json.dumps(parameters),
                                 headers=headers).json()

        if response["status"] != "ok":
            raise ValueError("Cannot set log levels. Original error:%s\n" % response["message"])

        return response["message"]

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
        """
        Kill this process instance.
        """
        try:
            kill_command_url = self._api_address.format(url="kill")
            requests.delete(kill_command_url).json()
        except requests.ConnectionError as e:
            # This exception message is expected when killing the server.
            # TODO: Fix this, it is only a temporary hack.
            excepted_message = "('Connection aborted.', RemoteDisconnected(" \
                            "'Remote end closed connection without response',))"
            if str(e) != excepted_message:
                raise

        return "Node killed."
