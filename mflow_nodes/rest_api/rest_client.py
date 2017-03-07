import json
import requests


def set_parameters(base_url, parameters):
    """
    Set parameters on the node.
    :param base_url: Base URL of the node.
    :param parameters: Parameters to set.
    :return: Response message.
    """
    headers = {'content-type': 'application/json'}
    set_parameters_url = base_url.rstrip("/") + "/parameters"
    response = requests.post(set_parameters_url,
                             data=json.dumps(parameters),
                             headers=headers).json()

    if response["status"] != "ok":
        raise ValueError("Cannot set node parameters. Original error:%s\n" % response["message"])

    return response["message"]


def start(base_url):
    """
    Start the node.
    :param base_url: Base URL of the node.
    :return: Response message.
    """
    start_command_url = base_url.rstrip("/") + "/start"
    response = requests.get(start_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot start node. Original error:%s\n" % response["message"])

    return response["message"]


def stop(base_url):
    """
    Stop the node.
    :param base_url: Base URL of the node.
    :return: Response message.
    """
    stop_command_url = base_url.rstrip("/") + "/stop"
    response = requests.get(stop_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot stop node. Original error:%s\n" % response["message"])

    return response["message"]


def get_status(base_url):
    """
    Get node status.
    :param base_url: Base URL of the node.
    :return: Response data.
    """
    status_command_url = base_url.rstrip("/") + "/status"
    response = requests.get(status_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot get status. Original error:%s\n" % response["message"])

    return response["data"]


def get_statistics(base_url):
    """
    Get node statistics.
    :param base_url: Base URL of the node.
    :return: Response data.
    """
    parameters_command_url = base_url.rstrip("/") + "/statistics"
    response = requests.get(parameters_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot get statistics. Original error:%s\n" % response["message"])

    return response["data"]


def get_statistics_raw(base_url):
    """
    Get node raw statistics.
    :param base_url: Base URL of the node.
    :return: Response data (in JSON string).
    """
    parameters_command_url = base_url.rstrip("/") + "/statistics_raw"
    response = requests.get(parameters_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot get raw statistics. Original error:%s\n" % response["message"])

    return response["data"]


def get_parameters(base_url):
    """
    Get node parameters.
    :param base_url: Base URL of the node.
    :return: Response data.
    """
    parameters_command_url = base_url.rstrip("/") + "/parameters"
    response = requests.get(parameters_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot get parameters. Original error:%s\n" % response["message"])

    return response["data"]


def get_help(base_url):
    """
    Get node help.
    :param base_url: Base URL of the node.
    :return: Response data.
    """
    parameters_command_url = base_url.rstrip("/") + "/help"
    response = requests.get(parameters_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot get help. Original error:%s\n" % response["message"])

    return response["data"]
