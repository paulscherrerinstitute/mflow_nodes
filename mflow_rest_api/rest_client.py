import json
from urllib.parse import urljoin
import requests


def set_parameters(base_url, parameters):
    """
    Set parameters on the node.
    :param base_url: Base URL of the node.
    :param parameters: Parameters to set.
    :return: Response message.
    """
    headers = {'content-type': 'application/json'}
    set_parameters_url = urljoin(base_url, "/parameters")
    response = requests.post(set_parameters_url,
                             data=json.dumps(parameters),
                             headers=headers).json()

    if response["status"] != "ok":
        raise ValueError("Cannot set writer parameters. Original error:%s\n" % response["message"])

    return response["message"]


def start(base_url):
    """
    Start the node.
    :param base_url: Base URL of the node.
    :return: Response message.
    """
    start_command_url = urljoin(base_url, "/start")
    response = requests.get(start_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot start writer. Original error:%s\n" % response["message"])

    return response["message"]


def stop(base_url):
    """
    Stop the node.
    :param base_url: Base URL of the node.
    :return: Response message.
    """
    stop_command_url = urljoin(base_url, "/stop")
    response = requests.get(stop_command_url).json()
    if response["status"] != "ok":
        raise ValueError("Cannot stop writer. Original error:%s\n" % response["message"])

    return response["message"]
