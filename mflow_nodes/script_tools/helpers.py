import json
import logging
import os
import sys
from collections import OrderedDict

from mflow_nodes import config
from mflow_nodes.stream_node import start_stream_node

_logger = logging.getLogger(__name__)


def add_default_arguments(parser, binding_argument=False, default_rest_host=None, default_rest_port=None):
    """
    Adds the arguments every script needs
    :param parser: ArgumentParser instance to add the arguments to.
    :param binding_argument: If True, include the binding address to forward the strem.
    :param default_rest_host: The default rest host to use. Default: 0.0.0.0
    :param default_rest_port: The default rest port to use. Default: 41000
    """
    default_rest_host = default_rest_host or config.DEFAULT_REST_HOST
    default_rest_port = default_rest_port or config.DEFAULT_REST_PORT

    parser.add_argument("instance_name", type=str, help="Name of the node instance. Should be unique.")
    parser.add_argument("connect_address", type=str, help="Connect address for mflow receiver.\n"
                                                          "Example: tcp://127.0.0.1:40000")
    if binding_argument:
        parser.add_argument("binding_address", type=str, help="Binding address for mflow stream forwarding.\n"
                                                              "Example: tcp://127.0.0.1:40001")
    parser.add_argument("--config_file", type=str, default=None, help="Config file with the detector properties.")
    parser.add_argument("--log_level", default=config.DEFAULT_LOGGING_LEVEL,
                        choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'],
                        help="Log level to use.")
    parser.add_argument("--raw", action='store_true', help="Receive and send mflow messages with raw handler.")
    parser.add_argument("--rest_host", type=str, default=default_rest_host, help="Host for web interface.\n"
                                                                                 "Default: %s" % default_rest_host)
    parser.add_argument("--rest_port", type=int, default=default_rest_port, help="Port for web interface.\n"
                                                                                 "Default: %s" % default_rest_port)


def load_logging_config_files(additional_config_file=None):
    """
    Load the logging config files.
    :param additional_config_file: Specify an additional config file, if needed. Default: None.
    :return: Dictionary with logger config.
    """
    config_from_file = {}

    if os.path.exists(config.LOG_MACHINE_FILENAME):
        config_from_file.update(json.loads(config.LOG_MACHINE_FILENAME, object_pairs_hook=OrderedDict))

    if os.path.exists(config.LOG_USER_FILENAME):
        config_from_file.update(json.loads(config.LOG_USER_FILENAME, object_pairs_hook=OrderedDict))

    if os.path.exists(config.LOG_PWD_FILENAME):
        config_from_file.update(json.loads(config.LOG_PWD_FILENAME, object_pairs_hook=OrderedDict))

    if additional_config_file and os.path.exists(additional_config_file):
        config_from_file.update(json.loads(additional_config_file, object_pairs_hook=OrderedDict))

    return config_from_file


def setup_logging(log_level=logging.DEBUG):
    """
    Most common set of logging configuration for debugging.
    :param log_level: Logging level.
    """
    logging.basicConfig(stream=sys.stdout, level=log_level)

    logging.getLogger("mflow.mflow").setLevel(logging.ERROR)
    logging.getLogger("ThroughputStatistics").setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)


def construct_processor_parameters(input_args, parameters):
    """
    Based on the script input and eventual config file, construct the processor parameters.
    The attributes sources (which overrides which) is: input_args < config_file < parameters
    :param input_args: Input arguments from the ArgumentParser.
    :param parameters: Additional parameters to set.
    :return: Dictionary of processor parameters.
    """
    # Check if the binding address was provided.
    processor_parameters = {}
    if "binding_address" in input_args and input_args.binding_address:
        processor_parameters["binding_address"] = input_args.binding_address

    # Parameters in the config file override all other parameters parameters.
    if "config_file" in input_args and input_args.config_file:
        with open(input_args.config_file) as config_file:
            processor_parameters.update(json.load(config_file))

    # Parameters passed to the function override all other parameter sources.
    processor_parameters.update(parameters or {})

    return processor_parameters


def start_stream_node_helper(processor_instance, input_args, parameters, start_node_immediately=False):
    """
    Run the sream node by extracting common arguments from the ArgumentParser created namespace.
    :param processor_instance: Processor instance to pass to the stream node.
    :param input_args: Input arguments form the ArgumentParser.
    :param parameters: Additional processor parameters.
    :param start_node_immediately: Start node as soon as it is instantiated.
    """

    processor_parameters = construct_processor_parameters(input_args, parameters)

    if "rest_host" in input_args and input_args.rest_host:
        control_host = input_args.rest_host
    else:
        control_host = config.DEFAULT_REST_HOST

    if "rest_port" in input_args and input_args.rest_port:
        control_port = input_args.rest_port
    else:
        control_port = config.DEFAULT_REST_PORT

    if "raw" in input_args and input_args.raw:
        receive_raw = input_args.raw
    else:
        receive_raw = config.DEFAULT_REST_PORT

    start_stream_node(instance_name=input_args.instance_name,
                      processor=processor_instance,
                      processor_parameters=processor_parameters,
                      connection_address=input_args.connect_address,
                      control_host=control_host,
                      control_port=control_port,
                      receive_raw=receive_raw,
                      start_node_immediately=start_node_immediately)


def load_config_file(filename):
    if not filename:
        return {}

    abs_filename = os.path.abspath(os.path.expanduser(filename))

    # If the filename is not specified, None throws an exception, while "" simply return False.
    if os.access(abs_filename or "", os.R_OK):
        _logger.debug("Reading scripts config file '%s'." % abs_filename)
        with open(abs_filename) as file:
            file_config = json.load(file, object_pairs_hook=OrderedDict)
            for instance in file_config.values():
                instance["config_file"] = filename

            return file_config
    else:
        _logger.debug("Scripts config file not readable: '%s'." % abs_filename)
        return {}


def load_scripts_config(specified_config_file=None):
    """
    Load the scripts config on the current machine.
    :param specified_config_file: Additional config file, if needed. Otherwise, None.
    :return: Dictionary with config file.
    """
    manager_config = {}

    # From least to most important config:
    # Common machine config, user home folder config, current folder config, user specified config.
    manager_config.update(load_config_file(config.MANAGE_MACHINE_FILENAME))
    manager_config.update(load_config_file(config.MANAGE_USER_FILENAME))
    manager_config.update(load_config_file(config.MANAGE_PWD_FILENAME))
    manager_config.update(load_config_file(specified_config_file))

    if not manager_config:
        raise ValueError("No config files available. Checked files:\n'%s',\n'%s',\n'%s',\n'%s'" %
                         (config.MANAGE_MACHINE_FILENAME, config.MANAGE_USER_FILENAME,
                          config.MANAGE_PWD_FILENAME, specified_config_file or ""))

    return OrderedDict(sorted(manager_config.items()))


def get_instance_client_parameters(instance_name, config_file=None):
    """
    Return the parameters to construct a REST client.
    :param instance_name: Name of the instance to address.
    :param config_file: Additional config file to use.
    :return: (Instance name, control address)
    """
    instance_config = get_instance_config(instance_name, config_file)
    instance_name = instance_config["input_args"]["instance_name"]
    control_address = "%s:%s" % (instance_config["input_args"].get("rest_host", config.DEFAULT_REST_HOST),
                                 instance_config["input_args"].get("rest_port", config.DEFAULT_REST_PORT))
    return control_address, instance_name


def get_instance_config(instance_name, config_file=None):
    """
    Return the config of the specified instance.
    :param instance_name: Name of the instance.
    :param config_file: Additional config file.
    :return: Dictionary with the instance config.
    """
    scripts_config = load_scripts_config(config_file)

    instance_config = scripts_config.get(instance_name)
    if not instance_config:
        raise ValueError("The requested instance '%s' is not defined.\n"
                         "Available instances: %s" % (instance_name, list(scripts_config.keys())))

    return instance_config
