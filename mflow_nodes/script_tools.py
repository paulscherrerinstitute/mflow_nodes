import json
import logging

import sys

from mflow_nodes.stream_node import start_stream_node


def add_default_arguments(parser, binding_argument=False, default_rest_port=41000):
    """
    Adds the arguments every script needs
    :param parser: ArgumentParser instance to add the arguments to.
    :param binding_argument: If True, include the binding address to forward the strem.
    :param default_rest_port: The default rest port to use. Default: 41000
    """
    parser.add_argument("instance_name", type=str, help="Name of the node instance. Should be unique.")
    parser.add_argument("connect_address", type=str, help="Connect address for mflow receiver.\n"
                                                          "Example: tcp://127.0.0.1:40000")
    if binding_argument:
        parser.add_argument("binding_address", type=str, help="Binding address for mflow stream forwarding.\n"
                                                              "Example: tcp://127.0.0.1:40001")
    parser.add_argument("--config_file", type=str, default=None, help="Config file with the detector properties.")
    parser.add_argument("--raw", action='store_true', help="Receive and send mflow messages with raw handler.")
    parser.add_argument("--rest_port", type=int, default=default_rest_port, help="Port for web interface.")


def setup_console_logging(default_level=logging.DEBUG):
    """
    Most common set of logging configuration for debugging.
    :param default_level: Default logging level.
    """
    logging.basicConfig(stream=sys.stdout, level=default_level)
    logging.getLogger("mflow.mflow").setLevel(logging.ERROR)
    logging.getLogger("ThroughputStatistics").setLevel(logging.ERROR)


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
    if "binding_address" in input_args:
        processor_parameters["binding_address"] = input_args.binding_address

    # Parameters in the config file override all other parameters parameters.
    if input_args.config_file:
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

    start_stream_node(instance_name=input_args.instance_name,
                      processor=processor_instance,
                      processor_parameters=processor_parameters,
                      connection_address=input_args.connect_address,
                      control_port=input_args.rest_port,
                      receive_raw=input_args.raw,
                      start_node_immediately=start_node_immediately)
