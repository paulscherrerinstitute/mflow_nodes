import importlib
import json
import subprocess
from argparse import ArgumentParser, Namespace

from mflow_nodes import NodeClient
from mflow_nodes.script_tools.helpers import load_scripts_config, get_instance_config, get_instance_client_parameters


def run(instance_name, config_file=None):
    """
    Run the node instance.
    :param instance_name: Name of the instance.
    :param config_file: Additional config file to search for the instance.
    """
    instance_config = get_instance_config(instance_name, config_file)

    # Module name and run arguments are mandatory.
    module_name = instance_config["module_to_run"]
    run_arguments = instance_config["input_args"]
    # Parameters are optional.
    parameters = instance_config.get("parameters", {})

    try:
        script_module = importlib.import_module(module_name)
    except ImportError as e:
        raise ValueError("Unable to load module '%s'.\n%s" % (module_name, e))

    script_module.run(Namespace(**run_arguments), parameters)


def list_nodes(config_file=None, verbose=False):
    """
    List the available node configurations.
    :param config_file: Additional config file to search for the instance.
    :param verbose: Print complete config for each node.
    """
    if not verbose:
        print("Instance name (module_to_run):")

    for instance_name, instance_config in load_scripts_config(config_file).items():
        if verbose:
            print("Instance name: %s" % instance_name)
            print(json.dumps(instance_config, indent=4))
            print("-" * 60)
        else:
            print("\t %s (%s)" % (instance_name, instance_config["module_to_run"]))


def start(instance_name, config_file=None):
    """
    Start the processor on the defined instance.
    :param instance_name: Name of the instance to start the processor on.
    :param config_file: Additional config file to search for the instance.
    """
    address, name = get_instance_client_parameters(instance_name, config_file)
    client = NodeClient(address, name)
    client.start()


def stop(instance_name, config_file=None):
    """
    Stop the processor on the defined instance.
    :param instance_name: Name of the instance to stop the processor on.
    :param config_file: Additional config file to search for the instance.
    """
    address, name = get_instance_client_parameters(instance_name, config_file)
    client = NodeClient(address, name)
    client.stop()


def client_info(instance_name, config_file=None):
    """
    Returns the info needed to instantiate a client.
    :param instance_name: Name of the instance to get the client info.
    :param config_file: Additional config file to search for the instance.
    :return: String to start a Node Client for the specified instance.
    """
    address, name = get_instance_client_parameters(instance_name, config_file)
    return '%s = NodeClient(address="%s", instance_name="%s")' % (instance_name, address, name)


def client(instance_name, config_file=None):
    """
    Returns the info needed to instantiate a client.
    :param instance_name: Name of the instance to get the client for.
    :param config_file: Additional config file to search for the instance.
    :return: IPython with the node client activated.
    """
    client_command = client_info(instance_name, config_file)
    ipython_command = "ipython -i -c '" \
                      "from mflow_nodes import NodeClient;" \
                      "{client_command};" \
                      "print();" \
                      "print({client_command_string})'".format(client_command=client_command,
                                                               client_command_string=json.dumps(client_command))
    subprocess.call(ipython_command, shell=True)


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("--config_file", type=str, default=None, help="Additional config file to search for instances.")
    sub_parsers = parser.add_subparsers(help="Available commands:", dest="command")

    parser_list = sub_parsers.add_parser("list", help="List the available nodes from the config.")
    parser_list.add_argument("-v", "--verbose", action='store_true', help="Print details about each instance.")

    parser_run = sub_parsers.add_parser("run", help="Run a node instance.")
    parser_run.add_argument("instance_name", type=str, help="Name of the node instance to run from the config.")

    parser_start = sub_parsers.add_parser("start", help="Start a processor inside a running node.")
    parser_start.add_argument("instance_name", type=str, help="Name of the instance to start the processor on.")

    parser_stop = sub_parsers.add_parser("stop", help="Stop the processor inside a running node.")
    parser_stop.add_argument("instance_name", type=str, help="Name of the instance to stop the processor on.")

    parser_stop = sub_parsers.add_parser("client-info", help="Get client connection parameters.")
    parser_stop.add_argument("instance_name", type=str, help="Name of the instance to get the info.")

    parser_stop = sub_parsers.add_parser("client", help="Start IPython with the node client.")
    parser_stop.add_argument("instance_name", type=str, help="Name of the instance to get the client for.")

    input_args = parser.parse_args()

    try:

        if input_args.command == "list":
            list_nodes(input_args.config_file, verbose=input_args.verbose)
        elif input_args.command == "run":
            run(input_args.instance_name, input_args.config_file)
        elif input_args.command == "start":
            start(input_args.instance_name, input_args.config_file)
        elif input_args.command == "stop":
            stop(input_args.instance_name, input_args.config_file)
        elif input_args.command == "client-info":
            print(client_info(input_args.instance_name, input_args.config_file))
        elif input_args.command == "client":
            client(input_args.instance_name, input_args.config_file)
        else:
            parser.print_help()

    except ValueError as e:
        print(e)
