import logging
import sys
from argparse import ArgumentParser

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.stream_node import start_stream_node

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("mflow.mflow").setLevel(logging.ERROR)

parser = ArgumentParser()
parser.add_argument("instance_name", type=str, help="Name of the node instance. Should be unique.")
parser.add_argument("connect_address", type=str, help="Connect address for mflow.\n"
                                                      "Example: tcp://127.0.0.1:40000")
parser.add_argument("--rest_port", type=int, default=8080, help="Port for web interface.")
parser.add_argument("--raw", action='store_true', help="Receive the mflow messages in raw mode.")
input_args = parser.parse_args()

start_stream_node(instance_name=input_args.instance_name,
                  processor=BaseProcessor(),
                  connection_address=input_args.connect_address,
                  control_port=input_args.rest_port,
                  receive_raw=input_args.raw)
