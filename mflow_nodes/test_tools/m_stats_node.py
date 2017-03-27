import logging
import sys
from argparse import ArgumentParser

from mflow.tools import ThroughputStatisticsPrinter

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
parser.add_argument("--sampling_interval", type=float, default=0.5, help="Stream sampling interval in seconds."
                                                                         "If zero, each message will be measured "
                                                                         "separately.")
input_args = parser.parse_args()

# Sampling rate must be positive, otherwise infinite loop.
if input_args.sampling_interval < 0:
    raise ValueError("Sampling interval cannot be less than zero.")


class StatisticsNode(BaseProcessor):
    def __init__(self, sampling_interval):
        self.sampling_interval = sampling_interval
        self._statistics = ThroughputStatisticsPrinter(sampling_interval=sampling_interval)

    def process_message(self, message):
        self._statistics.save_statistics(message.get_statistics())

    def stop(self):
        self._statistics.print_summary()


start_stream_node(instance_name=input_args.instance_name,
                  processor=StatisticsNode(input_args.sampling_interval),
                  connection_address=input_args.connect_address,
                  control_port=input_args.rest_port,
                  receive_raw=input_args.raw,
                  start_listener=True)
