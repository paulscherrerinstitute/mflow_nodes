import logging
import sys
from argparse import ArgumentParser

import time

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
parser.add_argument("--sampling_interval", type=float, default=0.5, help="Stream sampling interval in seconds.")
input_args = parser.parse_args()

# Sampling rate must be positive, otherwise infinite loop.
if input_args.sampling_interval <= 0:
    raise ValueError("Sampling interval cannot be less or equal zero.")


class StatisticsNode(BaseProcessor):
    def __init__(self):
        self.previous_total_bytes_received = None
        self.previous_messages_received = None
        self.previous_time = None

    def start(self):
        # Initialize all the statistics values.
        self.previous_total_bytes_received = 0
        self.previous_messages_received = 0
        self.previous_time = time.time()

    def process_message(self, message):
        current_time = time.time()
        delta_time = current_time - self.previous_time

        # Update screen every 200ms.
        if delta_time > input_args.sampling_interval:
            # Calculate the rate in the last interval.
            total_bytes_received = message.raw_message.statistics.total_bytes_received
            messages_received = message.raw_message.statistics.messages_received

            data_rate = (total_bytes_received - self.previous_total_bytes_received) / delta_time
            message_rate = (messages_received - self.previous_messages_received) / delta_time

            output = "\rData rate: {data_rate: >10.3f} MB/s    Message rate: {message_rate: >10.3f} Hz"\
                .format(data_rate=data_rate/10**6, message_rate=message_rate)

            print(output)

            # Store the values for the next interval.
            self.previous_time = current_time
            self.previous_total_bytes_received = total_bytes_received
            self.previous_messages_received = messages_received

    def stop(self):
        total_mb = self.previous_total_bytes_received/10**6
        average_mb_message = total_mb / self.previous_messages_received

        print("_" * 60)
        print("Total MB received: %.3f" % total_mb)
        print("Total messages received: %d" % self.previous_messages_received)
        print("Average MB message size: %.3f" % average_mb_message)
        print("_" * 60)


start_stream_node(instance_name=input_args.instance_name,
                  processor=StatisticsNode(),
                  connection_address=input_args.connect_address,
                  control_port=input_args.rest_port,
                  receive_raw=input_args.raw,
                  start_listener=True)
