from argparse import ArgumentParser

from mflow.tools import ThroughputStatisticsPrinter

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.script_tools.helpers import setup_logging, add_default_arguments, start_stream_node_helper


class StatisticsNode(BaseProcessor):
    def __init__(self, sampling_interval):
        self.sampling_interval = sampling_interval
        self._statistics = ThroughputStatisticsPrinter(sampling_interval=sampling_interval)

    def process_message(self, message):
        self._statistics.save_statistics(message.get_statistics())

    def stop(self):
        self._statistics.print_summary()


def run(input_args, parameters=None):
    # Sampling rate must be positive or zero(sample each message), otherwise infinite loop.
    if input_args.sampling_interval < 0:
        raise ValueError("Sampling interval cannot be less than zero.")

    start_stream_node_helper(StatisticsNode(input_args.sampling_interval), input_args, parameters,
                             start_node_immediately=True)

if __name__ == "__main__":
    parser = ArgumentParser()
    add_default_arguments(parser, binding_argument=False)
    parser.add_argument("--sampling_interval", type=float, default=0.5, help="Stream sampling interval in seconds."
                                                                             "If zero, each message will be measured "
                                                                             "separately.")
    arguments = parser.parse_args()

    setup_logging(arguments.log_level)

    run(arguments)
