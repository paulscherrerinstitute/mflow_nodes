import json

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.stream_node import ExternalProcessWrapper, get_zmq_listener
from mflow_nodes.stream_node import start_stream_node


def setup_file_writing_receiver(listening_address, output_filename):
    """
    Setup a node that writis the message headers into an output file for later inspection.
    :param listening_address: Address the node listens on.
    :param output_filename: Output file.
    :return: Instance of ExternalProcessWrapper.
    """
    # Format the output file.
    with open(output_filename, 'w') as output_file:
        output_file.write("[]")

    def process_message(message):
        with open(output_filename, 'r') as input_file:
            test_data = json.load(input_file)

        test_data.append(message.get_header())

        with open(output_filename, 'w') as output:
            output.write(json.dumps(test_data, indent=4))

    processor = BaseProcessor()
    processor.process_message = process_message
    receiver = ExternalProcessWrapper(get_zmq_listener(processor=processor,
                                                       listening_address=listening_address),
                                      processor_instance=processor)

    return receiver


def start_base_stream_node(listening_address="tcp://127.0.0.1:40000", instance_name="base"):
    """
    Start an instance of base stream node and attach the rest interface.
    :param listening_address: Address to listen for the stream.
    :param instance_name: Name of the node instance.
    """
    processor = BaseProcessor()
    start_stream_node(instance_name, processor, listening_address=listening_address)


if __name__ == "__main__":
    start_base_stream_node()
