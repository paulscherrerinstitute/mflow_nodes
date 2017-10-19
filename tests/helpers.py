import json

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.stream_node import get_processor_function, get_receiver_function
from mflow_nodes.node_manager import NodeManager


def setup_file_writing_receiver(connect_address, output_filename):
    """
    Setup a node that writis the message headers into an output file for later inspection.
    :param connect_address: Address the node connects to.
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
    receiver = NodeManager(processor_function=get_processor_function(processor=processor,
                                                                     connection_address=connect_address),
                           receiver_function=get_receiver_function(connection_address=connect_address),
                           processor_instance=processor)

    return receiver
