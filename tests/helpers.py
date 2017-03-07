import json

from mflow_nodes.processors.base import BaseProcessor
from mflow_nodes.stream_node import ExternalProcessWrapper, get_zmq_listener


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
                                                       connection_address=listening_address),
                                      processor_instance=processor)

    return receiver
