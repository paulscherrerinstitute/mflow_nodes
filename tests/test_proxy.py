import json
import os
import unittest
from time import sleep

from mflow_nodes.processors.proxy import ProxyProcessor
from mflow_nodes.stream_node import ExternalProcessWrapper, get_zmq_listener
from mflow_nodes.test_tools.m_generate_test_stream import generate_test_array_stream
from tests.helpers import setup_file_writing_receiver

proxy_address = "tcp://127.0.0.1:40000"
writer_address = "tcp://127.0.0.1:40001"
output_filename = "test_proxy_output.txt"
number_of_frames = 16


class ProxyTest(unittest.TestCase):

    def setUp(self):
        # Setup receiving node.
        self.receiver = setup_file_writing_receiver(writer_address, output_filename)
        self.receiver.start()

        # Let only even frame numbers pass.
        def proxy_function(message):
            if message.get_frame_index() % 2 == 0:
                return True
            return False

        processor = ProxyProcessor(proxy_function=proxy_function)

        initial_parameters = {
            "forwarding_address": "tcp://127.0.0.1:40001"
        }
        self.proxy = ExternalProcessWrapper(get_zmq_listener(processor=processor,
                                                             listening_address=proxy_address),
                                            processor_instance=processor,
                                            initial_parameters=initial_parameters)
        self.proxy.start()

    def tearDown(self):
        self.proxy.stop()
        self.receiver.stop()
        # Remove the output file.
        if os.path.exists(output_filename):
            os.remove(output_filename)

    def test_proxy(self):
        """
        Test if forwarder transfers all frames and if they are in the correct order.
        """
        generate_test_array_stream(proxy_address, number_of_frames=number_of_frames)
        # Wait a bit for the transfer to complete.
        sleep(0.5)

        with open(output_filename, 'r') as input_file:
            test_data = json.load(input_file)

        even_frames = [x for x in range(number_of_frames) if x % 2 == 0]

        # Check if only even frames were transfered
        self.assertEqual(len(even_frames), len(test_data), "Unexpected number of frames.")

        for index, frame_index in enumerate(x for x in range(number_of_frames) if x % 2 == 0):
            # Check if they were transfered in the correct order.
            self.assertEqual(frame_index, test_data[index]["frame"], "Wrong frames were transfered.")
