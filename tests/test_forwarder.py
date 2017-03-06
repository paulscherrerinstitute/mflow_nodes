import json
import os
import unittest
from time import sleep

from mflow_nodes.test_tools.m_generate_test_stream import generate_test_array_stream
from tests.helpers import setup_file_writing_receiver

writer_address = "tcp://127.0.0.1:40000"
output_filename = "test_forwarder_output.txt"
number_of_frames = 16


class ForwarderTest(unittest.TestCase):

    def setUp(self):
        self.receiver = setup_file_writing_receiver(writer_address, output_filename)
        self.receiver.start()

    def tearDown(self):
        self.receiver.stop()
        # Remove the output file.
        if os.path.exists(output_filename):
            os.remove(output_filename)

    def test_forwarder(self):
        """
        Test if forwarder transfers all frames and if they are in the correct order.
        """
        generate_test_array_stream(writer_address, number_of_frames=number_of_frames)
        # Wait a bit for the transfer to complete.
        sleep(0.5)

        with open(output_filename, 'r') as input_file:
            test_data = json.load(input_file)

        # Check if all the frames were transfered.
        self.assertEqual(number_of_frames, len(test_data), "Not all frames were transfered.")

        for index, frame in enumerate(test_data):
            # Check if they were transfered in the correct order.
            self.assertEqual(index, frame["frame"], "Frames transfered out of order.")
