import os
import unittest
from time import sleep
from types import SimpleNamespace

import h5py
import numpy as np

from mflow_node.processor import MFlowForwarder
from mflow_node.stream_node import ExternalProcessWrapper, get_zmq_listener
from mflow_processor.h5_chunked_writer import HDF5ChunkedWriterProcessor

listening_address = "tcp://127.0.0.1:40000"
output_file = "ignore_output.h5"
dataset_name = "entry/dataset/data"


def _get_frame_data(frame_number):
    return np.zeros(shape=(4, 4), dtype=np.int32) + frame_number


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self._sender_node = MFlowForwarder()
        self._sender_node.start(listening_address)

        self._processor = HDF5ChunkedWriterProcessor()

        self._parameters = {"dataset_name": dataset_name,
                            "output_file": output_file}

        self._receiver_node = ExternalProcessWrapper(get_zmq_listener(processor=self._processor,
                                                                      listening_address=listening_address),
                                                     initial_parameters=self._parameters,
                                                     processor_instance=self._processor)

    def tearDown(self):

        self._sender_node.stop()

        if self._receiver_node.is_running():
            self._receiver_node.stop()

        if os.path.exists(output_file):
            os.remove(output_file)

        self._sender_node.stop()

    def _prepare_receiver(self):
        """
        Just restart (or start) the receiver.
        :return:
        """
        if self._receiver_node.is_running():
            self._receiver_node.stop()

        self._receiver_node.start()

    def test_data_transfer(self):
        self._prepare_receiver()

        number_of_frames = 16
        frame_shape = [4, 4]

        header = {"htype": "array-1.0",
                  "type": "int32",
                  "shape": frame_shape,
                  "frame": 0}

        for frame_number in range(number_of_frames):
            header["frame"] = frame_number
            data = _get_frame_data(frame_number)

            message = SimpleNamespace()
            message.data = {"header": header, "data": [data]}
            self._sender_node.forward(message)

        counter = 0
        # Statistics is stored in a shared namespace. It might take some time for synchronization.
        statistics = None
        while not statistics:
            statistics = self._receiver_node.get_statistics()
            sleep(0.1)
            counter += 1

            if counter > 20:
                raise Exception("Could not retrieve statistics in 2 seconds.")

        # Count the total number of received frames.
        self.assertEqual(statistics["total_frames"], number_of_frames, "Not all frames were transferred.")

        self._receiver_node.stop()

        # Check if the output file was written.
        self.assertTrue(os.path.exists(output_file), "Output file does not exist.")
        file = h5py.File(output_file, 'r')

        # Check if the dataset groups were correctly constructed.
        self.assertTrue(dataset_name in file, "Required dataset not present in output file.")

        # Check if the shrink procedure worked correctly.
        dataset = file[dataset_name]
        self.assertEqual(dataset.shape, tuple([number_of_frames] + frame_shape), "Dataset of incorrect size.")

        # Check if all the frames are correct.
        for frame_number in range(number_of_frames):
            self.assertTrue((dataset.value[frame_number] == _get_frame_data(frame_number)).all(),
                            "Dataset data does not match original data for frame %d." % frame_number)


if __name__ == '__main__':
    unittest.main()
