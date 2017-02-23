import os
import h5py
import unittest
from time import sleep

from mflow_processor.h5_chunked_writer import HDF5ChunkedWriterProcessor
from tests.utils.generate_test_stream import generate_test_array_stream, generate_frame_data
from tests.utils.receiver_helper import setup_receiver, cleanup_receiver, default_output_file, default_dataset_name, \
    default_number_of_frames, default_frame_shape


class TransferTest(unittest.TestCase):
    def setUp(self):
        self.receiver_node = setup_receiver(processor=HDF5ChunkedWriterProcessor())

    def tearDown(self):
        cleanup_receiver(self.receiver_node)

    def test_transfer(self):
        """
        Check if the message pipeline works - from receiving to writing the message down in H5 format.
        """
        generate_test_array_stream(frame_shape=default_frame_shape, number_of_frames=default_number_of_frames)

        counter = 0
        # Statistics is stored in a shared namespace. It might take some time for synchronization.
        statistics = None
        while not statistics:
            statistics = self.receiver_node.get_statistics()
            sleep(0.1)
            counter += 1
            # ..but not more than 2 seconds..
            if counter > 20:
                raise Exception("Could not retrieve statistics in 2 seconds.")

        # Count the total number of received frames.
        self.assertEqual(statistics["total_frames"], default_number_of_frames, "Not all frames were transferred.")

        self.receiver_node.stop()

        # Check if the output file was written.
        self.assertTrue(os.path.exists(default_output_file), "Output file does not exist.")
        file = h5py.File(default_output_file, 'r')

        # Check if the dataset groups were correctly constructed.
        self.assertTrue(default_dataset_name in file, "Required dataset not present in output file.")

        # Check if the shrink procedure worked correctly.
        dataset = file[default_dataset_name]
        self.assertEqual(dataset.shape, (default_number_of_frames,) + default_frame_shape, "Dataset of incorrect size.")

        # Check if all the frames are correct.
        for frame_number in range(default_number_of_frames):
            self.assertTrue((dataset.value[frame_number] ==
                             generate_frame_data(default_frame_shape, frame_number)).all(),
                            "Dataset data does not match original data for frame %d." % frame_number)

if __name__ == '__main__':
    unittest.main()
