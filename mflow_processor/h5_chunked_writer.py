from logging import getLogger
from mflow_node.processor import StreamProcessor
import h5py

from mflow_processor.utils.h5_utils import populate_h5_file, create_dataset, compact_dataset, expand_dataset


class HDF5ChunkedWriterProcessor(StreamProcessor):
    """
    H5 chunked writer

    Writes the received stream to a HDF5 file.

    Writer commands:
        start                          Starts the writer, overwriting the output file if exists.
        stop                           Stop the writer, compact the dataset and close the file.

    Writer parameters:
        compression                    Filter number to be used. None for no compression.
        compression_opts               Options to pass to the compression filter. None is defautl.
        dataset_frames_increase_step   When dataset is full, increase its capacity for the step value.
        dataset_initial_frame_count    Number of pre-allocated frames in the dataset.
        dataset_name                   Name of the dataset to write the data inside the H5 file.
        dtype                          Data type of the stream.
        frame_size                     Size of a single frame in pixels.
        h5_attributes                  Attributes to add to the H5 file.
        h5_datasets                    Datasets to add to the H5 file.
        output_file                    Location to write the H5 file to.
    """
    _logger = getLogger(__name__)

    def __init__(self, name="H5 chunked writer"):
        self.__name__ = name

        self._file = None
        self._dataset = None
        self._max_frame_index = 0
        self._current_frame_chunk = None

        # H5 attributes to add to the file.
        self.h5_group_attributes = {}
        self.h5_dataset_attributes = {}
        self.h5_datasets = {}

        # Parameters to be set.
        self.dataset_name = None
        self.output_file = None
        self.frames_per_file = None
        self.frame_size = None
        self.dtype = None

        # Parameters with default values
        self.compression = None
        self.compression_opts = None

    def _validate_parameters(self):
        error_message = ""

        if not self.dataset_name:
            error_message += "Parameter 'dataset_name' not set.\n"

        if not self.frame_size:
            error_message += "Parameter 'frame_shape' not set.\n"

        if not self.dtype:
            error_message += "Parameter 'dtype' not set.\n"

        if self.dataset_initial_frame_count < 1:
            error_message += "Invalid values for parameter 'expected_frames_in_dataset'. Must be more than 0.\n"

        if self.dataset_frames_increase_step < 1:
            error_message += "Invalid values for parameter 'dataset_frames_increase_step'. Must be more than 0.\n"

        if not self.output_file:
            error_message += "Parameter 'output_file' not set.\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def start(self):
        self._logger.debug("Writer started.")
        # Check if all the needed input parameters are available.
        self._validate_parameters()
        self._logger.debug("Starting mflow_processor. Writing frames of size %s." % self.frame_size)

    def _create_file(self, frame_chunk=0):
        if self._file:
            self._close_file()

        filename = self.output_file.format(chunk_number=frame_chunk)
        self._logger.debug("Writing to file '%s' chunks of size %s." % (filename, self.frame_size))

        # Truncate file if it already exists.
        self._file = h5py.File(filename, "w")

        # Construct the dataset.
        self._dataset = create_dataset(self._file,
                                       self.dataset_name,
                                       self.frame_size,
                                       self.dtype,
                                       self.compression,
                                       self.compression_opts)

    def _close_file(self):
        compact_dataset(self._dataset, self._max_frame_index)

        # Do not display the index number, but the the frame number (starts with 1)
        if self.frames_per_file:
            min_frame_in_dataset = self._current_frame_chunk * self.frames_per_file
            max_frame_in_dataset = self._max_frame_index + min_frame_in_dataset
        else:
            max_frame_in_dataset = self._max_frame_index + 1
            min_frame_in_dataset = 1

        self._dataset.attrs["image_nr_high"] = max_frame_in_dataset
        self._dataset.attrs["image_nr_low"] = min_frame_in_dataset

        populate_h5_file(self._file, self.h5_group_attributes, self.h5_datasets, self.h5_dataset_attributes)

        self._file.close()
        self._file = None
        self._max_frame_index = 0

    def process_message(self, message):
        frame_header = message.data["header"]
        frame_data = message.data["data"][0]
        frame_index = frame_header["frame"]

        self._logger.debug("Received frame '%d'." % frame_header["frame"])

        # If the image does not belong to the current chunk, close the file and create a new one.
        if self.frames_per_file:
            frame_chunk = (frame_index // self.frames_per_file) + 1
            if not self._current_frame_chunk == frame_chunk:
                self._current_frame_chunk = frame_chunk
                self._create_file(frame_chunk)

            frame_index -= (frame_chunk - 1) * self.frames_per_file

        # If the current frame does not fit in the dataset, expand it.
        if not frame_index < self._dataset.shape[0]:
            expand_dataset(self._dataset, frame_index)

        bytes_to_write = frame_data if isinstance(frame_data, bytes) else frame_data.tobytes()
        self._dataset.id.write_direct_chunk((frame_index, 0, 0), bytes_to_write)

        # Keep track of the max frame index to shrink the dataset before closing it.
        self._max_frame_index = max(self._max_frame_index, frame_index)

        self._file.flush()

    def stop(self):
        self._logger.debug("Writer stopped.")
        self._close_file()
