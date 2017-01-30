from logging import getLogger
from mflow_node.processor import StreamProcessor
import h5py


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
        self.h5_dataset_attributes = {}
        self.h5_group_attributes = {}

        # Parameters to be set.
        self.dataset_name = None
        self.output_file = None
        self.frame_size = None
        self.dtype = None

        # Parameters with default values
        self.dataset_initial_frame_count = 1
        self.dataset_frames_increase_step = 1000
        self.compression = None
        self.compression_opts = None

    def _validate_parameters(self):
        error_message = ""

        if not self.dataset_name:
            error_message += "Parameter 'dataset_name' not set.\n"

        if not self.output_file:
            error_message += "Parameter 'output_file' not set.\n"

        if not self.frame_size:
            error_message += "Parameter 'frame_shape' not set.\n"

        if not self.dtype:
            error_message += "Parameter 'dtype' not set.\n"

        if self.dataset_initial_frame_count < 1:
            error_message += "Invalid values for parameter 'expected_frames_in_dataset'. Must be more than 0.\n"

        if self.dataset_frames_increase_step < 1:
            error_message += "Invalid values for parameter 'dataset_frames_increase_step'. Must be more than 0.\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def start(self):
        self._logger.debug("Writer started.")
        # Check if all the needed input parameters are available.
        self._validate_parameters()
        self._logger.debug("Starting mflow_processor. Writing to file '%s' chunks of size %s." % (self.output_file,
                                                                                            self.frame_size))
        # Truncate file if it already exists.
        self._file = h5py.File(self.output_file, "w")

        # Generate the dataset groups if needed.
        self.dataset_name = self.dataset_name.rstrip("/")
        dataset_group = "/".join(self.dataset_name.split("/")[:-1])

        if dataset_group and dataset_group not in self._file:
            self._file.create_group(dataset_group)

        self._dataset = self._file.create_dataset(name=self.dataset_name,
                                                  shape=[self.dataset_initial_frame_count] + self.frame_size,
                                                  maxshape=[None] + self.frame_size,
                                                  chunks=tuple([1] + self.frame_size),
                                                  dtype=self.dtype,
                                                  compression=self.compression,
                                                  compression_opts=tuple(self.compression_opts))

    def _resize_dataset(self, received_frame_index):
        new_dataset_size = received_frame_index + self.dataset_frames_increase_step

        self._logger.debug("Current dataset is to small (size=%d) for frame index '%d'. Resizing it to %d."
                           % (self._dataset.shape[0], received_frame_index, new_dataset_size))

        self._dataset.resize(size=new_dataset_size, axis=0)

    def process_message(self, message):
        frame_header = message.data["header"]
        frame_data = message.data["data"][0]
        frame_index = frame_header["frame"]

        self._logger.debug("Received frame '%d'." % frame_header["frame"])

        # If the current frame does not fit in the dataset, expand it.
        if not frame_index < self._dataset.shape[0]:
            self._resize_dataset(frame_index)

        bytes_to_write = frame_data if isinstance(frame_data, bytes) else frame_data.tobytes()
        self._dataset.id.write_direct_chunk((frame_header["frame"], 0, 0), bytes_to_write)

        # Keep track of the max frame index to shrink the dataset before closing it.
        self._max_frame_index = max(self._max_frame_index, frame_index)

        self._file.flush()

    def stop(self):
        self._logger.debug("Writer stopped.")

        # Compact the dataset before closing it.
        min_dataset_size = self._max_frame_index + 1
        self._logger.debug("Compacting dataset to size=%d." % min_dataset_size)
        self._dataset.resize(size=min_dataset_size, axis=0)

        # TODO: Figure out how to properly write dataset, groups, and file attributes.
        # # Set dataset attributes.
        # if self.h5_attributes:
        #     attribute_group =
        #     for name, value in self.h5_metadata.items():
        #         self._logger.debug("Set dataset attribute: '%s':'%s'." % (name, value))
        #         self._dataset.attrs[name] = value

        self._file.close()
