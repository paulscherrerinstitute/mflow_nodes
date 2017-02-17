import glob
import os
from logging import getLogger

import h5py

from mflow_node.processor import StreamProcessor, MFlowForwarder
from mflow_processor.utils.h5_utils import create_external_data_files_links, populate_h5_file
from mflow_rest_api import rest_client

MASTER_FILENAME_SUFFIX = "_master.h5"
DATA_FILENAME_TEMPLATE = "{experiment_id}_data_{{chunk_number:06d}}.h5"


class HDF5nxmxWriter(StreamProcessor):
    """
    H5 NXMX Master file writer

    Writes the master file for a NXMX compatible stream.

    Writer commands:
        start                          Starts the writer, configure the H5 file.
        stop                           Stop the writer, generate master H5 file.

    Writer parameters:
        To be defined.
    """
    _logger = getLogger(__name__)

    def __init__(self, h5_writer_stream_address, h5_writer_control_address, name="H5 NXMX master writer"):
        """
        Initialize the NXMX writer.
        :param h5_writer_stream_address: H5 writer stream address to forward the stream to.
        :param h5_writer_control_address: H5 writer control address to control the writer.
        :param name: Name of the writer.
        """
        self.__name__ = name
        self._file = None
        self._is_running = False
        self._data_filename_format = None
        self._zmq_forwarder = None
        self._h5_writer_stream_address = h5_writer_stream_address
        self._h5_writer_control_address = h5_writer_control_address

        # Parameters that need to be set.
        self.filename = None

        # Parameters with default values.
        self.frames_per_file = 100
        self.h5_group_attributes = {}
        self.h5_datasets = {}
        self.h5_dataset_attributes = {}

    def _validate_parameters(self):
        error_message = ""

        if not self.filename:
            error_message += "Parameter 'master_file_format' not set.\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def start(self):
        self._validate_parameters()

        # Start the forwarder.
        self._zmq_forwarder = MFlowForwarder()
        self._zmq_forwarder.start(self._h5_writer_stream_address)

        # Extract the experiment id and output folder from the master file name.
        master_filename = os.path.abspath(self.filename)
        output_path = os.path.dirname(master_filename)
        experiment_id = master_filename[0:master_filename.rindex(MASTER_FILENAME_SUFFIX)]
        self._data_filename_format = os.path.join(output_path,
                                                  DATA_FILENAME_TEMPLATE.format(experiment_id=experiment_id))

        # Construct the parameters for the H5 writer.
        h5_writer_parameters = {"output_file": self._data_filename_format,
                                "dataset_name": "entry/data/data",
                                "frames_per_file": self.frames_per_file,
                                "h5_group_attributes": {"/entry:NX_class": "NXentry",
                                                        "/entry/data:NX_class": "NXdata"}}

        rest_client.set_parameters(self._h5_writer_control_address, h5_writer_parameters)
        rest_client.start(self._h5_writer_control_address)

        # Create a master file.
        self._file = h5py.File(master_filename, "w")

        self._is_running = True

    def stop(self):
        # Stop the writer.
        rest_client.stop(self._h5_writer_control_address)

        # Link the generated output files.
        files_to_link = glob.glob("%s*.h5" % self._data_filename_format[0:self._data_filename_format.rindex("{")])
        create_external_data_files_links(self._file, files_to_link)

        # Set the dataset and attributes in the h5 master file.
        populate_h5_file(self._file, self.h5_group_attributes, self.h5_datasets, self.h5_dataset_attributes)

        self._zmq_forwarder.stop()
        self._file.close()
        self._is_running = False

    def _process_header_attributes(self, header_data):
        self._logger.debug("Processing header message attributes.")

    def process_message(self, message):
        if message.htype.startswith("dimage-"):
            self._zmq_forwarder.forward(message.raw_message)
        elif message.htype.startswith("dseries_end-"):
            self._logger.debug("End series message received.")
            self.stop()
        elif message.htype.startswith("dheader-"):
            self._logger.debug("Header message received.")
            self._process_header_attributes(message.get_data())
        else:
            self._logger.debug("Skipping message of type '%s'." % message.htype)
