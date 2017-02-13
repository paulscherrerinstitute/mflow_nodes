import glob
import json
import os
import h5py
import requests
from urllib.parse import urljoin

from mflow_processor.utils.h5_utils import create_external_data_files_links, populate_h5_file
from mflow_rest_api.rest_interface import RestInterfacedProcess

MASTER_FILENAME_SUFFIX = "_master.h5"
DATA_FILENAME_TEMPLATE = "{experiment_id}_data_{{chunk_number:06d}}.h5"


class HDF5nxmxWriter(RestInterfacedProcess):
    """
    H5 NXMX Master file writer

    Writes the master file for a NXMX compatible stream.

    Writer commands:
        start                          Starts the writer, configure the H5 file.
        stop                           Stop the writer, generate master H5 file.

    Writer parameters:
        To be defined.
    """

    def __init__(self, h5_writer_address, parameters=None, name="H5 NXMX master writer"):
        self.__name__ = name
        self._file = None
        self._is_running = False
        self._data_filename_format = None

        # Parameters that need to be set.
        self.h5_writer_address = h5_writer_address
        self.filename = None
        self.frames_per_file = None

        self.h5_group_attributes = {}
        self.h5_datasets = {}
        self.h5_dataset_attributes = {}

        # Set the initial parameters.
        for parameter in (parameters or {}).items():
            self.set_parameter(parameter)

    def _validate_parameters(self):
        error_message = ""

        if not self.h5_writer_address:
            error_message += "Parameter 'h5_writer_address' not set.\n"

        if not self.filename:
            error_message += "Parameter 'master_file_format' not set.\n"

        if not self.frames_per_file:
            error_message += "Parameter 'frames_per_file' not set.\n"

        if not self.h5_group_attributes:
            error_message += "Parameter 'h5_group_attributes' not set.\n"

        if not self.h5_datasets:
            error_message += "Parameter 'h5_datasets' not set.\n"

        if not self.h5_dataset_attributes:
            error_message += "Parameter 'h5_dataset_attributes' not set.\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def start(self):
        if self.is_running():
            raise ValueError("Writer already running.")

        self._validate_parameters()

        master_filename = os.path.abspath(self.filename)
        output_path = os.path.dirname(master_filename)
        experiment_id = master_filename[0:master_filename.rindex(MASTER_FILENAME_SUFFIX)]
        self._data_filename_format = os.path.join(output_path,
                                                  DATA_FILENAME_TEMPLATE.format(experiment_id=experiment_id))

        h5_writer_parameters = {"output_file": self._data_filename_format,
                                "dataset_name": "entry/data/data",
                                "frames_per_file": self.frames_per_file,
                                "h5_group_attributes": {"/entry:NX_class": "NXentry",
                                                        "/entry/data:NX_class": "NXdata"}}

        headers = {'content-type': 'application/json'}
        set_parameters_url = urljoin(self.h5_writer_address, "/parameters")
        response = requests.post(set_parameters_url,
                                 data=json.dumps(h5_writer_parameters),
                                 headers=headers).json()
        if response["status"] != "ok":
            raise ValueError("Cannot set writer parameters. Original error:%s\n" % response["message"])

        start_command_url = urljoin(self.h5_writer_address, "/start")
        response = requests.get(start_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot start writer. Original error:%s\n" % response["message"])

        # Create a master file.
        self._file = h5py.File(master_filename, "w")

        self._is_running = True

    def stop(self):
        if not self.is_running():
            raise ValueError("Writer not running.")

        # Request the start from the writer.
        stop_command_url = urljoin(self.h5_writer_address, "/stop")
        response = requests.get(stop_command_url).json()
        if response["status"] != "ok":
            raise ValueError("Cannot stop writer. Original error:%s\n" % response["message"])

        # Link the generated output files.
        files_to_link = glob.glob("%s*.h5" % self._data_filename_format[0:self._data_filename_format.rindex("%")])
        create_external_data_files_links(self._file, files_to_link)

        populate_h5_file(self._file, self.h5_group_attributes, self.h5_datasets, self.h5_dataset_attributes)

        self._file.close()
        self._is_running = False

    def is_running(self):
        return self._is_running

    def set_parameter(self, parameter):
        parameter_name = parameter[0]
        parameter_value = parameter[1]

        # If the parameter is a dictionary, update it.
        if isinstance(parameter_value, dict):
            current_dictionary = getattr(self, parameter_name, {})
            current_dictionary.update(parameter_value)
            setattr(self, parameter_name, current_dictionary)
        else:
            setattr(self, parameter_name, parameter_value)
