import json
import logging
import sys
from argparse import ArgumentParser
from mflow_node.rest_node import start_rest_node
from mflow_processor.h5_nxmx_writer import HDF5nxmxWriter

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("mflow.mflow").setLevel(logging.ERROR)

parser = ArgumentParser()
parser.add_argument("writer_address", type=str, help="URL of the H5 writer node REST Api.\n"
                                                     "Example: 127.0.0.1:40001")
parser.add_argument("--config_file", type=str, help="Config file with the detector properties.")
parser.add_argument("--rest_port", type=int, default=8080, help="Port for web interface.")
input_args = parser.parse_args()

# Read the processor parameters if provided.
processor_parameters = {"filename": "test_master.h5",
                        "frames_per_file": 4}


if input_args.config_file:
    with open(input_args.config_file) as config_file:
        processor_parameters.update(json.load(config_file))

start_rest_node(processor=HDF5nxmxWriter(h5_writer_address=input_args.writer_address,
                                         parameters=processor_parameters),
                control_port=input_args.rest_port)
