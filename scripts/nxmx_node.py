import json
import logging
import sys
from argparse import ArgumentParser
from mflow_node.stream_node import start_stream_node
from mflow_processor.h5_nxmx_writer import HDF5nxmxWriter

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("mflow.mflow").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)

parser = ArgumentParser()
parser.add_argument("listening_address", type=str, help="Listening address for mflow connection.\n"
                                                        "Example: tcp://127.0.0.1:40001")
parser.add_argument("forwarding_address", type=str, help="Forwarding address for mflow connection.\n"
                                                         "Example: tcp://127.0.0.1:40001")
parser.add_argument("writer_control_address", type=str, help="URL of the H5 writer node REST Api.\n"
                                                             "Example: 127.0.0.1:40001")
parser.add_argument("--config_file", type=str, help="Config file with the detector properties.")
parser.add_argument("--rest_port", type=int, default=41000, help="Port for web interface.")
input_args = parser.parse_args()

# Read the processor parameters if provided.
parameters = {"filename": "ignore_experiment_master.h5",
              "frames_per_file": 9}

if input_args.config_file:
    with open(input_args.config_file) as config_file:
        parameters.update(json.load(config_file))

start_stream_node(processor=HDF5nxmxWriter(h5_writer_stream_address=input_args.forwarding_address,
                                           h5_writer_control_address=input_args.writer_control_address),
                  processor_parameters=parameters,
                  listening_address=input_args.listening_address,
                  control_port=input_args.rest_port)
