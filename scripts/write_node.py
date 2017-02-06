import logging
import sys
from argparse import ArgumentParser
from bitshuffle.h5 import H5_COMPRESS_LZ4
from mflow_node.stream_node import start_stream_node
from mflow_processor.h5_chunked_writer import HDF5ChunkedWriterProcessor

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logging.getLogger("mflow.mflow").setLevel(logging.ERROR)

parser = ArgumentParser()
parser.add_argument("listening_address", type=str, help="Listening address for mflow connection.\n"
                                                        "Example: tcp://127.0.0.1:40001")
parser.add_argument("output_file", type=str, help="Name of output h5 file to write.")
parser.add_argument("--rest_port", type=int, default=8081, help="Port for web interface.")
input_args = parser.parse_args()

parameters = {"dataset_name": "data",
              "output_file": input_args.output_file,
              "frame_size": [4, 4],
              "dtype": "int32"
              # ,"compression": 32008
              # ,"compression_opts": (2048, H5_COMPRESS_LZ4)
              }

start_stream_node(processor=HDF5ChunkedWriterProcessor(),
                  processor_parameters=parameters,
                  listening_address=input_args.listening_address,
                  control_port=input_args.rest_port,
                  receive_raw=False,
                  start_listener=False
                  )
