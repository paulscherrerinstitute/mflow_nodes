import numpy as np

from mflow_node.processor import MFlowForwarder

address = "tcp://127.0.0.1:40000"

mflow_forwarder = MFlowForwarder()
mflow_forwarder.start(address)

header = {"htype": "array-1.0",
          "type": "int32",
          "shape": [4, 4],
          "frame": 0}

for frame_number in range(16):
    header["frame"] = frame_number
    data = np.zeros(shape=(4, 4), dtype=np.int32) + frame_number

    print("Sending frame %d" % frame_number)
    mflow_forwarder.send(header, data)

mflow_forwarder.stop()
