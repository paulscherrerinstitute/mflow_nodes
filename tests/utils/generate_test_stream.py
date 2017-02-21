from types import SimpleNamespace

import numpy as np

from mflow_node.processor import MFlowForwarder

address = "tcp://127.0.0.1:40000"

mflow_forwarder = MFlowForwarder()
mflow_forwarder.start(address)

# Test stream is of array type.
header = {"htype": "array-1.0",
          "type": "int32",
          "shape": [4, 4],
          "frame": 0}

# Send 16 4x4 frames. The values of each array cell is equal to the frame number.
for frame_number in range(16):
    header["frame"] = frame_number
    data = np.zeros(shape=(4, 4), dtype=np.int32) + frame_number

    print("Sending frame %d" % frame_number)

    message = SimpleNamespace()
    message.data = {"header": header, "data": [data]}
    mflow_forwarder.forward(message)

mflow_forwarder.stop()
