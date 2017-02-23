from types import SimpleNamespace

import numpy as np
from mflow_node.processor import MFlowForwarder


def generate_frame_data(frame_shape, frame_number):
    """
    Generate a frame that is filled with the frame number value.
    :param frame_shape: Shape of the frame to generate.
    :param frame_number: Number to fill the frame with.
    """
    return np.full(shape=frame_shape, fill_value=frame_number, dtype=np.int32)


def generate_test_array_stream(target_address="tcp://127.0.0.1:40000", frame_shape=(4, 4), number_of_frames=16):
    """
    Generate an array-1.0 stream of shape [4,4] and the specified number of frames.
    The values for each cell in the frame corresponds to the frame number.
    :param frame_shape: Shape (number of cells) of the frames to send.
    :param number_of_frames: Number of frames to send.
    :param target_address: Where to send the stream to.
    """
    mflow_forwarder = MFlowForwarder()
    mflow_forwarder.start(target_address)

    # Test stream is of array type.
    header = {"htype": "array-1.0",
              "type": "int32",
              "shape": list(frame_shape)}

    # Send 16 4x4 frames. The values of each array cell is equal to the frame number.
    for frame_number in range(number_of_frames):
        header["frame"] = frame_number
        data = generate_frame_data(frame_shape, frame_number)

        print("Sending frame %d" % frame_number)

        message = SimpleNamespace()
        message.data = {"header": header, "data": [data]}
        mflow_forwarder.forward(message)

    mflow_forwarder.stop()

if __name__ == "__main__":
    generate_test_array_stream("tcp://127.0.0.1:40000")
