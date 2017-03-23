from argparse import ArgumentParser

import numpy as np
from types import SimpleNamespace

from mflow_nodes.stream_tools.mflow_forwarder import MFlowForwarder


def generate_frame_data(frame_shape, frame_number):
    """
    Generate a frame that is filled with the frame number value.
    :param frame_shape: Shape of the frame to generate.
    :param frame_number: Number to fill the frame with.
    """
    return np.full(shape=frame_shape, fill_value=frame_number, dtype=np.int32)


def generate_test_array_stream(binding_address="tcp://127.0.0.1:40000", frame_shape=(4, 4), number_of_frames=16):
    """
    Generate an array-1.0 stream of shape [4,4] and the specified number of frames.
    The values for each cell in the frame corresponds to the frame number.
    :param frame_shape: Shape (number of cells) of the frames to send.
    :param number_of_frames: Number of frames to send.
    :param binding_address: Address to bind the stream to.
    """
    print("Preparing to send %d frames of shape %s." % (number_of_frames, str(frame_shape)))

    mflow_forwarder = MFlowForwarder()
    mflow_forwarder.start(binding_address)

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
    parser = ArgumentParser()
    parser.add_argument("binding_address", type=str, help="Binding address for mflow connection.\n"
                                                          "Example: tcp://127.0.0.1:40001")
    parser.add_argument("--n_frames", type=int, default=16, help="Number of frames to generate.")
    parser.add_argument("--frame_size", type=int, default=4, help="Number of values X and Y direction, per frame.")
    input_args = parser.parse_args()

    try:
        generate_test_array_stream(input_args.binding_address, number_of_frames=input_args.n_frames,
                                   frame_shape=(input_args.frame_size, input_args.frame_size))
    except KeyboardInterrupt:
        print("Terminated by user.")
