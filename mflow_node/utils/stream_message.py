from functools import partial
from logging import getLogger

_logger = getLogger(__name__)

class Handler_array_1_0(object):
    def get_frame_index(self, message):
        return message.data["header"]["frame"]

    def get_data_length(self, message):
        len(message.data["data"][0])

    def get_data(self, message):
        return message.data["data"][0]

    def get_frame_size(self, message):
        return message.data["header"]["shape"]

    def get_frame_dtype(self, message):
        return message.data["header"]["type"]


class Handler_dimage_1_0(object):
    def get_frame_index(self, message):
        return message.data["header"]["frame"]

    def get_data(self, message):
        return message.data["part_3_raw"]

    def get_data_length(self, message):
        return message.data["part_2"]["size"]

    def get_frame_size(self, message):
        return message.data["part_2"]["shape"]

    def get_frame_dtype(self, message):
        return message.data["part_2"]["type"]


class StreamMessageWrapper(object):
    def __init__(self, message, handler):
        self.message = message

        self.get_frame_index = partial(handler.get_frame_index, self.message)
        self.get_data = partial(handler.get_data, self.message)
        self.get_data_length = partial(handler.get_data_length, self.message)
        self.get_frame_size = partial(handler.get_frame_size, self.message)
        self.get_frame_dtype = partial(handler.get_frame_dtype, self.message)


def get_message(message):
    if message:
        htype = message.data["header"]["htype"]
        if htype == "dimage-1.0":
            handler = Handler_dimage_1_0()
        elif htype == "array-1.0":
            handler = Handler_array_1_0()
        else:
            _logger.warning("No handler found for message type '%s'." % message.data["header"]["htype"])
            return None

        return StreamMessageWrapper(message, handler)
    return None
