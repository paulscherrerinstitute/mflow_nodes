from functools import partial
from logging import getLogger
from mflow_nodes.stream_tools.message_handlers import array_1_0, dheader_1_0, dimage_1_0, dseries_end_1_0

_logger = getLogger(__name__)

# Mapping of available handlers to the 'htype' header attribute.
handlers_mapping = {"array-1.0": array_1_0.MessageHandler,
                    "dheader-1.0": dheader_1_0.MessageHandler,
                    "dimage-1.0": dimage_1_0.MessageHandler,
                    "dseries_end-1.0": dseries_end_1_0.MessageHandler}


def get_mflow_message(message):
    """
    Wrap the mflow return message based on the message type.
    :return MflowMessage or None if no handler is available or message is None.
    """
    # Nothing to do here.
    if message is None:
        return None

    htype = message.data["header"]["htype"]
    handler = handlers_mapping.get(htype)
    if handler is None:
        _logger.warning("No handler for htype='%s' available. Dropping message." % htype)
        return None

    return MFlowMessage(message, handler, htype)


class MFlowMessage(object):
    """
    Wrap for the mflow message.
    """
    def __init__(self, message, handler, htype):
        self.raw_message = message
        self.htype = htype

        # Bind all the functions from the handler.
        self.get_header = partial(handler.get_header, self.raw_message)
        self.get_frame_index = partial(handler.get_frame_index, self.raw_message)
        self.get_data = partial(handler.get_data, self.raw_message)
        self.get_data_length = partial(handler.get_data_length, self.raw_message)
        self.get_frame_size = partial(handler.get_frame_size, self.raw_message)
        self.get_frame_dtype = partial(handler.get_frame_dtype, self.raw_message)

    def __str__(self):
        return str(self.get_header())
