from logging import getLogger

from mflow_nodes.stream_tools.message_handlers import array_1_0, dheader_1_0, dimage_1_0, dseries_end_1_0, raw_1_0

_logger = getLogger(__name__)

# Mapping of available handlers to the 'htype' header attribute.
handlers_mapping = {"array-1.0": array_1_0.MessageHandler,
                    "dheader-1.0": dheader_1_0.MessageHandler,
                    "dimage-1.0": dimage_1_0.MessageHandler,
                    "dseries_end-1.0": dseries_end_1_0.MessageHandler,
                    "raw-1.0": raw_1_0.MessageHandler}


def get_mflow_message(message):
    """
    Wrap the mflow return message based on the message type.
    :return MflowMessage or None if no handler is available or message is None.
    """
    # Nothing to do here.
    if message is None:
        return None

    htype = message.data["header"]["htype"]
    try:
        handler = handlers_mapping[htype]
    except KeyError:
        _logger.warning("No handler for htype='%s' available. Dropping message." % htype)
        return None

    return MFlowMessage(message, handler, htype)


def get_raw_mflow_message(message):
    # Nothing to do here.
    if message is None:
        return None

    return MFlowMessage(message, handlers_mapping["raw-1.0"], message.data["header"]["htype"])


class MFlowMessage(object):
    """
    Wrap for the mflow message.
    """
    def __init__(self, message, handler, htype):
        self.raw_message = message
        self.handler = handler
        self.htype = htype

    def get_header(self):
        return self.handler.get_header(self.raw_message)

    def get_frame_index(self):
        return self.handler.get_frame_index(self.raw_message)

    def get_data(self):
        return self.handler.get_data(self.raw_message)

    def get_data_length(self):
        return self.handler.get_data_length(self.raw_message)

    def get_frame_size(self):
        return self.handler.get_frame_size(self.raw_message)

    def get_frame_dtype(self):
        return self.handler.get_frame_dtype(self.raw_message)

    def get_statistics(self):
        return self.raw_message.statistics

    def __str__(self):
        return str(self.get_header())
