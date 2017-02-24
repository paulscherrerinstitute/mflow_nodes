class MessageHandler(object):
    """
    Message handler for dseries_end-1.0
    """
    @staticmethod
    def get_header(message):
        return message.data["header"]

    @staticmethod
    def get_frame_index(message):
        return -1

    @staticmethod
    def get_data_length(message):
        return message.data.__sizeof__()

    @staticmethod
    def get_data(message):
        return None

    @staticmethod
    def get_frame_size(message):
        raise ValueError("No frame size in message.")

    @staticmethod
    def get_frame_dtype(message):
        raise ValueError("No dtype in message.")
