class MessageHandler(object):
    """
    Message handler for raw-1.0
    """
    @staticmethod
    def get_header(message):
        return message.data["header"]

    @staticmethod
    def get_frame_index(message):
        return message.data["header"]["frame"]

    @staticmethod
    def get_data_length(message):
        return sum(len(data) for data in message.data)

    @staticmethod
    def get_data(message):
        # Usually there is only 1 data message, in this case do not return a list.
        if len(message.data["data"]) == 1:
            return message.data["data"][0]
        else:
            return message.data["data"]

    @staticmethod
    def get_frame_size(message):
        return message.data["header"]["shape"]

    @staticmethod
    def get_frame_dtype(message):
        return message.data["header"]["type"]
