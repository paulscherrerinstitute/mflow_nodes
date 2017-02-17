class MessageHandler(object):
    """
    Message handler for array-1.0
    """
    @staticmethod
    def get_header(message):
        return message.data["header"]

    @staticmethod
    def get_frame_index(message):
        return message.data["header"]["frame"]

    @staticmethod
    def get_data_length(message):
        return len(message.data["data"][0])

    @staticmethod
    def get_data(message):
        return message.data["data"][0]

    @staticmethod
    def get_frame_size(message):
        return message.data["header"]["shape"]

    @staticmethod
    def get_frame_dtype(message):
        return message.data["header"]["type"]
