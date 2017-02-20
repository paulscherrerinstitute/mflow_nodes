class MessageHandler(object):
    """
    Message handler for dimage-1.0
    """
    @staticmethod
    def get_header(message):
        return message.data["header"]

    @staticmethod
    def get_frame_index(message):
        return message.data["header"]["frame"]

    @staticmethod
    def get_data(message):
        return message.data["part_3_raw"]

    @staticmethod
    def get_data_length(message):
        return message.data["part_2"]["size"]

    @staticmethod
    def get_frame_size(message):
        # Reverse the list, (X, Y) is received, but (Y, X) is needed by the writer.
        return message.data["part_2"]["shape"][::-1]

    @staticmethod
    def get_frame_dtype(message):
        return message.data["part_2"]["type"]