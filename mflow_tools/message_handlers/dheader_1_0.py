class MessageHandler(object):
    """
    Message handler for dheader-1.0
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
        data = {}
        data.update(message.data.get("part_2"))
        data.update(message.data.get("appendix", {}))

        if MessageHandler.get_header(message)["header_detail"] == "all":
            data.update(message.data["part_3"])
            data.update(message.data["part_4_raw"])
            data.update(message.data["part_5"])
            data.update(message.data["part_6_raw"])
            data.update(message.data["part_7"])
            data.update(message.data["part_8_raw"])

    @staticmethod
    def get_frame_size(message):
        raise ValueError("No frame size in message.")

    @staticmethod
    def get_frame_dtype(message):
        raise ValueError("No dtype in message.")
