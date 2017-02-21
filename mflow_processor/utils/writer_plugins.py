def write_frame_index_to_dataset(target_dataset):
    """
    Create a separate dataset with the list of frame indexes.
    :param target_dataset: Dataset to store the frame indexes into.
    :return: Function to pass to the writer as a plugin.
    """
    def plugin(writer, message):
        # Append the frame index to the specified dataset.
        writer.h5_datasets.setdefault(target_dataset, []).append(message.get_frame_index())

    return plugin
