from logging import getLogger

import numpy as np

# Initial size of the dataset (number of frames).
DATASET_INITIAL_FRAME_COUNT = 100
# Step for resizing the dataset.
DATASET_FRAMES_INCREASE_STEP = 100

_logger = getLogger(__name__)


def populate_h5_file(file, h5_group_attributes=None, h5_datasets=None,
                     h5_dataset_attributes=None, dataset_dtypes=None):
    """
    Set all datasets and attributes on the provided H5 file.
    :param file: file to apply the changes to.
    :param h5_group_attributes: Group attributes.
    :param h5_datasets: Datasets values.
    :param h5_dataset_attributes: Dataset attributes.
    :param dataset_dtypes: Mapping of datasets to desired dtypes.
    :return:
    """
    set_group_attributes(file, h5_group_attributes or {})
    create_datasets_from_data(file, h5_datasets or {}, dataset_dtypes)
    set_dataset_attributes(file, h5_dataset_attributes or {})


def create_dataset(file, dataset_name, frame_size, dtype, compression=None, compression_opts=None,
                   initial_frame_count=DATASET_INITIAL_FRAME_COUNT):
    """
    Create a dataset on the provided file.
    :param file: File to create the dataset on.
    :param dataset_name: Name of the dataset.
    :param frame_size: Size of each frame in the dataset.
    :param dtype: Datatype to create the dataset.
    :param compression: Compression to use on the dataset.
    :param compression_opts: Compression options.
    :param initial_frame_count: Initial frame count for the dataset. Default is 100.
    :return: Dataset handle.
    """
    # Generate the dataset groups if needed.
    dataset_name = dataset_name.rstrip("/")
    dataset_group = "/".join(dataset_name.split("/")[:-1])

    if dataset_group:
        file.require_group(dataset_group)

    compression_opts = tuple(compression_opts) if compression_opts else None
    dataset = file.create_dataset(name=dataset_name,
                                  shape=[initial_frame_count] + frame_size,
                                  maxshape=[None] + frame_size,
                                  chunks=tuple([1] + frame_size),
                                  dtype=dtype,
                                  compression=compression,
                                  compression_opts=compression_opts)
    return dataset


def create_datasets_from_data(file, datasets, dataset_dtypes=None):
    """
    Create dataset from value.
    :param file: File to create a dataset on.
    :param datasets: Data to create the dataset from.
    :param dataset_dtypes: Mapping of datasets to desired dtypes.
    """
    for name, value in datasets.items():
        if dataset_dtypes and name in dataset_dtypes:
            dtype = dataset_dtypes[name]
            if dtype == "S":
                file.create_dataset(name, data=np.string_(value))
            else:
                file.create_dataset(name, data=value, dtype=dtype)
        else:
            file.create_dataset(name, data=value)


def set_group_attributes(file, group_attributes):
    """
    Set the group attributes.
    :param file: File handle to set the group attributes on.
    :param group_attributes: Group attributes to set.
    """
    for name, value in group_attributes.items():
        group_name, attribute_name = name.split(":")

        group = file.require_group(group_name)
        group.attrs[attribute_name] = np.string_(value)


def set_dataset_attributes(file, dataset_attributes):
    """
    Set the dataset attributes.
    :param file: File handle to set the dataset attributes on.
    :param dataset_attributes: Dataset attributes to set.
    """
    for name, value in dataset_attributes.items():
        dataset_name, attribute_name = name.split(":")

        if dataset_name in file:
            dataset = file[dataset_name]
            # Strings should be of fixed length.
            if isinstance(value, str):
                value = np.string_(value)

            dataset.attrs[attribute_name] = value
        else:
            raise ValueError("Dataset '%s' does not exist." % name)


def expand_dataset(dataset, received_frame_index, increase_step=DATASET_FRAMES_INCREASE_STEP):
    """
    Expand an existing dataset.
    :param dataset: Dataset to expand.
    :param received_frame_index: Last received frame index.
    :param increase_step: Optional. Default is 100.
    """
    new_dataset_size = received_frame_index + increase_step

    _logger.debug("Current dataset is to small (size=%d) for frame index '%d'. Resizing it to %d."
                  % (dataset.shape[0], received_frame_index, new_dataset_size))

    dataset.resize(size=new_dataset_size, axis=0)


def compact_dataset(dataset, max_frame_index):
    """
    Shrink the existing dataset.
    :param dataset: Dataset to shrink.
    :param max_frame_index: Max frame index in dataset.
    """
    min_dataset_size = max_frame_index + 1
    _logger.debug("Compacting dataset to size=%d." % min_dataset_size)
    dataset.resize(size=min_dataset_size, axis=0)
