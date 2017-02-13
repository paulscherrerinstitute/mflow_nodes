import os
from logging import getLogger

import h5py

DATASET_INITIAL_FRAME_COUNT = 100
DATASET_FRAMES_INCREASE_STEP = 100

_logger = getLogger(__name__)


def populate_h5_file(file, h5_group_attributes=None, h5_datasets=None, h5_dataset_attributes=None):
    set_group_attributes(file, h5_group_attributes or {})
    create_datasets_from_data(file, h5_datasets or {})
    set_dataset_attributes(file, h5_dataset_attributes or {})


def create_dataset(file, dataset_name, frame_size, dtype, compression=None, compression_opts=None):
    # Generate the dataset groups if needed.
    dataset_name = dataset_name.rstrip("/")
    dataset_group = "/".join(dataset_name.split("/")[:-1])

    if dataset_group:
        file.require_group(dataset_group)

    compression_opts = tuple(compression_opts) if compression_opts else None
    dataset = file.create_dataset(name=dataset_name,
                                  shape=[DATASET_INITIAL_FRAME_COUNT] + frame_size,
                                  maxshape=[None] + frame_size,
                                  chunks=tuple([1] + frame_size),
                                  dtype=dtype,
                                  compression=compression,
                                  compression_opts=compression_opts)
    return dataset


def create_datasets_from_data(file, datasets):
    for name, value in datasets.items():
        file.create_dataset(name, data=value)


def set_group_attributes(file, group_attributes):
    for name, value in group_attributes.items():
        group_name, attribute_name = name.split(":")

        group = file.require_group(group_name)
        group.attrs[attribute_name] = value


def set_dataset_attributes(file, dataset_attributes):
    for name, value in dataset_attributes.items():
        dataset_name, attribute_name = name.split(":")

        if dataset_name in file:
            dataset = file[dataset_name]
            dataset.attrs[attribute_name] = value
        else:
            raise ValueError("Dataset '%s' does not exist." % name)


def create_external_data_files_links(file, external_files):
    for file_path in external_files:
        filename = os.path.basename(file_path)
        link_name = "/entry/data/" + filename[filename.index("_data_") + 1:filename.rindex(".h5")]

        file[link_name] = h5py.ExternalLink(filename, "entry/data/data")


def expand_dataset(dataset, received_frame_index):
    new_dataset_size = received_frame_index + DATASET_FRAMES_INCREASE_STEP

    _logger.debug("Current dataset is to small (size=%d) for frame index '%d'. Resizing it to %d."
                  % (dataset.shape[0], received_frame_index, new_dataset_size))

    dataset.resize(size=new_dataset_size, axis=0)


def compact_dataset(dataset, max_frame_index):
    min_dataset_size = max_frame_index + 1
    _logger.debug("Compacting dataset to size=%d." % min_dataset_size)
    dataset.resize(size=min_dataset_size, axis=0)
