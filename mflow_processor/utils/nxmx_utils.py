import os
from logging import getLogger

import h5py
import numpy as np

from mflow_processor.utils.h5_utils import _logger

_logger = getLogger(__name__)

MASTER_FILENAME_SUFFIX = "_master.h5"
DATA_FILENAME_TEMPLATE = "{experiment_id}_data_{{chunk_number:06d}}.h5"
NUMBER_OF_FRAMES_FROM_HEADER = "/entry/instrument/detector/detectorSpecific/nimages"


def create_external_data_files_links(file, external_files):
    """
    Create links to external datasets (in external files).
    :param file: File handle to create the external links on.
    :param external_files: List of external files.
    """
    for file_path in external_files:
        filename = os.path.basename(file_path)
        link_name = "/entry/data/" + filename[filename.index("_data_") + 1:filename.rindex(".h5")]

        file[link_name] = h5py.ExternalLink(filename, "entry/data/data")


def convert_header_to_dataset_values(header_data, image_count):
    """
    Given the data from the header, generate the needed H5 datasets for the master file.
    :param header_data: Data from the header.
    :param image_count: Number of received images.
    :return: Dictionary of dataset values.
    """
    attributes = {}
    attributes_to_calculate = {}

    # Process all items in the header.
    for name, value in header_data.items():
        if name in header_to_h5_attributes_mapping:
            # Just a normal parameter mapping.
            attributes[header_to_h5_attributes_mapping[name]] = value
        elif name in header_calculated_attributes:
            # The angle will have to be calculated for all frames.
            attribute_base_name = header_calculated_attributes[name]
            values_for_calculation = attributes_to_calculate.get(attribute_base_name, [None, None])

            # Defines the start value for the attribute.
            if name.endswith("_start"):
                values_for_calculation[0] = value
            # Defines the increment for the attribute.
            else:
                values_for_calculation[1] = value

            attributes_to_calculate[attribute_base_name] = values_for_calculation
        else:
            _logger.warning("Attribute '%s' in header was not processed." % name)

    # Process all attributes that need calculation.
    for name, value in attributes_to_calculate.items():
        min_value = value[0]
        step = value[1]

        if step == 0:
            max_value = min_value
            steps_array = np.full(image_count, max_value)
        else:
            max_value = min_value + (image_count * step)
            steps_array = np.arange(min_value, max_value, step)

        # All the angle datasets have the same form.
        attributes[name + "_start"] = min_value
        attributes[name + "_range_total"] = max_value - min_value
        attributes[name + "_increment"] = step
        attributes[name + "_range_average"] = step
        attributes[name] = steps_array
        attributes[name + "_end"] = steps_array + step

    return attributes


header_calculated_attributes = {
    "two_theta_start": "/entry/instrument/detector/goniometer/two_theta",
    "two_theta_increment": "/entry/instrument/detector/goniometer/two_theta",
    "chi_start": "/entry/sample/goniometer/chi",
    "chi_increment": "/entry/sample/goniometer/chi",
    "omega_start": "/entry/sample/goniometer/omega",
    "omega_increment": "/entry/sample/goniometer/omega",
    "kappa_start": "/entry/sample/goniometer/kappa",
    "kappa_increment": "/entry/sample/goniometer/kappa",
    "phi_start": "/entry/sample/goniometer/phi",
    "phi_increment": "/entry/sample/goniometer/phi"
}

header_to_h5_attributes_mapping = {
    "number_of_excluded_pixels": "/entry/instrument/detector/detectorSpecific/number_of_excluded_pixels",
    "detector_translation": "/entry/instrument/detector/geometry/translation/distances",
    "data_collection_date": "/entry/instrument/detector/detectorSpecific/data_collection_date",
    "frame_period": "/entry/instrument/detector/detectorSpecific/frame_period",
    "pixel_mask_applied": "/entry/instrument/detector/pixel_mask_applied",
    "threshold_energy": "/entry/instrument/detector/threshold_energy",
    "frame_count_time": "/entry/instrument/detector/detectorSpecific/frame_count_time",
    "beam_center_y": "/entry/instrument/detector/beam_center_y",
    "software_version": "/entry/instrument/detector/detectorSpecific/software_version",
    "countrate_correction_applied": "/entry/instrument/detector/countrate_correction_applied",
    "flatfield_correction_applied": "/entry/instrument/detector/flatfield_correction_applied",
    "virtual_pixel_correction_applied": "/entry/instrument/detector/virtual_pixel_correction_applied",
    "detector_number": "/entry/instrument/detector/detector_number",
    "count_time": "/entry/instrument/detector/count_time",
    "element": "/entry/instrument/detector/detectorSpecific/element",
    "frame_time": "/entry/instrument/detector/frame_time",
    "summation_nimages": "/entry/instrument/detector/detectorSpecific/summation_nimages",
    "x_pixels_in_detector": "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector",
    "efficiency_correction_applied": "/entry/instrument/detector/efficiency_correction_applied",
    "sensor_thickness": "/entry/instrument/detector/sensor_thickness",
    "y_pixel_size": "/entry/instrument/detector/y_pixel_size",
    "wavelength": "/entry/instrument/beam/incident_wavelength",
    "detector_orientation": "/entry/instrument/detector/geometry/orientation/value",
    "detector_readout_time": "/entry/instrument/detector/detector_readout_time",
    "beam_center_x": "/entry/instrument/detector/beam_center_x",
    "bit_depth_readout": "/entry/instrument/detector/bit_depth_readout",
    "countrate_correction_bunch_mode": "/entry/instrument/detector/detectorSpecific/countrate_correction_bunch_mode",
    "trigger_mode": "/entry/instrument/detector/detectorSpecific/trigger_mode",
    "x_pixel_size": "/entry/instrument/detector/x_pixel_size",
    "calibration_type": "/entry/instrument/detector/detectorSpecific/calibration_type",
    "description": "/entry/instrument/detector/description",
    "nframes_sum": "/entry/instrument/detector/detectorSpecific/nframes_sum",
    "y_pixels_in_detector": "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector",
    "nimages": "/entry/instrument/detector/detectorSpecific/nimages",
    "detector_distance": "/entry/instrument/detector/detector_distance",
    "detector_readout_period": "/entry/instrument/detector/detectorSpecific/detector_readout_period",
    "photon_energy": "/entry/instrument/detector/detectorSpecific/photon_energy",
    "ntrigger": "/entry/instrument/detector/detectorSpecific/ntrigger",
    "sensor_material": "/entry/instrument/detector/sensor_material",
    "auto_summation": "/entry/instrument/detector/detectorSpecific/auto_summation"
}

dataset_types = {
    "/entry/instrument/detector/goniometer/two_theta_end": "float32",
    "/entry/instrument/detector/goniometer/two_theta_increment": "float32",
    "/entry/instrument/detector/goniometer/two_theta_range_average": "float32",
    "/entry/instrument/detector/goniometer/two_theta_range_total": "float32",
    "/entry/instrument/detector/goniometer/two_theta_start": "float32",
    "/entry/sample/goniometer/chi": "float32",
    "/entry/sample/goniometer/chi_end": "float32",
    "/entry/sample/goniometer/chi_increment": "float32",
    "/entry/sample/goniometer/chi_range_average": "float32",
    "/entry/sample/goniometer/chi_range_total": "float32",
    "/entry/sample/goniometer/chi_start": "float32",
    "/entry/sample/goniometer/kappa": "float32",
    "/entry/sample/goniometer/kappa_end": "float32",
    "/entry/sample/goniometer/kappa_increment": "float32",
    "/entry/sample/goniometer/kappa_range_average": "float32",
    "/entry/sample/goniometer/kappa_range_total": "float32",
    "/entry/sample/goniometer/kappa_start": "float32",
    "/entry/sample/goniometer/omega": "float32",
    "/entry/sample/goniometer/omega_end": "float32",
    "/entry/sample/goniometer/omega_increment": "float32",
    "/entry/sample/goniometer/omega_range_average": "float32",
    "/entry/sample/goniometer/omega_range_total": "float32",
    "/entry/sample/goniometer/omega_start": "float32",
    "/entry/sample/goniometer/phi": "float32",
    "/entry/sample/goniometer/phi_end": "float32",
    "/entry/sample/goniometer/phi_increment": "float32",
    "/entry/sample/goniometer/phi_range_average": "float32",
    "/entry/sample/goniometer/phi_range_total": "float32",
    "/entry/sample/goniometer/phi_start": "float32",
    "/entry/instrument/beam/incident_wavelength": "float32",
    "/entry/instrument/detector/beam_center_x": "float32",
    "/entry/instrument/detector/beam_center_y": "float32",
    "/entry/instrument/detector/bit_depth_image": "int32",
    "/entry/instrument/detector/bit_depth_readout": "int32",
    "/entry/instrument/detector/count_time": "float32",
    "/entry/instrument/detector/countrate_correction_applied": "int32",
    "/entry/instrument/detector/description": "S",
    "/entry/instrument/detector/detectorSpecific/auto_summation": "int32",
    "/entry/instrument/detector/detectorSpecific/calibration_type": "S",
    "/entry/instrument/detector/detectorSpecific/compression": "S",
    "/entry/instrument/detector/detectorSpecific/countrate_correction_bunch_mode": "S",
    "/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff": "uint32",
    "/entry/instrument/detector/detectorSpecific/data_collection_date": "S",
    "/entry/instrument/detector/detectorSpecific/detector_readout_period": "float32",
    "/entry/instrument/detector/detectorSpecific/eiger_fw_version": "S",
    "/entry/instrument/detector/detectorSpecific/element": "S",
    "/entry/instrument/detector/detectorSpecific/flatfield": "float32",
    "/entry/instrument/detector/detectorSpecific/frame_count_time": "float32",
    "/entry/instrument/detector/detectorSpecific/frame_period": "float32",
    "/entry/instrument/detector/detectorSpecific/module_bandwidth": "uint32",
    "/entry/instrument/detector/detectorSpecific/nframes_sum": "uint32",
    "/entry/instrument/detector/detectorSpecific/nimages": "uint32",
    "/entry/instrument/detector/detectorSpecific/nsequences": "uint32",
    "/entry/instrument/detector/detectorSpecific/ntrigger": "uint32",
    "/entry/instrument/detector/detectorSpecific/number_of_excluded_pixels": "uint32",
    "/entry/instrument/detector/detectorSpecific/photon_energy": "float32",
    "/entry/instrument/detector/detectorSpecific/pixel_mask": "uint32",
    "/entry/instrument/detector/detectorSpecific/roi_mode": "S",
    "/entry/instrument/detector/detectorSpecific/software_version": "S",
    "/entry/instrument/detector/detectorSpecific/summation_nimages": "uint32",
    "/entry/instrument/detector/detectorSpecific/test_mode": "int32",
    "/entry/instrument/detector/detectorSpecific/trigger_mode": "S",
    "/entry/instrument/detector/detectorSpecific/x_pixels_in_detector": "uint32",
    "/entry/instrument/detector/detectorSpecific/y_pixels_in_detector": "uint32",
    "/entry/instrument/detector/detector_distance": "float32",
    "/entry/instrument/detector/detector_number": "S",
    "/entry/instrument/detector/detector_readout_time": "float32",
    "/entry/instrument/detector/efficiency_correction_applied": "int32",
    "/entry/instrument/detector/flatfield_correction_applied": "int32",
    "/entry/instrument/detector/frame_time": "float32",
    "/entry/instrument/detector/geometry/orientation/value": "float32",
    "/entry/instrument/detector/geometry/translation/distances": "float32",
    "/entry/instrument/detector/goniometer/two_theta": "float32",
    "/entry/instrument/detector/pixel_mask_applied": "int32",
    "/entry/instrument/detector/sensor_material": "S",
    "/entry/instrument/detector/sensor_thickness": "float32",
    "/entry/instrument/detector/threshold_energy": "float32",
    "/entry/instrument/detector/virtaul_pixel_correction_applied": "int32",
    "/entry/instrument/detector/x_pixel_size": "float32",
    "/entry/instrument/detector/y_pixel_size": "float32"
}