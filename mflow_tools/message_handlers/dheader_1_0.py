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
    "virtual_pixel_correction_applied": "/entry/instrument/detector/virtaul_pixel_correction_applied",
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

        return data

    @staticmethod
    def get_frame_size(message):
        raise ValueError("No frame size in message.")

    @staticmethod
    def get_frame_dtype(message):
        raise ValueError("No dtype in message.")
