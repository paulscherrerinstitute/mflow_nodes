from mflow import mflow, PUSH, json
import requests
import numpy as np

# Test stream is of Array-1.0 type.
header = {"htype": "array-1.0",
          "type": "uint32",
          "pulse_id": 0,
          "shape": [2048, 2048]}

stream = mflow.connect("tcp://127.0.0.1:40000", conn_type="bind", mode=PUSH)

data = {
    "bpm4_gain_setting": 1.0,
    "bpm4_saturation_value": 1.0,
    "bpm4s": 1.0,
    "bpm4x": 1.0,
    "bpm4y": 1.0,
    "bpm4z": 1.0,
    "bpm5_gain_setting": 1.0,
    "bpm5_saturation_value": 1.0,
    "bpm5s": 1.0,
    "bpm5x": 1.0,
    "bpm5y": 1.0,
    "bpm5z": 1.0,
    "bpm6_gain_setting": 1.0,
    "bpm6_saturation_value": 1.0,
    "bpm6s": 1.0,
    "bpm6x": 1.0,
    "bpm6y": 1.0,
    "bpm6z": 1.0,
    "bs1_det_dist": 1.0,
    "bs1_status": "placeholder text",
    "bs1x": 1.0,
    "bs1y": 1.0,
    "bs2_det_dist": 1.0,
    "bs2_status": "placeholder text",
    "bs2x": 1.0,
    "bs2y": 1.0,
    "curr": 1.0,
    "date": "placeholder text",
    "diode": 1.0,
    "fil_comb_description": "placeholder text",
    "ftrans": 1.0,
    "harmonic": 1,
    "idgap": 1.0,
    "mibd": 1.0,
    "mirror_coating": "placeholder text",
    "mith": 1.0,
    "mobd": 1.0,
    "mokev": 1.0,
    "moth1": 1.0,
    "sample_description": "placeholder text",
    "sample_name": "placeholder text",
    "samx": 1.0,
    "samy": 1.0,
    "samz": 1.0,
    "scan": "placeholder text",
    "sec": 1.0,
    "sl0ch": 1.0,
    "sl0wh": 1.0,
    "sl1ch": 1.0,
    "sl1cv": 1.0,
    "sl1wh": 1.0,
    "sl1wv": 1.0,
    "sl2ch": 1.0,
    "sl2cv": 1.0,
    "sl2wh": 1.0,
    "sl2wv": 1.0,
    "sl3ch": 1.0,
    "sl3cv": 1.0,
    "sl3wh": 1.0,
    "sl3wv": 1.0,
    "sl4ch": 1.0,
    "sl4cv": 1.0,
    "sl4wh": 1.0,
    "sl4wv": 1.0,
    "temp": 1.0,
    "temp_mono_cryst_1": 1.0,
    "temp_mono_cryst_2": 1.0
}

requests.post("http://0.0.0.0:10000/parameters", json=data)

for frame_number in range(10):

    header["frame"] = frame_number
    header["pulse_id"] = frame_number + 10000
    data = np.full(shape=header["shape"], fill_value=frame_number, dtype=header["type"])

    stream.send(json.dumps(header).encode('utf-8'), send_more=True)

    stream.send(data.tobytes(), send_more=False)
