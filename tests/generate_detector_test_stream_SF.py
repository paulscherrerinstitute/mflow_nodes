from mflow import mflow, PUSH, json, sleep
import requests
import numpy as np
from datetime import datetime

# Test stream is of Array-1.0 type.
header = {
    "htype": "array-1.0",
    "type": "uint16",
    "pulse_id": 9989,
    "shape": [2048, 2048],
    "frame": 0,
    "is_good_frame": 1,
    "daq_rec": 10000,

    "missing_packets_1": [0, 0, 0, 0, 0, 0, 0, 0, 0],
    "missing_packets_2": [10, 10, 10, 10, 10, 10, 10, 10, 10],

    "daq_recs": [20, 20, 20, 20, 20, 20, 20, 20, 20],
    "framenum_diff": [-100, -100, -100, -100, -100, -100, -100, -100, -100],
    "pulse_ids": [10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000,
                  10000],
    "framenums": [100, 100, 100, 100, 100, 100, 100, 100, 100],
    "pulse_id_diff": [-1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000, -1000],
    "module_number": [0, 1, 2, 3, 4, 5, 6, 7, 8],

}

stream = mflow.connect("tcp://127.0.0.1:40000", conn_type="bind", mode=PUSH)

data = {
    "general/created": str(datetime.now()),
    "general/user": "owner",
    "general/process": "p12345",
    "general/instrument": "Jungfrau",
}

requests.post("http://0.0.0.0:10000/parameters", json=data)

for frame_number in range(10):
    header["frame"] = frame_number
    header["pulse_id"] = header["pulse_id"] + 1
    header["daq_rec"] = header["daq_rec"] + 1

    data = np.full(shape=header["shape"], fill_value=frame_number, dtype=header["type"])

    for index in range(9):
        header["missing_packets_1"][index] = header["missing_packets_1"][index] + 1
        header["missing_packets_2"][index] = header["missing_packets_2"][index] + 1
        header["daq_recs"][index] = header["daq_recs"][index] + 1
        header["framenum_diff"][index] = header["framenum_diff"][index] + 1
        header["pulse_ids"][index] = header["pulse_ids"][index] + 1
        header["framenums"][index] = header["framenums"][index] + 1
        header["pulse_id_diff"][index] = header["pulse_id_diff"][index] + 1
        header["module_number"][index] = header["module_number"][index] + 1

    stream.send(json.dumps(header).encode('utf-8'), send_more=True)

    stream.send(data.tobytes(), send_more=False)

    sleep(0.1)
