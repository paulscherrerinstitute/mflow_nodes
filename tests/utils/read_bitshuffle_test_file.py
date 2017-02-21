import h5py
import bitshuffle.h5

filename = "../scripts/temp_output.h5"

path = "/group1/group2/group3/dataset/"

file = h5py.File(filename, "r")
print(file['data'])
print(file['data'][0])
print(file['data'][15])

file.close()
