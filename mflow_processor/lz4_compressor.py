import json
import struct

import bitshuffle.h5

from logging import getLogger

from mflow_processor.proxy import ProxyProcessor


class LZ4CompressionProcessor(ProxyProcessor):
    """
    LZ4 bitshuffle compression

    Compresses the provided stream and forwards it to the next node.

    Compressor commands:
        start                          Starts the compressor.
        stop                           Stop the compressor.

    Compressor parameters:
        block_size                     Size to use for the LZ4 compression.
        forwarding_address             Address to forward the stream to.
    """
    _logger = getLogger(__name__)

    def __init__(self, name="LZ4 bitshuffle compression"):
        super().__init__(self, name=name)

        self.block_size = 2048

    def _validate_parameters(self):
        error_message = ""

        if not self.block_size:
            error_message += "Parameter 'block_size' not set.\n"

        if not self.forwarding_address:
            error_message += "Parameter 'forwarding_address' not set.\n"

        if error_message:
            self._logger.error(error_message)
            raise ValueError(error_message)

    def _compress_lz4(self, header, data):
        def compress_as_chunk(array, block_size):
            compressed_bytes = bitshuffle.compress_lz4(array, block_size)
            bytes_number_of_elements = struct.pack('>q', (array.shape[0] * array.shape[1] * array.dtype.itemsize))
            bytes_block_size = struct.pack('>i', block_size * array.dtype.itemsize)
            all_bytes = bytes_number_of_elements + bytes_block_size + compressed_bytes.tobytes()
            return all_bytes

        compressed_data = compress_as_chunk(data, self.block_size)

        new_header = header.copy()
        new_header["encoding"] = "bitshuffle_lz4"

        return new_header, compressed_data

    def process_message(self, message):
        frame_header = message.data["header"]
        frame_data = message.data["data"][0]

        self._logger.debug("Received frame '%d'." % frame_header["frame"])
        new_header, compressed_bytes = self._compress_lz4(frame_header, frame_data)

        self._zmq_forwarder.stream.send(json.dumps(new_header).encode(), send_more=True, block=True)
        self._zmq_forwarder.stream.send(compressed_bytes, block=True)
