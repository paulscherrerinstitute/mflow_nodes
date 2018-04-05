import logging
from time import sleep, time

import numpy
from multiprocessing import Process, Queue

import zmq
from mflow import Stream, mflow

_logger = logging.getLogger(__name__)

READ_POLLING_INTERVAL = 0.1
ZMQ_IO_THREADS = 1


class RingBuffer(object):

    READ_INSTANCE = 0
    WRITE_INSTANCE = 1

    SLOT_EMPTY = 0
    SLOT_FULL = 1

    def __init__(self, n_slots, slot_bytes, buffer_file='ringbuffer_data.dat', header_file='ringbuffer_header.dat'):
        self.slot_bytes = slot_bytes
        self.n_slots = n_slots

        self.buffer_file = buffer_file
        self.header_file = header_file

        self.ring_buffer = None
        self.header_buffer = None

        self.instance_type = None
        self._next_index = None

        _logger.info("Setup ring buffer data file '%s' and header file '%s' with n_slots=%s and slot_bytes=%s." %
                     (self.buffer_file, self.header_file, self.n_slots, self.slot_bytes))

    def initialize_for_writing(self):
        buffer_size = self.slot_bytes * self.n_slots

        _logger.info("Initializing the ring buffer for writing. buffer_size=%s" % buffer_size)

        self.ring_buffer = numpy.memmap(self.buffer_file, mode='w+', dtype=numpy.byte, shape=buffer_size)
        self.header_buffer = numpy.memmap(self.header_file, mode='w+', dtype=numpy.byte, shape=self.n_slots)

        self.instance_type = RingBuffer.WRITE_INSTANCE
        self._next_index = 0

    def initialize_for_reading(self):
        buffer_size = self.slot_bytes * self.n_slots

        _logger.info("Initializing the ring buffer for reading. buffer_size=%s" % buffer_size)

        self.ring_buffer = numpy.memmap(self.buffer_file, mode='r', dtype=numpy.byte, shape=buffer_size)
        self.header_buffer = numpy.memmap(self.header_file, mode='w+', dtype=numpy.byte, shape=self.n_slots)

        self.instance_type = RingBuffer.READ_INSTANCE
        self._next_index = 0

    def _get_slot_interval_indexes(self, data_index):
        slot_start_index = data_index * self.slot_bytes
        slot_end_index = slot_start_index + self.slot_bytes

        return slot_start_index, slot_end_index

    def _get_slot_index(self, data_index):

        if data_index is None:
            data_index = self._next_index

            # Be sure to wrap around at the end of the buffer.
            self._next_index = (self._next_index + 1) % self.n_slots

        return data_index

    def read(self, timeout=None, data_index=None):

        if self.ring_buffer is None or self.header_buffer is None:
            raise Exception("RingBuffer not initialized. Call 'initialize_for_writing' or 'initialize_for_reading'.")

        data_index = self._get_slot_index(data_index)

        start_time = time()
        while self.header_buffer[data_index] == RingBuffer.SLOT_EMPTY:
            if timeout is not None and time() - start_time > timeout:
                raise TimeoutError("Slot still empty. Read timeout exceeded.")

            sleep(READ_POLLING_INTERVAL)

        slot_start_index, slot_end_index = self._get_slot_interval_indexes(data_index)

        data_view = self.ring_buffer[slot_start_index:slot_end_index]

        return data_index, data_view

    def release(self, data_index):
        if self.header_buffer[data_index] == 0:
            raise ValueError("Buffer slot at index '%s' already released." % data_index)

        self.header_buffer[data_index] = 0

    def write(self, data, data_index=None):
        if self.ring_buffer is None or self.header_buffer is None:
            raise Exception("RingBuffer not initialized. Call 'initialize_for_writing' or 'initialize_for_reading'.")

        if not isinstance(data, bytes):
            raise ValueError("Only bytes can be written to the ring buffer. Variable data should be of type bytes.")

        data_n_bytes = len(data)
        if data_n_bytes > self.slot_bytes:
            raise ValueError("Trying to store %s bytes in a %s slot." % (data_n_bytes, self.slot_bytes))

        data_index = self._get_slot_index(data_index)

        if self.header_buffer[data_index] == RingBuffer.SLOT_FULL:
            raise ValueError("Buffer slot at index '%s' already full." % data_index)

        slot_start_index, slot_end_index = self._get_slot_interval_indexes(data_index)

        self.ring_buffer[slot_start_index:slot_end_index] = numpy.frombuffer(data, dtype=numpy.byte)

        self.header_buffer[data_index] = 1


def processor_function(header_queue, ring_buffer):
    ring_buffer.initialize_for_reading()

    while True:
        header = header_queue.get()
        data_index, data = ring_buffer.read()

        print("Received ring buffer index '%s' with data '%s' bytes.", data_index, len(data))
        print("header index ", header['index'])
        print("First byte %s and last byte %s" % (data[0], data[-1]))
        print('----------------------------')

        ring_buffer.release(data_index)


def receiver_function(connection_address, header_queue, ring_buffer):
    ring_buffer.initialize_for_writing()

    # Setup the ZMQ listener and the stream mflow_processor.
    context = zmq.Context(io_threads=ZMQ_IO_THREADS)

    stream = Stream()
    stream.connect(address=connection_address,
                   conn_type=mflow.CONNECT,
                   mode=mflow.PULL,
                   context=context)
    index = 0
    while True:
        raw_message = stream.receive_raw()

        header = raw_message["header"]
        data = raw_message["data"][0]

        index += 1

        if header is not None and data is not None:
            header_queue.put(header)
            ring_buffer.write(data)

        sleep(0.01)


header_queue = Queue()
ring_buffer = RingBuffer(100, 1024 * 1024 * 1)

process_receiver = Process(target=processor_function, args=(header_queue, ring_buffer))
process_receiver.start()

receiver_function(None, header_queue, ring_buffer)
process_receiver.terminate()
