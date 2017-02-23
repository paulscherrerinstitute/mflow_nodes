import os

from mflow_node.stream_node import ExternalProcessWrapper, get_zmq_listener

default_frame_shape = (4, 4)
default_number_of_frames = 16
default_output_file = "ignore_test_output.h5"
default_dataset_name = "entry/dataset/data"


def setup_receiver(processor, parameters=None, listening_address="tcp://127.0.0.1:40000"):
    """
    Setup and start the receiver with default values, mainly for testing purposes.
    To be used in the setUp test method.
    :param processor: Processor instance.
    :param parameters: Processor parameters. Defaults (dataset_name, output_file) provided.
    :param listening_address: Listening address. Default "tcp://127.0.0.1:40000".
    :return: instance of ExternalProcessorWrapper.
    """

    # Setup the parameters for the processor.
    process_parameters = {"dataset_name": default_dataset_name,
                          "output_file": default_output_file}
    process_parameters.update(parameters or {})

    receiver_node = ExternalProcessWrapper(get_zmq_listener(processor=processor,
                                                            listening_address=listening_address),
                                           initial_parameters=process_parameters,
                                           processor_instance=processor)
    receiver_node.start()
    return receiver_node


def cleanup_receiver(receiver_node, files_to_cleanup=None):
    """
    Stop the receiver and delete the temporary files.
    To be used in the tearDown test method.
    :param receiver_node: ExternalProcessWrapper to cleanup.
    :param files_to_cleanup: Temporary files to delete (default already provided).
    """
    if receiver_node.is_running():
        receiver_node.stop()

    files_to_cleanup = files_to_cleanup or []
    files_to_cleanup.append(default_output_file)

    for file in files_to_cleanup:
        if os.path.exists(file):
            os.remove(file)
