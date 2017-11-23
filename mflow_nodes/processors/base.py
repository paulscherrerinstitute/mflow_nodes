from logging import getLogger


class BaseProcessor(object):
    """
    Just a stub, for actual stream processors to extend.
    """
    _logger = getLogger(__name__)

    def start(self):
        """
        Start the stream mflow_processor.
        :return: None.
        """
        self._logger.debug("Starting mflow_processor.")

    def process_message(self, message):
        """
        Process the message received over ZMQ.
        :param message: Message received from the ZMQ stream.
        :return: None
        """
        self._logger.debug("Received message.")

    def set_parameter(self, parameter):
        """
        Set the parameter received from the REST API.
        :param parameter: Tuple of format (parameter_name, parameter_value).
        :return: None
        """

        # Parameters can only be received in tuple format: (parameter_name, parameter_value)
        if not isinstance(parameter, tuple):
            error = "Received parameter '%s' value is not in tuple format. " \
                    "All parameters must be in format: (name, value)."
            self._logger.error(error)
            raise ValueError(error)

        name = parameter[0]
        value = parameter[1]
        self._logger.debug("Update parameter '%s'='%s'" % (name, value))

        # Overwrite current attribute value.
        setattr(self, name, value)

    def stop(self):
        """
        Stop the mflow_processor. Called after the processing node stop command has been invoked.
        :return: None.
        """
        self._logger.debug("Stopping mflow_processor.")

    def is_running(self):
        """
        Check if the processor is still running.
        :return:
        """
        return True

