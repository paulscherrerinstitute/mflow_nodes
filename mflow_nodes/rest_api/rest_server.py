import json
import os
from collections import OrderedDict
from logging import getLogger, Logger, getLevelName

import bottle
import time
from bottle import request, run, Bottle, static_file, response

from mflow_nodes import config

_logger = getLogger(__name__)


def start_web_interface(process, instance_name, host, port):
    """
    Start the web interface for the supplied external process.
    :param process: External process to communicate with.
    :param instance_name: Name if this processor instance. Used to set url paths.
    :param host: Host to start the web interface on.
    :param port: Port to start the web interface on.
    :return: None
    """
    app = Bottle()
    static_root_path = os.path.join(os.path.dirname(__file__), "static")
    _logger.debug("Static files root folder: %s", static_root_path)

    # Set the path for the templates.
    bottle.TEMPLATE_PATH = [static_root_path]

    # Set the URL paths based on the format and instance name.
    api_path = config.API_PATH_FORMAT.format(instance_name=instance_name)
    html_path = config.HTML_PATH_FORMAT.format(instance_name=instance_name)

    @app.get("/")
    def redirect_to_index():
        return bottle.redirect(html_path.format(url=""))

    @app.get(html_path.format(url=""))
    @bottle.view("index")
    def index():
        return {"instance_name": instance_name}

    @app.get(api_path.format(url="help"))
    def get_help():
        return {"status": "ok",
                "data": process.get_process_help()}

    @app.get(api_path.format(url="status"))
    def get_status():
        return {"status": "ok",
                "data": {"processor_name": process.get_process_name(),
                         "is_running": process.is_running(),
                         "parameters": get_parameters()["data"]}}

    @app.get(api_path.format(url="statistics"))
    def get_statistics():
        return {"status": "ok",
                "data": {"statistics": process.get_statistics()}}

    @app.get(api_path.format(url="statistics_raw"))
    def get_statistics_raw():
        return {"status": "ok",
                "data": {"statistics_raw": process.get_statistics_raw()}}

    @app.get(api_path.format(url="parameters"))
    def get_parameters():
        return {"status": "ok",
                "data": process.get_parameters()}

    @app.post(api_path.format(url="reset"))
    def reset():
        process.reset()

        return {"status": "ok",
                "data": process.get_parameters()}

    def _set_parameters(parameters):
        _logger.debug("Passing parameters %s to external process." % parameters)
        process.set_parameters(parameters)

    @app.post(api_path.format(url="parameters"))
    def set_parameter():
        _set_parameters(request.json)
        return {"status": "ok",
                "message": "Parameters set successfully."}

    @app.get(api_path.format(url="logging"))
    def get_log_level():
        loggers = Logger.manager.loggerDict

        data = {}
        for logger_name, logger in loggers.items():
            if isinstance(logger, Logger):
                data[logger_name] = getLevelName(logger.level)

        return {"status:": "ok",
                "message": "List of current loggers.",
                "data": data}

    @app.post(api_path.format(url="logging"))
    def set_log_level():
        for name, value in request.json.items():
            getLogger(name).setLevel(value)

        return {"status": "ok",
                "message": "Parameters set."}

    @app.put(api_path.format(url=""))
    @app.get(api_path.format(url="start"))
    def start():
        if request.json:
            _set_parameters(request.json)

        _logger.debug("Starting process.")
        process.start()

        return {"status": "ok",
                "message": "Process started."}

    @app.delete(api_path.format(url=""))
    @app.get(api_path.format(url="stop"))
    def stop():
        _logger.debug("Stopping process.")
        process.stop()

        return {"status": "ok",
                "message": "Process stopped."}

    @app.delete(api_path.format(url="kill"))
    def kill():
        # Clean up as much as possible.
        stop()
        # Bottle does not make it easy to kill it.
        os._exit(0)

    @app.get(html_path.format(url="static/<filename:path>"))
    def get_static(filename):
        return static_file(filename=filename, root=static_root_path)

    @app.error(500)
    def error_handler_500(error):
        response.content_type = 'application/json'
        response.status = 500

        return json.dumps({"status": "error",
                           "message": str(error.exception)})

    try:
        host = host.replace("http://", "").replace("https://", "")
        run(app=app, host=host, port=port)
    finally:
        # Close the external processor when terminating the web server.

        # Wait for the external process poll timeout.
        time.sleep(config.DEFAULT_IPC_POLL_TIMEOUT * 2)

        process.stop()


class RestInterfacedProcess(object):
    """
    Base class for all classes that interact with the Bottle instance.
    """

    def get_process_name(self):
        """
        Return the process name.
        :return: String representation of the name.
        """
        return getattr(self, "__name__", self.__class__.__name__)

    def get_process_help(self):
        """
        Return the processor documentation.
        :return:
        """
        return self.__doc__ or "Sorry, no help available."

    def start(self):
        """
        Start the processor.
        """
        pass

    def stop(self):
        """
        Stop the processor.
        :return:
        """
        pass

    def is_running(self):
        """
        Check if the process is running.
        :return: True if the process is running, False otherwise.
        """
        pass

    def get_parameters(self):
        """
        Get process parameters.
        :return: Dictionary with all the parameters.
        """
        return OrderedDict((key, value) for key, value
                           in sorted(vars(self).items())
                           if not key.startswith('_'))

    def set_parameters(self, parameter):
        """
        Set the parameters on the process.
        :param parameter: Parameter to set, in (parameter_name, parameter_value) form.
        """
        pass

    def get_statistics(self):
        """
        Get the processor statistics.
        :return: Dictionary of relevant statistics.
        """
        return self.get_statistics_raw()

    def get_statistics_raw(self):
        """
        Get the processor raw statistics data.
        :return: List of statistics events.
        """
        pass

    def reset(self):
        """
        Reset the status of the integration.
        """
