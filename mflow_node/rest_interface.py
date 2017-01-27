import json
import os
from collections import OrderedDict
from logging import getLogger

from bottle import request, run, Bottle, static_file, response

_logger = getLogger(__name__)


def start_web_interface(external_process, host, port, processor_instance=None):
    """
    Start the web interface for the supplied external process.
    :param external_process: External process to communicate with.
    :param host: Host to start the web interface on.
    :param port: Port to start the web interface on.
    :param processor_instance: Pass the mflow_processor instance to display parameters and the class documentation.
    :return: None
    """
    app = Bottle()
    static_root_path = os.path.join(os.path.dirname(__file__), "static")
    processor_name = getattr(processor_instance, "__name__", processor_instance.__class__.__name__) \
        if processor_instance else "Unknown mflow_processor"

    @app.get("/")
    def index():
        return static_file(filename="index.html", root=static_root_path)

    @app.get("/help")
    def get_help():
        return {"status": "ok",
                "data": processor_instance.__doc__ or "Sorry, no help available."}

    @app.get("/status")
    def get_status():

        # Collect default mflow_processor parameters and update them with the user set.
        if processor_instance:
            all_parameters = OrderedDict((key, value) for key, value in sorted(vars(processor_instance).items())
                                         if not key.startswith('_'))
            all_parameters.update(external_process.current_parameters)
        # Otherwise return only the parameters we know about - the one that were set.
        else:
            all_parameters = external_process.current_parameters

        return {"status": "ok",
                "data": {"processor_name": processor_name,
                         "is_running": external_process.is_running(),
                         "parameters": all_parameters}}

    @app.get("/parameters")
    def get_parameters():
        return {"status": "ok",
                "data": external_process.current_parameters}

    @app.post("/parameters")
    def set_parameter():
        for parameter in request.json.items():
            _logger.debug("Passing parameter '%s'='%s' to external process." % parameter)
            external_process.set_parameter(parameter)

        return {"status": "ok",
                "message": "Parameters set successfully."}

    @app.get("/start")
    def start():
        _logger.debug("Starting external mflow_processor.")
        external_process.start()

        return {"status": "ok",
                "message": "External process started."}

    @app.get("/stop")
    def stop():
        _logger.debug("Stopping external mflow_processor.")
        external_process.stop()

        return {"status": "ok",
                "message": "External process stopped."}

    @app.get("/static/<filename:path>")
    def get_static(filename):
        return static_file(filename=filename, root=static_root_path)

    @app.error(500)
    def error_handler_500(error):
        response.content_type = 'application/json'
        response.status = 200

        return json.dumps({"status": "error",
                           "message": str(error.exception)})

    try:
        run(app=app, host=host, port=port)
    finally:
        # Close the external processor when terminating the web server.
        if external_process.is_running():
            external_process.stop()
