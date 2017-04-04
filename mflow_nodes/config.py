# Configuration for the m_manage.py, where to look for config files.
MACHINE_FILENAME = "/etc/mflow_nodes.json"
USER_FILENAME = "~/.mflow_nodes_rc.json"
PWD_FILENAME = "mflow_nodes.json"

# Stream node defaults.
DEFAULT_CONNECT_ADDRESS = "tcp://127.0.0.1:40000"
DEFAULT_REST_HOST = "http://0.0.0.0"
DEFAULT_REST_PORT = 41000
DEFAULT_DATA_QUEUE_LENGTH = 16
DEFAULT_STATISTICS_BUFFER_LENGTH = 100
DEFAULT_STARTUP_TIMEOUT = 5

# Node thread defaults.
DEFAULT_RECEIVE_TIMEOUT = 1000
DEFAULT_QUEUE_READ_TIMEOUT = 1000
DEFAULT_ZMQ_QUEUE_LENGTH = 32

# REST Interface defaults.
API_PATH_FORMAT = "/api/v1/{instance_name}/{{url}}"
HTML_PATH_FORMAT = "/{instance_name}/{{url}}"
