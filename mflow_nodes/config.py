# Configuration for the m_manage.py, where to look for config files.
MANAGE_MACHINE_FILENAME = "/etc/mflow_nodes.json"
MANAGE_USER_FILENAME = "~/.mflow_nodes_rc.json"
MANAGE_PWD_FILENAME = "mflow_nodes.json"

LOG_MACHINE_FILENAME = "/etc/mflow_nodes_logging.json"
LOG_USER_FILENAME = "~/.mflow_nodes_rc_logging.json"
LOG_PWD_FILENAME = "mflow_nodes_logging.json"

# Stream node defaults.
DEFAULT_CONNECT_ADDRESS = "tcp://127.0.0.1:40000"
DEFAULT_REST_HOST = "http://0.0.0.0"
DEFAULT_REST_PORT = 41000
DEFAULT_DATA_QUEUE_LENGTH = 16
DEFAULT_N_RECEIVING_THREADS = 1
DEFAULT_STATISTICS_BUFFER_LENGTH = 100
DEFAULT_STARTUP_TIMEOUT = 5
# Default logging level
DEFAULT_LOGGING_LEVEL = "DEBUG"
# How much time we allow for the processor to shut down, before we terminate it.
DEFAULT_SHUTDOWN_TIMEOUT = 5
# Time it takes for the multiprocess exchange to happen.
DEFAULT_IPC_TIMEOUT = 2
# Time to use for polling the ipc communication.
DEFAULT_IPC_POLL_TIMEOUT = 0.5
# Number of ZMQ io threads
ZMQ_IO_THREADS = 1

# Node thread defaults.
DEFAULT_RECEIVE_TIMEOUT = 1000
DEFAULT_QUEUE_READ_INTERVAL = 0
DEFAULT_ZMQ_QUEUE_LENGTH = 32

# REST Interface defaults.
API_PATH_FORMAT = "/api/v1/{instance_name}/{{url}}"
HTML_PATH_FORMAT = "/{instance_name}/{{url}}"

# Client defaults.
DEFAULT_CLIENT_INSTANCE = '{variable_name} = NodeClient(address="{address}", instance_name="{instance_name}")'

# Process parameters.
PARAMETER_PROCESS_UID = "process_uid"
PARAMETER_PROCESS_GID = "process_gid"
PARAMETER_N_MESSAGES = "n_messages"
PROCESS_PARAMETERS = [PARAMETER_PROCESS_UID, PARAMETER_PROCESS_GID, PARAMETER_N_MESSAGES]

