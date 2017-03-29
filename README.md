# mflow nodes
An mflow node is network node operating on an mflow stream.

Every node has a Rest API and a simple web interface to control it. 
The purpose of the node is to loads a processor (a class or function), 
which is used to manipulate, analyze or save the mflow stream.

## Conda setup
If you use conda, you can create an environment with the mflow_nodes library by running:

```bash
conda create -c paulscherrerinstitute --name <env_name> mflow_nodes
```

After that you can just source you newly created environment and start using the library.

## Local build
You can build the library by running the setup script in the root folder of the project:

```bash
python setup.py install
```

or by using the conda also from the root folder of the project:

```bash
conda build conda-recipe
conda install --use-local mflow_nodes
```

### Requirements
The library relies on the following packages:

- mflow
- bottle
- requests

In case you are using conda to install the packages, you might need to add the **paulscherrerinstitute** channel to 
your conda config:

```
conda config --add channels paulscherrerinstitute
```

If you do not want to install the packages manually you can use the **conda-recipe/conda_env.txt** file to create 
the conda environment with the packages used for developemnt:

```bash
conda create --name <env> --file conda-recipe/conda_env.txt
```

## Web interface
Each node starts a web interface on **0.0.0.0** using the port provided at the node startup (default port is **8080**).
Navigate your browser to **0.0.0.0:8080** (or corresponding port) to view the interface.

## REST api
The web server running the web interface exposes the REST api as well. All the functionality available via the web 
interface is also available via the REST api. The following endpoints are exposed:

- **api/[api_version]/[instance_name]/** [PUT]: Start the processor.
- **api/[api_version]/[instance_name]/** [DELETE]: Stop the processor.
- **api/[api_version]/[instance_name]/parameters** [GET]: Get the current processor parameters.
- **api/[api_version]/[instance_name]/parameters** [POST]: Set the processor parameters. You need to specify only the parameters you want to modify, and
can omit the already set parameters.
- **api/[api_version]/[instance_name]/status** [GET]: Get the processor status.
- **api/[api_version]/[instance_name]/help** [GET]: Get the processor documentation.
- **api/[api_version]/[instance_name]/statistics** [GET]: Get the processor statistics.
- **api/[api_version]/[instance_name]/statistics_raw** [GET]: Get the processor statistics events.

All endpoints respond with JSONs objects. The endpoints that accept parameters do so in JSON format as well.

There are 2 variables in the URL schema:

- **api\_version**: Current version of the API.
- **instance\_name**: Name of the processor instance. Each processor should have a unique instance in order to 
avoid accidental interference from other processes.

For example, (supposing your control port is 8080, and your instance name is "stats", and obviously the node is running), 
you can execute the following commands in your terminal:

```bash
# Start the stream statistics node.
#   - Name the instance stats
#   - Connect to tcp://127.0.0.1:40000
#   - Expose REST api on port 8080.
m_stats_node.py stats tcp://127.0.0.1:40000 --rest_port 8080
```

In a separate terminal, try:

```bash
# Get the processor status.
curl 0.0.0.0:8080/api/v1/stats/status;

# Start the processor
curl -X PUT 0.0.0.0:8080/api/v1/stats/;

# Set processor parameters (in this case, we update or set only the value of one parameter):
curl -H "Content-Type: application/json" -X POST -d '{"parameter_name":"parameter_value"}' 0.0.0.0:8080/api/v1/base/parameters;

# Get processor parameters.
curl 0.0.0.0:8080/api/v1/stats/parameters;

# Stop the processor
curl -X DELETE 0.0.0.0:8080/api/v1/stats/;
```

Each command will return a JSON object with a status and message. You can also see the results of your actions via 
the web interface (do not forget to refresh the page after you have made changes).

### REST Client
The REST api is also exposed via a client class. You can load it, for example in ipython:

```python
from mflow_nodes import NodeClient
client = NodeClient(address="http://127.0.0.1:8080", instance_name="stats")
# Get the status of the node.
client.get_status()
# Start the node.
client.start_node()
# ..and so on for all the available commands..
```

### Response format
The response format is always JSON. The JSON has one mandatory field, **status**, and 2 optional fields, **message** 
and **data**.

- **"status"** [mandatory] is always present and its value can be either **"ok"** or **"error"**.
- **"message"** [optional] contains the description of what happened in human readable format. Note that this field 
is not always present.
- **"data"** [optional] contains any data that the endpoint returned. Note that this field is not always present either.

**Example response** (from **/status** request, on a processor implementation):
```json
{
  "status": "ok", 
  "data": 
    {
      "processor_name": "H5 chunked writer", 
      "parameters": 
        {
          "compression": 32008, 
          "compression_opts": [2048, 2], 
          "dataset_frames_increase_step": 1000, 
          "dataset_initial_frame_count": 1, 
          "dataset_name": "data", 
          "dtype": "int32", 
          "frame_size": [4, 4], 
          "output_file": "temp_output.h5"
        }
    },
    "is_running": true
}
```

## Testing tools
There are 2 executable scripts to test your setup and debug any potential issues on the network:

- **m\_stats\_node.py** (mflow node processor that measures your network speed when using mflow nodes)
- **m\_generate\_test\_stream.py** (generates a test stream to debug your nodes or network)

All executable scripts are added to you PATH when the library is installed.