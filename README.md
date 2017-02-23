# MFLOW nodes
An mflow node is a standalone processes that receive, process, and maybe forward an mflow stream.
Every node has a Rest API and a simple web interface to control it. The node loads a processor (a class or function), 
which is used to manipulate, analyze or save the mflow stream.

## Conda production setup
If you use conda in production, you can create a production ready environment 
by running:

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
- bitshuffle
- bottle
- h5py
- numpy
- requests

In case you are using conda to install the package, you might need to add the **paulscherrerinstitute** channel to 
your conda config:

```
conda config --add channels paulscherrerinstitute
```

## Using existing nodes
There are already existing processors and the scripts to run them in this library. All the 
running scripts should be automatically added to your path, so you should be able to run 
them from anywhere.

### Running nodes
The scripts for running the existing nodes are located under the **scripts/** folder. To start a node, execute the 
script with the required parameters (which parameters you need to provide depends on the node type).

The currently available nodes are:

- **Proxy node** (proxy_node.py): Outputs the stream to the console and forwards it to the next node.
- **Compression node** (compression_node.py): Compresses the stream using the bitshuffle LZ4 algorithm.
- **Writer node** (writer_node.py): Writes the stream to a H5 file.
- **NXMX node** (nxmx_node.py): Creates the master H5 file in the NXMX standard.

The documentation for each node should be located at the end of this document (chapter **Nodes documentation**), but 
some help if also available if you run the scripts with the '-h' parameter.

### Controlling running nodes
Once a node has been started it can be monitored and its configuration changed via the web interface or the REST 
api. Every node supports some basic operations:

- **Start**: Starts the node.
- **Stop**: Stops the node.
- **Get parameters**: Return the currently set parameters.
- **Update parameters**: Update the parameters in the node.

What exactly each command means to a specific node depends on which processor is loaded in the node. For more details 
on the existing nodes, see the **Nodes documentation** chapter.

### Web interface
Each node starts a web interface on **0.0.0.0** using the port provided at the node startup (default port is **8080**).
Navigate your browser to **0.0.0.0:8080** (or corresponding port) to view the interface.

### REST api
The web server running the web interface exposes the REST api as well. All the functionality available via the web 
interface is also available via the REST api. The following endpoints are exposed:

- **/start** [GET]: Start the processor.
- **/stop** [GET]: Stop the processor.
- **/parameters** [GET]: Get the current processor parameters.
- **/parameters** [POST]: Set the processor parameters. You need to specify only the parameters you want to modify, and 
can omit the already set parameters.
- **/status** [GET]: Get the processor status.
- **/help** [GET]: Get the processor documentation.

All endpoints respond with JSONs objects. The endpoints that accept parameters do so in JSON format as well.

For example, (supposing your control port is 8080), you can execute the following commands in your terminal:
```bash
# Get the processor status.
curl 0.0.0.0:8080/status;

# Stop the processor
curl 0.0.0.0:8080/status;

# Set processor parameters (in this case, we update or set only the value of one parameter):
curl -H "Content-Type: application/json" -X POST -d '{"parameter_name":"parameter_value"}' 0.0.0.0:8080/parameters;
```

Each command will return a JSON object with a status and message. You can also see the results of your actions via 
the web interface (do not forget to refresh the page after you have made changes).

#### Response format
The response format is always JSON. The JSON has one mandatory field, **status**, and 2 optional fields, **message** 
and **data**.

- **"status"** [mandatory] is always present and its value can be either **"ok"** or **"error"**.
- **"message"** [optional] contains the description of what happened in human readable format. Note that this field 
is not always present.
- **"data"** [optional] contains any data that the endpoint returned. Note that this field is not always present either.

**Example response** (from **/status** request on **write\_node.py**):
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


## Run a sample node chain
In order to better illustrate how nodes are supposed to be used, we are going to generate a test mflow stream, output 
its content to the console, compress the stream, output the compressed stream to the console, and finally write it into 
a H5 file.

Run the following commands, each in a separate terminal (from the **root** project folder) to execute the example:
```bash
# Start a proxy node:
#   - listen on localhost port 40000
#   - forward the stream to localhost port 40001
#   - start the web interface on port 8080
python scripts/proxy_node.py tcp://127.0.0.1:40000 tcp://127.0.0.1:40001 --rest_port 8080
```

```bash
# Start a compression node:
#   - listen on localhost port 40001
#   - forward the stream to localhost port 40002
#   - start the web interface on port 8081
python scripts/compression_node.py tcp://127.0.0.1:40001 tcp://127.0.0.1:40002 --rest_port 8081
```

```bash
# Start a proxy node:
#   - listen on localhost port 40002
#   - forward the stream to localhost port 40003
#   - start the web interface on port 8082
python scripts/proxy_node.py tcp://127.0.0.1:40002 tcp://127.0.0.1:40003 --rest_port 8082
```
```bash
# Start a writer node:
#   - listen on localhost port 40003
#   - save the stream to file sample_output.h5
#   - start the web interface on port 8083
python scripts/write_node.py tcp://127.0.0.1:40003 sample_output.h5 --rest_port 8083
```

```bash
# Generate the test stream:
python test/generate_test_stream.py
```

Inspect the output of each terminal. The default logging level is DEBUG - what is happening in the nodes should 
be self explanatory.

When you no longer need the nodes terminate them by pressing **CTRL+C** in each terminal.

## Nodes documentation

### Proxy node

### Compression node

### Write node

### Developing new nodes
...
#### Processor
...
#### Running scripts
...
#### Processor documentation
...