#!/usr/bin/env python
from setuptools import setup

setup(
    name='mflow_nodes',
    version="0.0.1",
    description="mflow stream nodes and their processors",
    author='Paul Scherrer Institute',
    author_email='andrej.babic@psi.ch',
    requires=["bottle", "mflow", "h5py", "numpy", "bitshuffle", 'requests'],

    scripts=['scripts/write_node.py',
             'scripts/proxy_node.py',
             "scripts/compression_node.py",
             "scripts/compression_node.py"],

    packages=['mflow_node',
              'mflow_processor',
              'mflow_rest_api']
)
