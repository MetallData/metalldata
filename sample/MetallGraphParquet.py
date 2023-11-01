# This is a sample script to show how to use MetallGraph with parquet files
# The sample parquet files are compressed with snappy

import os
from clippy import clippy_import, config

# Change the following lines to your environment
metall_dir = './metalldata-parquet-graph'
this_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(this_dir + '../')
build_dir = src_dir + '/build/'
data_dir = src_dir + '/sample/data/'

clippy_import(build_dir + '/src/MetallGraph')

config.cmd_prefix = f'mpirun -np 2'
# Or
# config.cmd_prefix = f'srun -N 1 --ntasks-per-node 2 --mpibind=off'

graph = MetallGraph(metall_dir, key="id", srckey="src", dstkey="dst", overwrite=True)

graph.read_vertices(data_dir + "/vertices.parquet", fileType='parquet')

graph.read_edges(data_dir + "/edges.parquet", fileType='parquet')

graph.count()

graph.connected_components()

graph.kcore(3)

graph[graph.nodes.kcore >= 3].connected_components()