from clippy import clippy_import, config

metall_dir = '/dev/shm/graph'
src_dir = '/g/g90/iwabuchi/metalldata'
build_dir = '/g/g90/iwabuchi/metalldata/build'
data_dir = src_dir + '/sample/data/'

clippy_import(build_dir + '/src/MetallGraph')

config.cmd_prefix = f'srun -N 1 --ntasks-per-node 32 --mpibind=off'

graph = MetallGraph(metall_dir, key="id", srckey="src", dstkey="dst", overwrite=True)

graph.read_vertices(data_dir + "/vertices.parquet", fileType='parquet')

graph.read_edges(data_dir + "/edges.parquet", fileType='parquet')

graph.count()

graph.connected_components()

graph.kcore(3)

graph[graph.nodes.kcore >= 3].connected_components()