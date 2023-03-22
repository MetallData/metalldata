##
## import clippy and load jsonlines operations

from clippy import clippy_import, config

# choose a suitable config:
#   direct mpi
config.cmd_prefix = 'mpirun -np 4'

#   slurm
#config.cmd_prefix = 'srun -N NODES -n TASKS --mpibind=off'

#   on mammoth
#config.cmd_prefix = 'srun -N 1 -n 4 -A hpcgeda -p pbatch --mpibind=off'

#   on pascal
#config.cmd_prefix = 'srun -N 1 -n 4 -p pvis --mpibind=off'

# load executables
clippy_import("/PATH/TO/METALLDATA/BUILD/src/MetallGraph")

## create MetallGraph object
mg = MetallGraph("/PATH/TO/DATASTORE/mg-1_4", overwrite=True)

# read node and edge file lists
mg.read_json(node_files="/PATH/TO/METALLDATA/sample/data/xnodes.json", edge_files="/PATH/TO/METALLDATA/md/sample/data/xedges.json")
# > {'nodes': 14, 'edges': 10}

## select subset of data
##   note: if a json entry does not have a property, it does not match.
##         e.g., departments have a .dept field but not .name field.
q = mg[mg.nodes.name >= "Pam"][mg.edges.id2 == 13]
q.count()
{'nodes': 6, 'edges': 4}

