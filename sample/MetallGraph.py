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
#    The MetallGraph consists of two MetallJsonLines objects nodes and edges
#    An entry in nodes MUST have a field identified by key (i.e., "id")
#    An entry in edges MUST have two fields identified by srckey and dstkey (i.e., "id1" and "id2")
#  Note:
#    The sample data contains persons and departments in xnodes.json and relations between
#    persons and departments in xedges.json. Persons and departments are uniquely identified by a field "id",
#    whereas valid edges have srckey "id1" (person) and dstkey "id2" (department).
mg = MetallGraph("/PATH/TO/DATASTORE/mg-1_4", key="id", srckey="id1", dstkey="id2", overwrite=True)

## read node and edge file lists separately
#    note 1, rejected refers to the number of rejected lines (lines not meeting the key criteria)
#    note 2, xnodes.json contains persons (have name field) and departments (have dept field).
mg.read_nodes("/PATH/TO/METALLDATA/sample/data/xnodes.json")
# > {'imported': 14, 'rejected': 1}

mg.read_edges("/PATH/TO/METALLDATA/md/sample/data/xedges.json")
# > {'imported': 10, 'rejected': 2}

## check how many nodes and edges meet selection criteria independently
mg.count_lines()
# > {'nodes': 14, 'edges': 10}

## check how many nodes and CONNECTED edges meet selection criteria
#    to be counted, an edge MUST have both endpoints (nodes) meet selection criteria
mg.count()
# > {'nodes': 14, 'edges': 10}

## select subset of data
#    note: if a json entry does not have a property, it does not match.
#          e.g., departments have a .dept field but not .name field.
#    For example if we want to count the number of departments with
#    heads whose name is greater or equal than Pam, we need to make sure
#    that all departments are included.
#    ( ==> use mg.nodes.dept for including all departments, otherwise none of the edges will survive. )
q = mg[(mg.nodes.name >= "Pam") | (mg.nodes.dept)][mg.edges.rel == "head"]
q.count()
{'nodes': 10, 'edges': 3}


#
# auto generation of vertices from edge files
# i

#  Consider the sample file (softcom.json). Each consists of a person (unique name)
a# associated with a department. An entry can also be considered an edge with attributes.

# To create a graph from the dataset, we require each vertex to have a from and a to vertex.
# The vertex entries are not part of a dataset, but will be auto-generated. In this case we choose
# the vertices in the edge set to be named (member, group) and the entry in the vertex set to be ID.
mg = new MetallGraph("/PATH/TO/DATASTORE/soft-1_4", key = "ID", srckey = "member", dstkey = "group");


# The dataset originally consists only of a set of edges where none of the entries contains any of these keys. The keys will be auto-generated upon import.

mg.read_edges("/PATH/TO/METALLDATA/sample/data/softcom.json", autoVertices=["name", "dept"])

# This extends each record with two fields: member = "1@" + name, and group = "2@" + dept. Any entry without name or dept field will be discarded.
# At the same time, a vertex file gets generated. An entry in the vertex file only contains IDs consisting of the union of member and group fields.

