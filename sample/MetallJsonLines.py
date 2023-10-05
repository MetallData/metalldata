##
## import clippy and load jsonlines operations

from clippy import clippy_import, config

# choose a suitable config:
#   direct mpi
config.cmd_prefix = 'mpirun -np 4'

#   slurm
config.cmd_prefix = 'srun -N NODES -n TASKS --mpibind=off'

#   on mammoth
config.cmd_prefix = 'srun -N 1 -n 4 -A hpcgeda -p pbatch --mpibind=off'

#   on pascal
config.cmd_prefix = 'srun -N 1 -n 4 -p pvis --mpibind=off'

# load executables
clippy_import("/PATH/TO/METALLDATA/BUILD/src/MetallJsonLines")

## open jsonlines object and import data
mjl = MetallJsonLines("/PATH/TO/DATASTORE/jframe-1_4")

mjl.read_json("/p/lustre3/llamag/reddit/comments/RC_2007-01")
# > '81341 rows imported'

## select subset of data

q = mjl[mjl.keys.score > 5]
q.count()
# > 'Selected 15710 rows.'

q = mjl[mjl.keys.score > 5, mjl.keys.controversiality == 1]
q.count()
# > 'Selected 216 rows.'

# alternative 1:
q = mjl[mjl.keys.score > 5][mjl.keys.controversiality == 1]
q.count()
# > 'Selected 216 rows.'

# alternative 2:
q = mjl[(mjl.keys.score > 5) & (mjl.keys.controversiality == 1)]
q.count()
# > 'Selected 216 rows.'

q = mjl[mjl.keys.subreddit == 'programming']
q.count()
# > 'Selected 7650 rows.'

## using contains and head

q = mjl[mjl.keys.subreddit.contains('gram')]
q.head()
# > 'Selected 7650 rows.'

# use regex and print the first 10 matching lines (default is 5)
q = mjl[mjl.keys.subreddit.contains('prog.*g', True)]
q.count()
# > 'Selected 0 rows.'

q.head(num = 10)

#
# update elements
q = mjl[mjl.keys.author == "[deleted]"]
q.count()
# > 'Selected 22387 rows.'

# print first entries
q.head()

# update element
q.set("author", q.keys.author.cat("_").cat(q.keys.rowid))
# > 'updated column author in 22387 entries\n'

# next returns empty because author_id == "[deleted]" does no longer exist
q.head()

# so try
q = m[m.keys.author.contains("[deleted]")]
q.count()
# > 'Selected 22387 rows.'

q.head()




##
## NOT WORKING

# CANNOT USE LOGICAL OPERATORS
q = mjl[mjl.keys.score > 5 and mjl.rows.controversiality == 1]
q.count() ### the left-hand-side condition gets dropped on the Python side

# Reason: "and" is the logical short circuit operator and cannot
#         be overloaded.

# CANNOT USE in OPERATION
q = mjl['prog' in vec.keys.subreddit]

# Reason: in calls __contains__. While __contains__ may return a
#         lazy object, the conversion to in forces its evaluation
#         => instead of the Expression in json format, just 'true' is serialized.


##
## Example demonstrating join of two MetallJsonLines objects (merge)

# A) Initialize the clippy environment

from clippy import clippy_import, config

# on mammoth: config.cmd_prefix = 'srun -N NODES -n TASKS -A hpcgeda -p pbatch'
config.cmd_prefix = 'srun -N 1 -n 4 -A hpcgeda -p pbatch'

clippy_import("/PATH/TO/METALLDATA/BUILD/src/MetallJsonLines")

# B) open three MetallJsonLines
places = MetallJsonLines("/PATH/TO/DATASTORE/places-1_4")
places.read_json("/PATH/TO/METALLDATA/sample/data/places.json")

names = MetallJsonLines("/PATH/TO/DATASTORE/names-1_4")
names.read_json("/PATH/TO/METALLDATA/sample/data/names.json")

# the output frame
result = MetallJsonLines("/PATH/TO/DATASTORE/combined-1_4")

# C) execute merge computes an inner joins
#    @arg-1: result, the output jsonframe. all existing data will be deleted
#    @arg-2: places, the left hand side data
#    @arg-3: names, the right hand side data
#    kwargs
#    on, left_on, right_on: define the join columns (i.e., fields in the json object);
#                           left_on, right_on supersede any list defined by on
#    left_columns, right_columns: projection list of columns that will be copied to output. If
#                                 undefined, every field is copied.
#    @pre the number of join columns in arg-4 and arg-5 must be the same.
#    @post the result contains all joined records, where columns have an _l and _r
#          suffix depending whether they stemmed from arg-2 (left) or arg-3 (right) input.
#    @details
#          merge computes an inner join of arg-2 with arg-3 on the columns specified
#          by on, left_on, right_on respectively. The output consists of the combined fields as
#          defined by left_columns, right_columns.
merge(result, places, names, on="id")

# joins on subselections can be computed by using selectors
merge(result, places, names[names.keys.name > "Pat"], on="id")


# possible future extensions
- select the columns from each side that should be selected
- add optional parameters to specify suffixes
- use computed join columns (e.g., to allow uppercase joins)
- support left-, right-, outer-, and full outer-joins

# end join example

##
## POSSIBLE FUTURE EXTENSIONS

# compute new entry based on existing entries

###






###
###
###

from clippy import clippy_import, config
clippy_import("/g/g92/peterp/git/md/build2/src/MetallJsonLines")
config.cmd_prefix = "srun"


time mjl = MetallJsonLines("/dev/shm/peterp/data-X")
#256> Wall time: 32.4 s
#16> Wall time: 4.5 s

time mjl.read_json(["/g/g92/peterp/workspace/data/data1.json", "/g/g92/peterp/workspace/data/data2.json", "/g/g92/peterp/workspace/data/data3.json", "/g/g92/peterp/workspace/data/data4.json"])
#256> Wall time: 39.9 s
#16> Wall time: 8.41 s

time result = MetallJsonLines("/dev/shm/peterp/result-X")
#256> Wall time: 40.1 s
#16> Wall time: 4.14 s

time merge(result, mjl, mjl, left_on=["p"], right_on=["s"], left_columns=["p", "d"], right_columns=["s", "d"])
#256> Wall time: 1min 19s
#16> Wall time: 14.5s
#>Out[7]: 8704888









