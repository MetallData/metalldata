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
config.cmd_prefix = 'srun -N 4 -n 16 -A hpcgeda -p pbatch --mpibind=off -t 4:0:0 --nodelist=mammoth9,mammoth10,mammoth11,mammoth12'


config.cmd_prefix = 'srun'

#   on pascal
config.cmd_prefix = 'srun -N 1 -n 4 -p pvis --mpibind=off'

# load executables
clippy_import("/PATH/TO/METALLDATA/BUILD/src/MetallJsonLines")

clippy_import("/PATH/TO/METALLDATA/BUILD/src/MetallJsonLines")
clippy_import("/g/g92/peterp/git/md/build/src/MetallJsonLines/")

## open jsonlines object and import data
mjl = MetallJsonLines("/PATH/TO/DATASTORE/jframe-1_4")

mjl.read_json("/p/lustre3/llamag/reddit/comments/RC_2007-01")
# > '81341 rows imported'  ???

time mjl.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])
# > Wall time: 17.6 s
# > Wall time: 24.7 s
# > 16694012

-----

time mjl.read_json(["/home/pirkelbauer2/reddit/RC_2010-10", "/home/pirkelbauer2/reddit/RC_2010-11", "/home/pirkelbauer2/reddit/RC_2010-12"])
# > 16694012

time q = mjl[mjl.keys.score > 5]
#> CPU times: user 1.06 ms, sys: 5.48 ms, total: 6.54 ms
#> Wall time: 2.18 s
#> Wall time: 10.4 s

time q.count()
#> Wall time: 9.59 s
#> Wall time: 15.3 s
#> 2015574


time q = mjl[mjl.keys.author == "[deleted]"]
#> CPU times: user 1.39 ms, sys: 5.8 ms, total: 7.19 ms
#> Wall time: 2.16 s
#> Wall time: 11.2 s

time q.count()
#> CPU times: user 1.51 ms, sys: 5.04 ms, total: 6.55 ms
#> Wall time: 6.7 s
#> Wall time: 15.4 s
#> Out[23]: 3546616

time q = mjl[mjl.keys.author.contains("[deleted]")]
#> CPU times: user 3.27 ms, sys: 4.12 ms, total: 7.39 ms
#> Wall time: 2.4 s
#> Wall time: 8.1 s

time q.count()
#> CPU times: user 1.13 ms, sys: 5.88 ms, total: 7 ms
#> Wall time: 7.1 s
#> Wall time: 12.5 s
#> Out[30]: 3546616

time q.set("author", q.keys.id.cat("_[deleted]"))
#> CPU times: user 3.12 ms, sys: 3.89 ms, total: 7 ms
#> Wall time: 8.27 s
#> Wall time: 12.1 s
#> Out[25]: 3546616

time q = mjl[mjl.keys.subreddit.contains('programming')]
#> ...

time q.count()
#> Wall time: 12.3 s
#> Out[15]: 177608

time result = MetallJsonLines("/l/ssd/peterp/result-4_16")
#> CPU times: user 408 µs, sys: 6.76 ms, total: 7.17 ms
#> Wall time: 5.65 s
#> Wall time: 9.44 s


time merge(result, mjl, mjl, left_on=["parent_id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#> Wall time: 12min 21s
#> 1867014548

----


## select subset of data

q = mjl[mjl.keys.score > 5]
q.count()
# > 'Selected 15710 rows.'

time q = mjl[mjl.keys.score > 5, mjl.keys.controversiality == 1]
time q.count()
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
time q.count()
# > Wall time: 8.69 s
# >  3546616

# print first entries
q.head()

# update element
time q.set("author", q.keys.author.cat("_").cat(q.keys.rowid))
# > Wall time: 15.6 s
# > 3546616

q = mjl[mjl.keys.author.contains("[deleted]", True)]

time q.set("author", q.keys.id.cat("_[deleted]"))

# next returns empty because author_id == "[deleted]" does no longer exist
q.head()

# so try
time q = m[m.keys.author.contains("[deleted]")]
time q.count()
# > 'Selected 22387 rows.'

q.head()

# > Wall time: 2h 1min 47s
# > 1867014548





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

-------------------------------

from clippy import clippy_import, config

config.cmd_prefix = 'srun'

clippy_import("/g/g92/peterp/git/md/build2/src/MetallJsonLines")

time mjl = MetallJsonLines("/l/ssd/peterp/reddit-4_16")

time mjl.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])
# > Wall time: 15.9 s

time result = MetallJsonLines("/l/ssd/peterp/result-4_16")
# > Wall time: 4.6 s

time merge(result, mjl, mjl, left_on=["parent_id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#> Wall time: 12min 5s
#> 1867014548


-------------------------------

from clippy import clippy_import, config

config.cmd_prefix = 'srun'

clippy_import("/g/g92/peterp/git/md/build2/src/MetallJsonLines")

time mjl = MetallJsonLines("/dev/shm/peterp/reddit-X")
# > Wall time: 4.05 s

time mjl.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])
# > Wall time: 12.7 s

time result = MetallJsonLines("/dev/shm/peterp/result-X")
# > Wall time: 4.03 s

time merge(result, mjl, mjl, left_on=["parent_id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#> Wall time: 10min 19s
#> Wall time: 10min 40s
#> 1867014548

time merge(result, mjl, mjl, left_on=["id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#> Wall time: 10.5 s
#> 0













###
###
###

from clippy import clippy_import, config
clippy_import("/g/g92/peterp/git/md/build/src/MetallJsonLines")
config.cmd_prefix = "srun"


time mjl = MetallJsonLines("/dev/shm/peterp/data-X")
#256> Wall time: 2.26 s

time mjl.read_json(["/g/g92/peterp/workspace/data/data1.json", "/g/g92/peterp/workspace/data/data2.json", "/g/g92/peterp/workspace/data/data3.json", "/g/g92/peterp/workspace/data/data4.json"])
#256> Wall time: 7.53 s

time result = MetallJsonLines("/dev/shm/peterp/result-X")
#256> Wall time: 1.89 s

time merge(result, mjl, mjl, left_on=["p"], right_on=["s"], left_columns=["p", "d"], right_columns=["s", "d"])
#256> Wall time: 1min 19s
#16> Wall time: 14.5s
#>Out[7]: 8704888







###
###
###

from clippy import clippy_import, config
clippy_import("/g/g92/peterp/git/md/build/src/MetallJsonLines")
config.cmd_prefix = "srun"


time mjl = MetallJsonLines("/dev/shm/peterp/reddit-X")
#512> Wall time: 2.27 s

time mjl.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-07", "/p/lustre3/llamag/reddit/comments/RC_2010-08", "/p/lustre3/llamag/reddit/comments/RC_2010-09", "/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])
#512> Wall time: 7.06 s
#> 29678800

reddit.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])


time result = MetallJsonLines("/dev/shm/peterp/result-X")
#512 > Wall time: 1.82 s

time merge(result, mjl, mjl, left_on=["parent_id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#512 > Wall time: 1.08 s
#> 3149471769







###
### salloc -N 8 -n 512 -A hpcgeda -p pbatch -t 1:0:0 --mpibind=off
### salloc -N 8 -n 512 -A hpcgeda -p pdebug -t 1:0:0 --mpibind=off

from clippy import clippy_import, config
clippy_import("/g/g92/peterp/git/md/build/src/MetallJsonLines")
config.cmd_prefix = "srun"


time mjl = MetallJsonLines("/dev/shm/peterp/reddit-X")
#2048> Wall time: 2.27 s


time mjl.read_json(["/p/lustre3/llamag/reddit/comments/RC_2010-01", "/p/lustre3/llamag/reddit/comments/RC_2010-02", "/p/lustre3/llamag/reddit/comments/RC_2010-03", "/p/lustre3/llamag/reddit/comments/RC_2010-04", "/p/lustre3/llamag/reddit/comments/RC_2010-05", "/p/lustre3/llamag/reddit/comments/RC_2010-06", "/p/lustre3/llamag/reddit/comments/RC_2010-07", "/p/lustre3/llamag/reddit/comments/RC_2010-08", "/p/lustre3/llamag/reddit/comments/RC_2010-09", "/p/lustre3/llamag/reddit/comments/RC_2010-10", "/p/lustre3/llamag/reddit/comments/RC_2010-11", "/p/lustre3/llamag/reddit/comments/RC_2010-12"])
#512> Wall time: 7.67 s
#> 48489057


time result = MetallJsonLines("/dev/shm/peterp/result-X")
#512 > Wall time: 1.82 s

time merge(result, mjl, mjl, left_on=["parent_id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])
#512 > Wall time: 1.41 s
#> 4894410202





time merge(result, mjl, mjl, left_on=["id"], right_on=["link_id"], left_columns=["id", "parent_id", "author"], right_columns=["id", "link_id", "author"])











