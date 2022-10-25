##
## import clippy and load jsonlines operations

from clippy import clippy_import, config

# on mammoth: config.cmd_prefix = 'srun -N NODES -n TASKS -A hpcgeda -p pbatch'
config.cmd_prefix = 'srun -N 1 -n 4 -A hpcgeda -p pbatch --mpibind=off'
config.cmd_prefix = 'mpirun -np 4'

# on pascal
config.cmd_prefix = 'srun -N 1 -n 4 -p pvis --mpibind=off'

clippy_import("/PATH/TO/metallData-22/build/src/MetallFrame")
clippy_import("/g/g92/peterp/git/metallData-22/build-rel/src/MetallFrame")


## open jsonlines object and import data
mf = MetallFrame("./jframe-1_4")

mf.read_json("/p/lustre3/llamag/reddit/comments/RC_2007-01")
# > '81341 rows imported'

## select subset of data

q = mf[mf.keys.score > 5]
q.count()
# > 'Selected 15710 rows.'

q = mf[mf.keys.score > 5, mf.keys.controversiality == 1]
q.count()
# > 'Selected 216 rows.'

# alternative 1:
q = mf[mf.keys.score > 5][mf.keys.controversiality == 1]
q.count()
# > 'Selected 216 rows.'

# alternative 2:
q = mf[(mf.keys.score > 5) & (mf.keys.controversiality == 1)]
q.count()
# > 'Selected 216 rows.'

q = mf[mf.keys.subreddit == 'programming']
q.count()
# > 'Selected 7650 rows.'

## using contains and head

q = mf[mf.keys.subreddit.contains('gram')]
q.head()
# > 'Selected 7650 rows.'

# use regex and print the first 10 matching lines (default is 5)
q = mf[mf.keys.subreddit.contains('prog.*g', True)]
q.count()
# > 'Selected 0 rows.'

q.head(num = 10)

#
# update elements
q = mf[mf.keys.author == "[deleted]"]
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
q = mf[mf.keys.score > 5 and mf.rows.controversiality == 1]
q.count() -- the left-hand-side condition gets dropped on the Python side

# Reason: "and" is the logical short circuit operator and cannot
#         be overloaded.

# CANNOT USE in OPERATION
q = mf['prog' in vec.keys.subreddit]

# Reason: in calls __contains__. While __contains__ may return a
#         lazy object, the conversion to in forces its evaluation
#         => instead of the Expression in json format, just 'true' is serialized.


##
## Example demonstrating join of two MetallFrame objects (merge)

# A) Initialize the clippy environment

from clippy import clippy_import, config

# on mammoth: config.cmd_prefix = 'srun -N NODES -n TASKS -A hpcgeda -p pbatch'
config.cmd_prefix = 'srun -N 1 -n 4 -A hpcgeda -p pbatch'

clippy_import("/PATH/TO/metallData-22/build/examples/MetallFrame")

# B) open three MetallFrame
places = MetallFrame("./test/places-1_4")
places.read_json("/PATH/TO/metallData-22/sample/data/places.json")

names = MetallFrame("./test./names-1_4")
names.read_json("/PATH/TO/metallData-22/sample/data/names.json")

# the output frame
result = MetallFrame("./test/combined-1_4")

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
#    @note the selection filter is currently ignored, and the join is executed on the full table.
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

