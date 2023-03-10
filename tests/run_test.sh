#!/usr/bin/env bash

#
# set up defaults

DEFAULT_NP=4
DEFAULT_DATASTORE="/tmp"
DEFAULT_REPODIR=$(pwd)
DEFAULT_BUILDDIR="../build"

NP=$DEFAULT_NP
DATASTORE=$DEFAULT_DATASTORE
BUILDDIR=$DEFAULT_BUILDDIR
REPODIR=$DEFAULT_REPODIR

#
# argument processing

VALID_ARGS=$(getopt -o hb:d:p:r: --long help,builddir:,datastore:,np:,repo: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -h | --help)
        echo "runs basic tests"
        echo "  -h --help this help message"
        echo "  -b --builddir the build directory (default: $DEFAULT_BUILDDIR)"
        echo "  -d --datastore the root directory for metall data data stores (default: $DEFAULT_DATASTORE)"
        echo "  -p --np number of processes (default: $DEFAULT_NP)"
        echo "  -r --repo root directory of MetallData repo (default: $DEFAULT_REPODIR)"
        shift
        ;;
    -b | --builddir)
        BUILDDIR=$2
        shift 2
        ;;
    -d | --datastore)
        DATASTORE=$2
        shift 2
        ;;
    -p | --np)
        NP=$2
        shift 2
        ;;
    -r | --repo)
        REPODIR=@2
        shift 2
        ;;
    --) shift;
        break
        ;;
  esac
done

#
# set up input json files and replace path placeholders

cp inputs/*.json .

sed -i "s|/PATH/TO/DATASTORE|$DATASTORE|g" *.json
sed -i "s|/PATH/TO/METALLDATA|$REPODIR/..|g" *.json

#
# run the tests
#
# note, output of mjl-head and mjl-info may not be stable

#~ set -o xtrace

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-names.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-names.json >mjl-init-names.out
cmp --silent mjl-init-names.out expected/mjl-init-names.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-read_json <mjl-read_json-names.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-read_json <mjl-read_json-names.json >mjl-read_json-names.out
cmp --silent mjl-read_json-names.out expected/mjl-read_json-names.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-places.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-places.json >mjl-init-places.out
cmp --silent mjl-init-places.out expected/mjl-init-places.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-read_json <mjl-read_json-places.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-read_json <mjl-read_json-places.json >mjl-read_json-places.out
cmp --silent mjl-read_json-places.out expected/mjl-read_json-places.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-results.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-init <mjl-init-results.json >mjl-init-results.out
cmp --silent mjl-init-results.out expected/mjl-init-results.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-count <mjl-count-names.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-count <mjl-count-names.json >mjl-count-names.out
cmp --silent mjl-count-names.out expected/mjl-count-names.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-head <mjl-head-names.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-head <mjl-head-names.json >mjl-head-names.out
#~ cmp --silent mjl-head-names.out expected/mjl-head-names.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-head <mjl-head-selected_places.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-head <mjl-head-selected_places.json >mjl-head-selected_places.out
#~ cmp --silent mjl-head-selected_places.out expected/mjl-head-selected_places.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-info <mjl-info-selected_places.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-info <mjl-info-selected_places.json >mjl-info-selected_places.out
#~ cmp --silent mjl-info-selected_places.out expected/mjl-info-selected_places.out || echo '** FAILED'

echo "mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-merge <mjl-merge.json"
mpirun -np $NP $BUILDDIR/src/MetallJsonLines/mjl-merge <mjl-merge.json >mjl-merge.out
cmp --silent mjl-merge.out expected/mjl-merge.out || echo '** FAILED'

echo "qed."
