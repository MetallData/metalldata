#!/usr/bin/env bash

exec_test()
{
  # call: exec_test "$NP" "$BUILDROOT/src/MetallJsonLines/mjl-read_json" "mjl-init-names" 1
  EXEPREFIX=$1
  PROG=$2
  TEST=$3
  CHECK=$4

  CMD="$EXEPREFIX $PROG"

  echo "$CMD <$TEST.json"
  $CMD <$TEST.json >$TEST.out

  if [ $? -ne 0 ]; then
    echo -e "\e[0;31m** FAILED\e[0m (non-zero return)"
  elif [ $CHECK -eq 1 ]; then
    cmp --silent $TEST.out $TEST.exp || echo -e "\e[0;31m** FAILED\e[0m (unexpected output)"
  fi
}

#
# set up defaults

DEFAULT_EXEPREFIX="mpirun -np 4"
DEFAULT_DATASTORE="/tmp"
DEFAULT_GITREPDIR=$(pwd)
DEFAULT_BUILDROOT="../build"
DEFAULT_CREATEINP=0

EXEPREFIX=$DEFAULT_EXEPREFIX
DATASTORE=$DEFAULT_DATASTORE
BUILDROOT=$DEFAULT_BUILDROOT
GITREPDIR=$DEFAULT_GITREPDIR
CREATEINP=$DEFAULT_CREATEINP

#
# argument processing

VALID_ARGS=$(getopt -o hb:d:ip:r: --long help,builddir:,datastore:,inputs,prefix:,repo: -- "$@")
if [[ $? -ne 0 ]]; then
    exit 1;
fi

eval set -- "$VALID_ARGS"
while [ : ]; do
  case "$1" in
    -h | --help)
        echo "runs basic tests on MetallData executables"
        echo "  -h --help      prints this help message"
        echo "  -b --builddir  the build directory (default: $DEFAULT_BUILDROOT)"
        echo "  -d --datastore the root directory for metall data data stores (default: $DEFAULT_DATASTORE)"
        echo "  -p --prefix    execution prefix, such as mpirun, srun (default: $DEFAULT_EXEPREFIX)"
        echo "  -r --repo      root directory of MetallData repo (default: $DEFAULT_GITREPDIR)"
        echo "  -i --inputs    just generate the json input and expected output files, without running tests"
        exit 0
        ;;
    -b | --builddir)
        BUILDROOT=$2
        shift 2
        ;;
    -d | --datastore)
        DATASTORE=$2
        shift 2
        ;;
    -i | --inputs)
        CREATEINP=1
        shift
        ;;
    -p | --prefix)
        EXEPREFIX=$2
        shift 2
        ;;
    -r | --repo)
        GITREPDIR=@2
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
cp expected/*.exp .

sed -i "s|/PATH/TO/DATASTORE|$DATASTORE|g" *.json
sed -i "s|/PATH/TO/METALLDATA|$GITREPDIR/..|g" *.json
sed -i "s|/PATH/TO/DATASTORE|$DATASTORE|g" *.exp
sed -i "s|/PATH/TO/METALLDATA|$GITREPDIR/..|g" *.exp

if [ $CREATEINP -eq 1 ]; then
  exit 0
fi

#
# run the tests
#
# note, output of mjl-head and mjl-info may not be stable

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-init" "mjl-init-names" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-read_json" "mjl-read_json-names" 1

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-init" "mjl-init-places" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-read_json" "mjl-read_json-places" 1

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-init" "mjl-init-results" 1

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-count" "mjl-count-names" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-head" "mjl-head-names" 0

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-head" "mjl-head-selected_places" 0
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-info" "mjl-info-selected_places" 0

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallJsonLines/mjl-merge" "mjl-merge" 1

rm -f ./m*.json ./m*.out ./m*.exp ./clippy*.log

echo "done."
