#!/usr/bin/env bash

EXIT_CODE=0

exec_test()
{
  # call: exec_test "$NP" "$BUILDROOT/src/MetallJsonLines/mjl-read_json" "mjl-init-names" 1
  EXEPREFIX=$1
  PROG=$2
  TEST=$3
  CHECK=$4

  CMD="$EXEPREFIX $PROG"

  BEGTIME=`date +%s`

  echo "$CMD <$TEST.json"
  $CMD <$TEST.json >$TEST.out

  if [ $? -ne 0 ]; then
    echo -e "\e[0;31m** FAILED\e[0m (non-zero return)"
    $EXIT_CODE=1
  else
    ENDTIME=`date +%s`
    echo "$((ENDTIME-BEGTIME))s"

    if [ $CHECK -eq 1 ]; then
      cmp --silent $TEST.out $TEST.exp || echo -e "\e[0;31m** FAILED\e[0m (unexpected output)"
      $EXIT_CODE=1
    fi
  fi
}

#
# set up defaults

DEFAULT_EXEPREFIX="mpirun -np 4"
DEFAULT_DATASTORE="/tmp"
DEFAULT_GITREPDIR=$(pwd)
DEFAULT_BUILDROOT="../build"
DEFAULT_CREATEINP=0
DEFAULT_KEEPFILES=0

EXEPREFIX=$DEFAULT_EXEPREFIX
DATASTORE=$DEFAULT_DATASTORE
BUILDROOT=$DEFAULT_BUILDROOT
GITREPDIR=$DEFAULT_GITREPDIR
CREATEINP=$DEFAULT_CREATEINP
KEEPFILES=$DEFAULT_KEEPFILES

#
# argument processing

VALID_ARGS=$(getopt -o hb:d:ikp:r: --long help,builddir:,datastore:,inputs,keepio,prefix:,repo: -- "$@")
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
        echo "  -k --keepio    keeps all input and expected output files after the tests"
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
    -k | --keepio)
        KEEPFILES=1
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

##
## run the tests

#
# MetallJsonLines tests
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


#
# MetallGraph tests

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-init" "mg-init" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count" "mg-count-0" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-read_vertices" "mg-read_vertices" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-read_edges" "mg-read_edges" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count_lines" "mg-count_lines" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count_lines" "mg-count_lines-selected" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count" "mg-count-1" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count" "mg-count-selected" 1

exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-init" "mg-softcom-init" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-read_edges" "mg-softcom-read_edges" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-count_lines" "mg-softcom-count_lines" 1
exec_test "$EXEPREFIX" "$BUILDROOT/src/MetallGraph/mg-cc" "mg-softcom-cc" 1

if [ $KEEPFILES -ne 1 ]; then
  rm -f ./m*.json ./m*.out ./m*.exp ./clippy*.log
fi

echo "done."

if [ $EXIT_CODE -eq 0 ]; then
  exit 0
else
  exit $EXIT_CODE
fi