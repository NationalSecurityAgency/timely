#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
    THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=$(cd $THIS_DIR/.. && pwd)
BIN_DIR=${BASE_DIR}/bin

. ${BIN_DIR}/timely-balancer-env.sh

for i in $(seq 1 $NUM_INSTANCES); do
    PID=`jps -m | grep -E 'timely-balancer-.*-exec.jar' | grep "instance=${i}" | awk '{print $1}'`
    if [ "$PID" != "" ]; then
        echo "stopping timely-balancer instance ${i} [${PID}]"
        kill -9 $PID
    fi
done
