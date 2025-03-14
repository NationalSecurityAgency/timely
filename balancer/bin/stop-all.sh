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

shutdown_pid() {
    PID=$1
    if [ "$PID" != "" ]; then
        kill -15 $PID > /dev/null 2>&1
        timeout $2 pidwait $PID
        kill -9 $PID > /dev/null 2>&1
    fi
}

for i in $(seq 1 $NUM_INSTANCES); do
    PID=`jps -m | grep -E 'timely-balancer-.*-exec.jar' | grep "instance=${i}" | awk '{print $1}'`
    if [ "$PID" != "" ]; then
        echo "stopping timely-balancer instance ${i} [${PID}]"
        shutdown_pid ${PID} 60
    fi
done
