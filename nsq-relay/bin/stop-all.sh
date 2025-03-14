#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
    THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=$(cd $THIS_DIR/.. && pwd)
BIN_DIR=${BASE_DIR}/bin

shutdown_pid() {
    PID=$1
    if [ "$PID" != "" ]; then
        kill -15 $PID > /dev/null 2>&1
        timeout $2 pidwait $PID
        kill -9 $PID > /dev/null 2>&1
    fi
}

PID=`jps -m | grep -E 'timely-nsq-relay-.*-exec.jar' | awk '{print $1}'`
if [ "$PID" != "" ]; then
    echo "stopping timely-nsq-relay [${PID}]"
    shutdown_pid ${PID} 60
fi
