#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=${THIS_DIR}/../..
TMP_DIR="${BASE_DIR}/tmp"
CONF_DIR="${BASE_DIR}/conf"
LIB_DIR="${BASE_DIR}/lib"

. ${THIS_DIR}/load-test-env.sh

export CLASSPATH="${CONF_DIR}:${THIS_DIR}:${LIB_DIR}/*"
JVM_ARGS="-Xmx1G -Xms1G"

echo "$JAVA_HOME/bin/java ${JVM_ARGS} timely.testing.MetricQueryLoadTest $*"
$JAVA_HOME/bin/java ${JVM_ARGS} timely.testing.MetricQueryLoadTest $*
