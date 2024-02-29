#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=$(cd $THIS_DIR/../.. && pwd)
LIB_DIR="${BASE_DIR}/lib"
export BIN_DIR="${BASE_DIR}/bin"
export CONF_DIR="${BASE_DIR}/conf"
export LOG_DIR="${BASE_DIR}/logs"

set -a
. ${BIN_DIR}/timely-server-env.sh
set +a

JVM_ARGS="-Xmx1G -Xms1G"
JVM_ARGS="${JVM_ARGS} -Dlogging.config=${CONF_DIR}/log4j2-console.yml"
JVM_ARGS="${JVM_ARGS} -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"

echo "$JAVA_HOME/bin/java ${JVM_ARGS} -jar ${THIS_DIR}/timely-server*-exec.jar testingest --spring.profiles.active=timely,loadtest"
$JAVA_HOME/bin/java ${JVM_ARGS} -jar ${THIS_DIR}/timely-server-*-exec.jar testingest --spring.profiles.active=timely,loadtest
