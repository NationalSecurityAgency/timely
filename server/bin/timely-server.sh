#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=${THIS_DIR}/..
CONF_DIR="${BASE_DIR}/conf"
CONF_FILE="${CONF_DIR}/timely.properties"
LIB_DIR="${BASE_DIR}/lib"
NUM_SERVER_THREADS=4

export CLASSPATH="${LIB_DIR}/*"
JVM_ARGS="-Xmx128m -Xms128m -Dio.netty.eventLoopThreads=${NUM_SERVER_THREADS} -Dlog4j.configurationFile=${THIS_DIR}/log4j2.xml -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"

echo "$JAVA_HOME/bin/java ${JVM_ARGS} timely.Server "${CONF_FILE}""
$JAVA_HOME/bin/java ${JVM_ARGS} timely.Server "${CONF_FILE}"

