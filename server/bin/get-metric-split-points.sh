#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=$(cd $THIS_DIR/.. && pwd)
CONF_DIR="${BASE_DIR}/conf"
LIB_DIR="${BASE_DIR}/lib"

export CLASSPATH="${CONF_DIR}:${LIB_DIR}/*"
JVM_ARGS="-Xmx256m -Xms128m"
JVM_ARGS="${JVM_ARGS} -Dlogging.config=${CONF_DIR}/log4j2-error-console.xml"
JVM_ARGS="${JVM_ARGS} -Dlog4j.configurationFile=${CONF_DIR}/log4j2-error-console.xml"
JVM_ARGS="${JVM_ARGS} -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"

echo "$JAVA_HOME/bin/java ${JVM_ARGS} timely.util.GetMetricTableSplitPoints --spring.config.name=timely"
$JAVA_HOME/bin/java ${JVM_ARGS} timely.util.GetMetricTableSplitPoints --spring.config.name=timely
