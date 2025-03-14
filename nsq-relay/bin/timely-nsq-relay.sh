#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
    THIS_SCRIPT=$(python -c 'import os,sys; print os.path.realpath(sys.argv[1])' "$0")
else
    THIS_SCRIPT=$(readlink -f "$0")
    HW=`uname -m`
    OS_PATH=linux-${HW}
fi

if [ -z "$1" ]; then
    export INSTANCE="1"
else
    export INSTANCE="$1"
fi

PID=$(jps -m | grep -E 'timely-nsq-relay-.*-exec.jar' | awk '{print $1}')

if [ "$PID" == "" ]; then
    THIS_DIR="${THIS_SCRIPT%/*}"
    BASE_DIR=$(cd "$THIS_DIR"/.. && pwd)
    LIB_DIR="${BASE_DIR}/lib"
    BIN_DIR="${BASE_DIR}/bin"
    export CONF_DIR="${BASE_DIR}/conf"

    set -a
    . "${BIN_DIR}/timely-nsq-relay-env.sh"
    set +a

    # use either a value from timely-server-env.sh or the default
    export LOG_DIR="${TIMELY_LOG_DIR:-${BASE_DIR}/logs}"

    if [[ ! -e ${LOG_DIR} ]]; then
        mkdir "${LOG_DIR}"
    fi

    JVM_ARGS="-Xmx4G -Xms4G -XX:NewSize=1G -XX:MaxNewSize=1G"
    JVM_ARGS="${JVM_ARGS} -Dlogging.config=${CONF_DIR}/log4j2.yml"
    JVM_ARGS="${JVM_ARGS} -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
    JVM_ARGS="${JVM_ARGS} -XX:+UseG1GC -XX:+UseStringDeduplication"
    JVM_ARGS="${JVM_ARGS} -Djava.net.preferIPv4Stack=true"
    JVM_ARGS="${JVM_ARGS} -XX:+UseNUMA"
    JVM_ARGS="${JVM_ARGS} -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:6005"

    echo "${JAVA_HOME}/bin/java ${JVM_ARGS} -jar ${BIN_DIR}/timely-nsq-relay*-exec.jar"
    nohup "${JAVA_HOME}"/bin/java ${JVM_ARGS} -jar "${BIN_DIR}"/timely-nsq-relay-*-exec.jar >> "${LOG_DIR}/timely-nsq-relay.out" 2>&1 &
else
    echo "timely-nsq-relay already running with pid ${PID}"
fi
