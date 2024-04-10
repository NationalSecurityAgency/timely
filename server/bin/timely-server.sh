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

PID=$(jps -m | grep -E 'timely-server-.*-exec.jar timelyserver' | grep "instance=${INSTANCE}" | awk '{print $1}')

if [ "$PID" == "" ]; then
    THIS_DIR="${THIS_SCRIPT%/*}"
    BASE_DIR=$(cd "$THIS_DIR"/.. && pwd)
    LIB_DIR="${BASE_DIR}/lib"
    BIN_DIR="${BASE_DIR}/bin"
    export CONF_DIR="${BASE_DIR}/conf"
    NATIVE_DIR="${BIN_DIR}/META-INF/native"

    set -a
    . "${BIN_DIR}/timely-server-env.sh"
    set +a

    # use either a value from timely-server-env.sh or the default
    export LOG_DIR="${TIMELY_LOG_DIR:-${BASE_DIR}/logs}"

    if [[ ! -e ${NATIVE_DIR} ]]; then
        mkdir -p "${NATIVE_DIR}"
    fi

    if [[ ! -e ${LOG_DIR} ]]; then
        mkdir "${LOG_DIR}"
    fi

    JVM_ARGS="-Xmx4G -Xms4G -XX:NewSize=1G -XX:MaxNewSize=1G"
    JVM_ARGS="${JVM_ARGS} -Dlogging.config=${CONF_DIR}/log4j2.yml"
    JVM_ARGS="${JVM_ARGS} -Dlog4j2.contextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
    JVM_ARGS="${JVM_ARGS} -XX:+UseG1GC -XX:+UseStringDeduplication"
    JVM_ARGS="${JVM_ARGS} -Djava.net.preferIPv4Stack=true"
    JVM_ARGS="${JVM_ARGS} -XX:+UseNUMA"

    # for linux varieties of OS, extract native libraries if not already present
    if [[ -n "$OS_PATH" ]]; then
        if [[ ! -d ${NATIVE_DIR} || -z "$(ls -A ${NATIVE_DIR})" ]]; then
            pushd "${BASE_DIR}/bin" || exit
            "${JAVA_HOME}"/bin/jar xf "${LIB_DIR}"/netty-tcnative-boringssl-static-*.Final-"${OS_PATH}".jar META-INF/native/libnetty_tcnative_linux_"${HW}".so
            "${JAVA_HOME}"/bin/jar xf "${LIB_DIR}"/netty-transport-native-epoll-*-"${OS_PATH}".jar META-INF/native/libnetty_transport_native_epoll_"${HW}".so
            popd || exit
        fi
        JVM_ARGS="${JVM_ARGS} -Djava.library.path=${NATIVE_DIR}"
    fi

    echo "${JAVA_HOME}/bin/java ${JVM_ARGS} -jar ${BIN_DIR}/timely-server*-exec.jar timelyserver --instance=${INSTANCE}"
    nohup "${JAVA_HOME}"/bin/java ${JVM_ARGS} -jar "${BIN_DIR}"/timely-server-*-exec.jar timelyserver --instance="${INSTANCE}" >> "${LOG_DIR}/timely-server${INSTANCE}.out" 2>&1 &
else
    echo "timely-server --instance=${INSTANCE} already running with pid ${PID}"
fi
