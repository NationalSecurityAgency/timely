#!/bin/bash

if [[ $(uname) == "Darwin" ]]; then
    THIS_SCRIPT=$(python -c 'import os,sys; print os.path.realpath(sys.argv[1])' "$0")
else
    THIS_SCRIPT=$(readlink -f "$0")
    HW=`uname -m`
    OS_PATH=linux-${HW}
fi

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

JVM_ARGS="-Xmx256m -Xms128m"
JVM_ARGS="${JVM_ARGS} -Dlogging.config=${CONF_DIR}/log4j2-console.yml"
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

echo "$JAVA_HOME/bin/java ${JVM_ARGS} -jar ${BIN_DIR}/timely-server*-exec.jar metrics"
$JAVA_HOME/bin/java ${JVM_ARGS} -jar ${BIN_DIR}/timely-server-*-exec.jar metrics
