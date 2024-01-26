#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
        TCNATIVE_SUFFIX="jnilib"
else
        THIS_SCRIPT=`readlink -f $0`
        TCNATIVE_SUFFIX="so"
fi

THIS_DIR="${THIS_SCRIPT%/*}"
NATIVE_DIR="${THIS_DIR}/META-INF/native"
BASE_DIR=$(cd $THIS_DIR/.. && pwd)
TMP_DIR="${BASE_DIR}/tmp"
CONF_DIR="${BASE_DIR}/conf"
LIB_DIR="${BASE_DIR}/lib"
NUM_SERVER_THREADS=4

if [[ -e ${TMP_DIR} ]]; then
  rm -rf ${TMP_DIR}
fi
mkdir ${TMP_DIR}

if [[ -e ${NATIVE_DIR} ]]; then
  rm -rf ${NATIVE_DIR}
fi
mkdir -p ${NATIVE_DIR}

pushd ${BASE_DIR}/bin
$JAVA_HOME/bin/jar xf ${LIB_DIR}/netty-tcnative-boringssl-static*.jar META-INF/native/libnetty_tcnative_linux_x86_64.${TCNATIVE_SUFFIX}
$JAVA_HOME/bin/jar xf ${LIB_DIR}/netty-all*.jar META-INF/native/libnetty_transport_native_epoll_x86_64.${TCNATIVE_SUFFIX}
popd

export CLASSPATH="${CONF_DIR}:${LIB_DIR}/*:${HADOOP_CONF_DIR}"
JVM_ARGS="-Xmx256m -Xms256m -Dio.netty.eventLoopThreads=${NUM_SERVER_THREADS} -Dlog4j.configurationFile=${CONF_DIR}/log4j2-spring.xml"
JVM_ARGS="${JVM_ARGS} -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
JVM_ARGS="${JVM_ARGS} -Djava.library.path=${NATIVE_DIR}"
# for debugging
#JVM_ARGS="${JVM_ARGS} -agentlib:jdwp=transport=dt_socket,address=54323,server=y,suspend=y"

echo "$JAVA_HOME/bin/java ${JVM_ARGS} timely.StandaloneServer "${TMP_DIR}" --spring.config.name=timely --spring.profiles.active=standalone"
$JAVA_HOME/bin/java ${JVM_ARGS} timely.StandaloneServer "${TMP_DIR}" --spring.config.name=timely --spring.profiles.active=standalone

