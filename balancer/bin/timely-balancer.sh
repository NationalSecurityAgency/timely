#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
        TCNATIVE_SUFFIX="jnilib"
else
        THIS_SCRIPT=`readlink -f $0`
        TCNATIVE_SUFFIX="so"
fi

THIS_DIR="${THIS_SCRIPT%/*}"

set -a
. ${THIS_DIR}/timely-balancer-env.sh
set +a

if [ -n "$1" ]; then
  export PROFILE_NUM=$1
  PROFILE_ARG="--spring.profiles.active=${PROFILE_NUM}"
  PID=`jps -m | grep timely.balancer.Balancer | grep "spring.profiles.active=${PROFILE_NUM}" | awk '{print $1}'`
else
  export PROFILE_NUM=""
  PID=`jps -m | grep timely.balancer.Balancer | awk '{print $1}'`
fi

if [ "$PID" == "" ]; then

    NATIVE_DIR="${THIS_DIR}/META-INF/native"
    BASE_DIR=$(cd $THIS_DIR/.. && pwd)
    TMP_DIR="${BASE_DIR}/tmp"
    CONF_DIR="${BASE_DIR}/conf"
    LIB_DIR="${BASE_DIR}/lib"
    export LOG_DIR="${BASE_DIR}/logs"
    NUM_SERVER_THREADS=4

    if [[ -e ${TMP_DIR} ]]; then
      rm -rf ${TMP_DIR}
    fi
    mkdir ${TMP_DIR}

    if [[ -e ${NATIVE_DIR} ]]; then
      rm -rf ${NATIVE_DIR}
    fi
    mkdir -p ${NATIVE_DIR}

    if [[ ! -e ${LOG_DIR} ]]; then
      mkdir ${LOG_DIR}
    fi

    pushd ${BASE_DIR}/bin
    $JAVA_HOME/bin/jar xf ${LIB_DIR}/netty-tcnative-boringssl-static*.jar META-INF/native/libnetty_tcnative_linux_x86_64.${TCNATIVE_SUFFIX}
    $JAVA_HOME/bin/jar xf ${LIB_DIR}/netty-all*.jar META-INF/native/libnetty_transport_native_epoll_x86_64.${TCNATIVE_SUFFIX}
    popd

    export CLASSPATH="${CONF_DIR}:${LIB_DIR}/*:${HADOOP_CONF_DIR}"
    JVM_ARGS="-Xmx1G -Xms1G -Dio.netty.eventLoopThreads=${NUM_SERVER_THREADS} -Dlog4j.configurationFile=${CONF_DIR}/log4j2-spring.xml"
    JVM_ARGS="${JVM_ARGS} -DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
    JVM_ARGS="${JVM_ARGS} -Djava.library.path=${NATIVE_DIR}"

    JVM_ARGS="${JVM_ARGS} -Xmx2G -Xms2G -XX:NewSize=1G -XX:MaxNewSize=1G"
    JVM_ARGS="${JVM_ARGS} -XX:+UseCompressedOops -XX:+UseNUMA -Djava.net.preferIPv4Stack=true"
    JVM_ARGS="${JVM_ARGS} -XX:+UseG1GC -XX:MaxGCPauseMillis=4000 -XX:ParallelGCThreads=20 -XX:+UseStringDeduplication"

    echo "$JAVA_HOME/bin/java ${JVM_ARGS} timely.balancer.Balancer --spring.config.name=timely-balancer ${PROFILE_ARG}"
    nohup $JAVA_HOME/bin/java ${JVM_ARGS} timely.balancer.Balancer --spring.config.name=timely-balancer ${PROFILE_ARG} >> ${LOG_DIR}/timely-balancer${PROFILE_NUM}-startup.log 2>&1 &

else
    if [ "$PROFILE_NUM" == ""]; then
        echo "timely-balancer already running with pid ${PID}"
    else
        echo "timely-balancer ${PROFILE_NUM} already running with pid ${PID}"
    fi
fi
