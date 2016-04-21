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
HOST="127.0.0.1"
PORT=54321

CP=""
SEP=""
for j in ${LIB_DIR}/*.jar; do
  CP="${CP}${SEP}${j}"
  SEP=":"
done

JVM_ARGS="-Xmx128m -Xms128m -Dio.netty.eventLoopThreads=${NUM_SERVER_THREADS} -Dlog4j.configurationFile=${THIS_DIR}/log4j2.xml"

echo "$JAVA_HOME/bin/java -classpath ${CP} ${JVM_ARGS} timely.util.InsertTestData ${HOST} ${PORT} --fast"
exec $JAVA_HOME/bin/java -classpath ${CP} ${JVM_ARGS} timely.util.InsertTestData ${HOST} ${PORT} --fast
