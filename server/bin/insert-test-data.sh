#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
        THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
        THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=${THIS_DIR}/..
LIB_DIR="${BASE_DIR}/lib"
NUM_SERVER_THREADS=4

if [ -z ${TIMELY_HOST+x} ]; then
  echo "TIMELY_HOST is unset, using 127.0.0.1"
  TIMELY_HOST="127.0.0.1"
else
  echo "TIMELY_HOST is set to '$TIMELY_HOST'"
fi

if [ -z ${TIMELY_PORT+x} ]; then
  echo "TIMELY_PORT is unset, using 54321"
  TIMELY_PORT=54321
else
  echo "TIMELY_PORT is set to '$TIMELY_PORT'"
fi

CP=""
SEP=""
for j in ${LIB_DIR}/*.jar; do
  CP="${CP}${SEP}${j}"
  SEP=":"
done

JVM_ARGS="-Xmx128m -Xms128m -Dio.netty.eventLoopThreads=${NUM_SERVER_THREADS} -Dlog4j.configurationFile=${THIS_DIR}/log4j2.xml"

echo "$JAVA_HOME/bin/java -classpath ${CP} ${JVM_ARGS} timely.util.InsertTestData ${TIMELY_HOST} ${TIMELY_PORT}"
exec $JAVA_HOME/bin/java -classpath ${CP} ${JVM_ARGS} timely.util.InsertTestData ${TIMELY_HOST} ${TIMELY_PORT}
