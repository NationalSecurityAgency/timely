#!/bin/bash

if [[ `uname` == "Darwin" ]]; then
    THIS_SCRIPT=`python -c 'import os,sys; print os.path.realpath(sys.argv[1])' $0`
else
    THIS_SCRIPT=`readlink -f $0`
fi

THIS_DIR="${THIS_SCRIPT%/*}"
BASE_DIR=$(cd $THIS_DIR/.. && pwd)
BIN_DIR=${BASE_DIR}/bin
CONF_DIR=${BASE_DIR}/conf

. ${BIN_DIR}/timely-env.sh

for i in $(seq 1 $NUM_PROFILES); do
    if [ "$NUM_PROFILES" -eq "1" ]; then
        FILENAME=timely.yml
    else
        FILENAME=timely-${i}.yml
    fi

    echo "creating configuration file ${CONF_DIR}/${FILENAME}"
    cp ${CONF_DIR}/timely-template.yml ${CONF_DIR}/${FILENAME}
    sed -i "s/\${ZOOKEEPERS}/${ZOOKEEPERS}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${AGE_OFF_DAYS}/${AGE_OFF_DAYS}/g" ${CONF_DIR}/${FILENAME}
    MULTIPLE=$(($i - 1))
    TCP_PORT=$(($TCP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${TCP_PORT}/${TCP_PORT}/g" ${CONF_DIR}/${FILENAME}
    HTTP_PORT=$(($HTTP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${HTTP_PORT}/${HTTP_PORT}/g" ${CONF_DIR}/${FILENAME}
    WEBSOCKET_PORT=$(($WEBSOCKET_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${WEBSOCKET_PORT}/${WEBSOCKET_PORT}/g" ${CONF_DIR}/${FILENAME}
    UDP_PORT=$(($UDP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${UDP_PORT}/${UDP_PORT}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${ACCUMULO_USER}/${ACCUMULO_USER}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${ACCUMULO_PASSWORD}/${ACCUMULO_PASSWORD}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${INSTANCE}/${i}/g" ${CONF_DIR}/${FILENAME}
done

