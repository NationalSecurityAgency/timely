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

. ${BIN_DIR}/timely-balancer-env.sh

HOST_IP=$(hostname -I | awk '{print $1}')
HOST=$(hostname -s)
HOST_FQDN=$(hostname -f)

for i in $(seq 1 $NUM_PROFILES); do
    if [ "$NUM_PROFILES" -eq "1" ]; then
        FILENAME=timely-balancer.yml
    else
        FILENAME=timely-balancer-${i}.yml
    fi

    echo "creating configuration file ${CONF_DIR}/${FILENAME}"
    cp ${CONF_DIR}/timely-balancer-template.yml ${CONF_DIR}/${FILENAME}
    sed -i "s/\${HOST}/${HOST}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${HOST_FQDN}/${HOST_FQDN}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${HOST_IP}/${HOST_IP}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${ZOOKEEPERS}/${ZOOKEEPERS}/g" ${CONF_DIR}/${FILENAME}
    MULTIPLE=$(($i - 1))
    TCP_PORT=$(($TCP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${TCP_PORT}/${TCP_PORT}/g" ${CONF_DIR}/${FILENAME}
    HTTP_PORT=$(($HTTP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${HTTP_PORT}/${HTTP_PORT}/g" ${CONF_DIR}/${FILENAME}
    WEBSOCKET_PORT=$(($WEBSOCKET_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${WEBSOCKET_PORT}/${WEBSOCKET_PORT}/g" ${CONF_DIR}/${FILENAME}
    UDP_PORT=$(($UDP_PORT_START + $MULTIPLE * $PORT_INCREMENT))
    sed -i "s/\${UDP_PORT}/${UDP_PORT}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${INSTANCE}/${i}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s#\${SERVER_CERTIFICATE_FILE}#${SERVER_CERTIFICATE_FILE}#g" ${CONF_DIR}/${FILENAME}
    sed -i "s#\${SERVER_KEY_FILE}#${SERVER_KEY_FILE}#g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${SERVER_KEY_PASSWORD}/${SERVER_KEY_PASSWORD}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${SERVER_USE_GENERATED_KEYPAIR}/${SERVER_USE_GENERATED_KEYPAIR}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s#\${CLIENT_KEY_FILE}#${CLIENT_KEY_FILE}#g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${CLIENT_KEY_TYPE}/${CLIENT_KEY_TYPE}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${CLIENT_KEY_PASSWORD}/${CLIENT_KEY_PASSWORD}/g" ${CONF_DIR}/${FILENAME}
    sed -i "s#\${TRUST_STORE_PEM}#${TRUST_STORE_PEM}#g" ${CONF_DIR}/${FILENAME}
    sed -i "s#\${TRUST_STORE_JKS}#${TRUST_STORE_JKS}#g" ${CONF_DIR}/${FILENAME}
    sed -i "s/\${TRUST_STORE_PASSWORD}/${TRUST_STORE_PASSWORD}/g" ${CONF_DIR}/${FILENAME}
done

