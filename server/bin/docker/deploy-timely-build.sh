#!/bin/sh

WORKING="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${WORKING}/deploy-env.sh

set -e
tar xzf ${SRC_DIR}/target/timely-server-$TIMELY_VERSION-dist.tar.gz -C ${TIMELY_DIR} --strip-components 1

