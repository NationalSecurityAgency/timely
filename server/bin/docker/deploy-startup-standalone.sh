#!/bin/sh

WORKING="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${WORKING}/deploy-env.sh

set -e
${SRC_DIR}/bin/docker/genCerts.sh
${SRC_DIR}/bin/docker/deploy-timely-build.sh
${TIMELY_DIR}/bin/timely-standalone.sh

