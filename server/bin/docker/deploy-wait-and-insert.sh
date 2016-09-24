#!/bin/sh

WORKING="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${WORKING}/deploy-env.sh

${SRC_DIR}/bin/docker/deploy-timely-build.sh


${SRC_DIR}/bin/docker/wait-for-it.sh timely:54322 -t 45

# create datasource in grafana
#curl -X POST -u admin:admin -H "Content-Type: application/json" -d \
#'{"name":"Timely","type":"opentsdb","url":"https://timely:54322","access":"proxy","jsonData":{"tsdbVersion":1,"tsdbResolution":1}}' \
#http://grafana:3000/api/datasources

# insert the dashboard
#curl -X POST -u admin:admin -H "Content-Type: application/json" \
#-d @/timely-server-src/bin/docker/standalone_test.json \
#http://grafana:3000/api/dashboards/db

${TIMELY_DIR}/bin/insert-test-data.sh

