#!/bin/sh

WORKING="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

. ${WORKING}/deploy-env.sh

${SRC_DIR}/bin/docker/deploy-timely-build.sh


${SRC_DIR}/bin/docker/wait-for-it.sh timely:54322 -t 45


# enable app
curl -k -H "Content-Type: application/json" -d \
'{
  "enabled":true
}' \
 https://admin:admin@grafana:3000/api/plugins/timely-app/settings

# create data source
curl -k -H "Content-Type: application/json" -d \
'{

        "name":"Timely",
        "type":"timely-datasource",
        "Access":"direct",
        "jsonData":
        {
                "timelyHost":"localhost",
                "httpsPort":"54322",
                "wsPort":"54323",
                "basicAuths":true
        }
}' \
 https://admin:admin@grafana:3000/api/datasources


# import the dashboard
curl -k -H "Content-Type: application/json" -d \
'{
        "pluginId":"timely-datasource",
        "path":"dashboards/standalone_test.json",
        "overwrite":false,
        "inputs":[{
                        "name":"*",
                        "type":"datasource",
                        "pluginId":"timely-datasource",
                        "value":"Timely"
                }]
}' \
 https://admin:admin@grafana:3000/api/dashboards/import



${TIMELY_DIR}/bin/insert-test-data.sh

