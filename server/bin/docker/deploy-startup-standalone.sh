#!/bin/sh

set -e
/timely-server-src/bin/docker/deploy-timely-build.sh
/timely/bin/timely-standalone.sh

