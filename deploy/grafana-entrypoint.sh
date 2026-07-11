#!/bin/sh
set -eu

if [ -z "${PROMETHEUS_URL:-}" ]; then
    : "${PROMETHEUS_HOSTPORT:=taskqueue-prometheus:10000}"
    export PROMETHEUS_URL="http://${PROMETHEUS_HOSTPORT}"
fi

exec /run.sh "$@"
