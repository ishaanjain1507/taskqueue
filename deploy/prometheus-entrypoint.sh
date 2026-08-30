#!/bin/sh
set -eu

: "${API_TARGET:=taskqueue-api:8080}"
: "${PORT:=10000}"

echo "==> Prometheus starting"
echo "    API_TARGET=${API_TARGET}"
echo "    PORT=${PORT}"

sed "s|__API_TARGET__|${API_TARGET}|g" \
    /etc/prometheus/prometheus.render.yml > /tmp/prometheus.yml

echo "==> Generated config:"
cat /tmp/prometheus.yml

exec /bin/prometheus \
    --config.file=/tmp/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=24h \
    --web.listen-address="0.0.0.0:${PORT}"
