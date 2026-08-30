#!/bin/sh
set -eu

: "${PORT:=10000}"

# Build API target from the external hostname (public URL) or fall back
# to the internal hostport for local docker-compose usage.
if [ -n "${API_HOST:-}" ]; then
    API_TARGET="${API_HOST}"
    SCHEME="https"
else
    API_TARGET="${API_TARGET:=taskqueue-api:8080}"
    SCHEME="http"
fi

echo "==> Prometheus starting"
echo "    API_TARGET=${API_TARGET}"
echo "    SCHEME=${SCHEME}"
echo "    PORT=${PORT}"

sed -e "s|__API_TARGET__|${API_TARGET}|g" \
    -e "s|__SCHEME__|${SCHEME}|g" \
    /etc/prometheus/prometheus.render.yml > /tmp/prometheus.yml

echo "==> Generated config:"
cat /tmp/prometheus.yml

exec /bin/prometheus \
    --config.file=/tmp/prometheus.yml \
    --storage.tsdb.path=/prometheus \
    --storage.tsdb.retention.time=24h \
    --web.listen-address="0.0.0.0:${PORT}"
