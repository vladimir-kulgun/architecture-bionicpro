#!/bin/sh
# register.sh — wait for Kafka Connect to become ready, then register the CRM connector.
# Runs once at startup via the connector-init service.

set -e

CONNECT_URL="http://kafka-connect:8083"
CONNECTOR_NAME="crm-postgres-connector"
CONNECTOR_FILE="/debezium/crm-connector.json"

echo "[register.sh] Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; do
    sleep 3
done
echo "[register.sh] Kafka Connect is ready."

# Skip registration if the connector already exists (idempotent on container restart).
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}")

if [ "$HTTP_STATUS" = "200" ]; then
    echo "[register.sh] Connector '${CONNECTOR_NAME}' already registered — skipping."
    exit 0
fi

echo "[register.sh] Registering connector '${CONNECTOR_NAME}' ..."
curl -sf -X POST \
    -H "Content-Type: application/json" \
    --data "@${CONNECTOR_FILE}" \
    "${CONNECT_URL}/connectors"

echo ""
echo "[register.sh] Connector registered successfully."
