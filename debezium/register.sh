#!/bin/sh
# register.sh — wait for Kafka Connect to become ready, then register the CRM connector.
# Runs once at startup via the connector-init service.

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
    echo "[register.sh] Connector '${CONNECTOR_NAME}' already registered."
    echo "[register.sh] Current status:"
    curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | \
        sed 's/,/,\n  /g'
    echo ""
    exit 0
fi

echo "[register.sh] Registering connector '${CONNECTOR_NAME}' (retries up to 30×10s) ..."
ATTEMPTS=0
HTTP_CODE=""
while [ "$ATTEMPTS" -lt 30 ]; do
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        --data "@${CONNECTOR_FILE}" \
        "${CONNECT_URL}/connectors")
    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | head -n -1)
    if [ "$HTTP_CODE" = "201" ] || [ "$HTTP_CODE" = "409" ]; then
        break
    fi
    ATTEMPTS=$((ATTEMPTS + 1))
    echo "[register.sh] HTTP ${HTTP_CODE} — retrying in 10s (attempt ${ATTEMPTS}/30)..."
    echo "  $BODY" | head -c 200
    sleep 10
done

if [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "409" ]; then
    echo "[register.sh] ERROR: could not register connector after 30 attempts (HTTP ${HTTP_CODE})"
    echo "$BODY"
    exit 1
fi

echo "[register.sh] Connector registered (HTTP ${HTTP_CODE})."
echo ""

# Wait for the connector to reach RUNNING state.
echo "[register.sh] Waiting for connector to start ..."
ATTEMPTS=0
while [ "$ATTEMPTS" -lt 20 ]; do
    CONNECTOR_STATE=$(curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" 2>/dev/null | \
        grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
    if [ "$CONNECTOR_STATE" = "RUNNING" ]; then
        break
    fi
    ATTEMPTS=$((ATTEMPTS + 1))
    sleep 3
done

echo "[register.sh] Connector status:"
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | \
    sed 's/,/,\n  /g'
echo ""

if [ "$CONNECTOR_STATE" != "RUNNING" ]; then
    echo "[register.sh] WARNING: connector did not reach RUNNING state in time."
    echo "[register.sh] Check: curl ${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
    exit 1
fi

echo "[register.sh] Done. Debezium is streaming CRM changes to Kafka."
echo "[register.sh] Topics:"
echo "    crm.public.crm_customers"
echo "    crm.public.crm_orders"
echo "    crm.public.crm_service_history"
