#!/usr/bin/env bash
# verify.sh — Automated + interactive verification of all sprint 9 requirements.
#
# Automated checks print PASS / FAIL.
# Manual (browser) steps pause and wait for the user to confirm before continuing.
#
# Usage (from repo root):
#   bash scripts/verify.sh

COMPOSE="docker compose"
CH_URL="http://localhost:8123"
CONNECT_URL="http://localhost:8083"
AUTH_URL="http://localhost:8000"
API_URL="http://localhost:8001"
KC_URL="http://localhost:8080"

GREEN='\033[0;32m'; RED='\033[0;31m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

PASS=0; FAIL=0; MANUAL=0

pass()    { echo -e "  ${GREEN}PASS${NC}  $*"; ((PASS++)); }
fail()    { echo -e "  ${RED}FAIL${NC}  $*"; ((FAIL++)); }
section() { echo -e "\n${CYAN}━━ $* ━━${NC}"; }

# Prints the manual step and waits for the user to press Enter.
manual() {
  ((MANUAL++))
  echo -e "\n  ${YELLOW}${BOLD}▶ MANUAL STEP $MANUAL${NC}"
  echo -e "  $*"
  echo -en "  ${YELLOW}Press Enter when done...${NC} "
  read -r
  echo -e "  ${GREEN}confirmed${NC}"
}

# ─────────────────────────────────────────────────────────────────────────────
section "Phase 0 — All containers running"

EXPECTED_SERVICES=(
  bionicpro-auth api pdf-service frontend
  app_db crm_db keycloak keycloak_db openldap
  clickhouse kafka zookeeper kafka-connect
  airflow-webserver airflow-scheduler airflow_db
  minio cdn
)

for SVC in "${EXPECTED_SERVICES[@]}"; do
  STATUS=$($COMPOSE ps "$SVC" --format "{{.Status}}" 2>/dev/null | head -1)
  if [[ -z "$STATUS" ]]; then
    CID=$($COMPOSE ps -q "$SVC" 2>/dev/null | head -1)
    if [[ -n "$CID" ]]; then
      RAW=$(docker inspect --format '{{.State.Status}}' "$CID" 2>/dev/null)
      [[ "$RAW" == "running" ]] && STATUS="Up (running)"
    fi
  fi
  if echo "$STATUS" | grep -qi "^up"; then
    pass "$SVC: $STATUS"
  else
    fail "$SVC: ${STATUS:-not found}"
  fi
done

# ─────────────────────────────────────────────────────────────────────────────
section "Task 1.2 — PKCE S256"

if grep -q '"pkce.code.challenge.method".*S256' keycloak/realm-export.json; then
  pass 'realm-export.json: pkce.code.challenge.method = S256'
else
  fail 'realm-export.json: pkce.code.challenge.method not set to S256'
fi

KC_TOKEN=$(curl -sf "$KC_URL/realms/master/protocol/openid-connect/token" \
  -d "client_id=admin-cli&grant_type=password&username=admin&password=admin" \
  2>/dev/null | grep -o '"access_token":"[^"]*"' | cut -d'"' -f4)

if [[ -n "$KC_TOKEN" ]]; then
  LIVE_PKCE=$(curl -sf "$KC_URL/admin/realms/reports-realm/clients?clientId=reports-frontend" \
    -H "Authorization: Bearer $KC_TOKEN" 2>/dev/null \
    | grep -o '"pkce[^"]*":"[^"]*"' | head -1)
  if echo "$LIVE_PKCE" | grep -q "S256"; then
    pass "Live Keycloak: $LIVE_PKCE"
  else
    fail "Live Keycloak: pkce not S256 (got: '${LIVE_PKCE}')"
  fi
else
  fail "Could not get Keycloak admin token"
fi

manual "Open http://localhost:3000 → click Login → check browser address bar: URL must contain code_challenge= and code_challenge_method=S256"

# ─────────────────────────────────────────────────────────────────────────────
section "Task 1.3 — BFF / session cookie"

if grep -q "HttpOnly: true" bionicpro-auth/main.go 2>/dev/null; then
  pass "bionicpro-auth: HttpOnly: true"
else
  fail "bionicpro-auth: HttpOnly not found in main.go"
fi

if grep -q "SameSite" bionicpro-auth/main.go 2>/dev/null; then
  pass "bionicpro-auth: SameSite set"
else
  fail "bionicpro-auth: SameSite not found"
fi

TTL=$(grep '"accessTokenLifespan"' keycloak/realm-export.json | grep -o '[0-9]*')
if [[ -n "$TTL" && "$TTL" -le 120 ]]; then
  pass "accessTokenLifespan: ${TTL}s (<= 120s)"
else
  fail "accessTokenLifespan: '${TTL}' (expected <= 120)"
fi

manual "Log in at http://localhost:3000. Open DevTools → Application → Cookies → confirm cookie named 'session_id' with HttpOnly flag. Confirm no 'access_token' key in Local Storage or Session Storage."

manual "Stay logged in and wait 2 minutes. Then click 'Download Report'. It must succeed (token auto-refreshed silently, no re-login prompt)."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 1.4 — LDAP"

if [[ -n "$KC_TOKEN" ]]; then
  LDAP_COUNT=$(curl -sf \
    "$KC_URL/admin/realms/reports-realm/components?type=org.keycloak.storage.UserStorageProvider" \
    -H "Authorization: Bearer $KC_TOKEN" 2>/dev/null | grep -c '"providerId":"ldap"' || true)
  if [[ "$LDAP_COUNT" -ge 1 ]]; then
    pass "Keycloak: LDAP federation configured"
  else
    fail "Keycloak: no LDAP UserStorageProvider found"
  fi

  LDAP_COMP_ID=$(curl -sf \
    "$KC_URL/admin/realms/reports-realm/components?type=org.keycloak.storage.UserStorageProvider" \
    -H "Authorization: Bearer $KC_TOKEN" 2>/dev/null \
    | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
  curl -s -o /dev/null -X POST \
    "$KC_URL/admin/realms/reports-realm/user-storage/$LDAP_COMP_ID/sync?action=triggerFullSync" \
    -H "Authorization: Bearer $KC_TOKEN" 2>/dev/null

  LDAP_USERS=$(curl -sf "$KC_URL/admin/realms/reports-realm/users?max=100" \
    -H "Authorization: Bearer $KC_TOKEN" 2>/dev/null \
    | grep -c '"federationLink"' || true)
  if [[ "$LDAP_USERS" -ge 1 ]]; then
    pass "Keycloak: ${LDAP_USERS} LDAP-synced user(s)"
  else
    fail "Keycloak: no LDAP-synced users found"
  fi
else
  fail "Cannot check LDAP — Keycloak admin token unavailable"
fi

manual "Open http://localhost:8080/admin → reports-realm → User Federation → confirm 'ldap' entry exists with Last Sync timestamp."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 1.5 — MFA / OTP"

TOTP_DEFAULT=$(grep -A3 '"CONFIGURE_TOTP"' keycloak/realm-export.json | grep '"defaultAction"' | head -1)
if echo "$TOTP_DEFAULT" | grep -q "true"; then
  pass "realm-export.json: CONFIGURE_TOTP defaultAction=true"
else
  fail "realm-export.json: CONFIGURE_TOTP not default action"
fi

if grep -q '"browserFlow".*"browser-mfa"' keycloak/realm-export.json; then
  pass "realm-export.json: browserFlow = browser-mfa"
else
  fail "realm-export.json: browserFlow not set to browser-mfa"
fi

manual "Log in as prothetic1 / prothetic123 at http://localhost:3000. The app must show an OTP setup screen (QR code). Scan with Google Authenticator, enter the 6-digit code, and confirm login succeeds. Log out and log in again — OTP must be required on the second login too."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 1.6 — Yandex OAuth"

if grep -q '"alias".*"yandex"' keycloak/realm-export.json; then
  pass "realm-export.json: Yandex identity provider defined"
else
  fail "realm-export.json: no Yandex identity provider"
fi

MAPPER_TYPE=$(grep -A4 '"yandex-role-prothetic_user"' keycloak/realm-export.json \
  | grep '"identityProviderMapper"' \
  | sed 's/.*"identityProviderMapper": *"\([^"]*\)".*/\1/')
if [[ "$MAPPER_TYPE" == "oidc-hardcoded-role-idp-mapper" ]]; then
  pass "Yandex role mapper: $MAPPER_TYPE"
else
  fail "Yandex role mapper: expected oidc-hardcoded-role-idp-mapper, got '${MAPPER_TYPE}'"
fi

SYNC_MODE=$(grep -A6 '"yandex-role-prothetic_user"' keycloak/realm-export.json \
  | grep '"syncMode"' | head -1 \
  | sed 's/.*"syncMode": *"\([^"]*\)".*/\1/')
if [[ "$SYNC_MODE" == "FORCE" ]]; then
  pass "Yandex role mapper syncMode: FORCE"
else
  fail "Yandex role mapper syncMode: expected FORCE, got '${SYNC_MODE}'"
fi

manual "Open http://localhost:3000 in incognito. Confirm a 'Sign in with Yandex' button is visible. Click it — browser must redirect to oauth.yandex.ru. After Yandex consent, confirm you are redirected back to the app and are logged in."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 2.2 — Airflow DAG"

DAG_PAUSED=$($COMPOSE exec -T airflow-webserver \
  airflow dags list --output plain 2>/dev/null \
  | awk '/bionicpro_etl/ {for(i=1;i<=NF;i++) if($i=="True"||$i=="False") {print $i; exit}}' | head -1)
if [[ "$DAG_PAUSED" == "False" ]]; then
  pass "bionicpro_etl: unpaused"
else
  fail "bionicpro_etl: paused or not found (is_paused='${DAG_PAUSED}')"
fi

SUCCESS_RUNS=$($COMPOSE exec -T airflow-webserver \
  airflow dags list-runs -d bionicpro_etl --output plain 2>/dev/null \
  | grep -c "success" || true)
if [[ "$SUCCESS_RUNS" -ge 1 ]]; then
  pass "bionicpro_etl: ${SUCCESS_RUNS} successful run(s)"
else
  fail "bionicpro_etl: no successful runs found"
fi

CH_MART=$(curl -sf "$CH_URL" \
  --data "SELECT count() FROM user_prosthetics_report FINAL" 2>/dev/null | tr -d '[:space:]')
if [[ "${CH_MART:-0}" -ge 1 ]]; then
  pass "user_prosthetics_report FINAL: ${CH_MART} rows"
else
  fail "user_prosthetics_report FINAL: 0 rows"
fi

manual "Open http://localhost:8082 (admin / admin). Open DAG 'bionicpro_etl'. Confirm at least one run where all 3 task circles are green (extract_crm → extract_telemetry → validate_datamart)."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 2.4 — Access control"

HTTP_NO_AUTH=$(curl -s -o /dev/null -w "%{http_code}" "$AUTH_URL/reports/me")
if [[ "$HTTP_NO_AUTH" == "401" ]]; then
  pass "GET /reports/me without auth → 401"
else
  fail "GET /reports/me without auth → ${HTTP_NO_AUTH} (expected 401)"
fi

HTTP_NO_COOKIE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/api/reports/me")
if [[ "$HTTP_NO_COOKIE" == "401" ]]; then
  pass "GET /api/reports/me without cookie → 401"
else
  fail "GET /api/reports/me without cookie → ${HTTP_NO_COOKIE} (expected 401)"
fi

manual "Log in as prothetic1. Copy the session_id cookie value from DevTools. Run: curl -v 'http://localhost:8001/api/reports/f992e346-c37e-4a8a-a5be-6920ade928f0' --cookie 'session_id=<value>' — must return 403."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 2.5 — PDF download"

FRONTEND=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:3000")
if [[ "$FRONTEND" == "200" ]]; then
  pass "Frontend http://localhost:3000 → 200"
else
  fail "Frontend http://localhost:3000 → ${FRONTEND}"
fi

manual "Log in at http://localhost:3000. Confirm a 'Download Report (PDF)' button is visible. Click it — browser must download a PDF file. Open DevTools → Network and confirm the request goes to /api/reports/me/pdf."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 3 — S3 / CDN"

MINIO=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9001")
if [[ "$MINIO" =~ ^(200|301|302)$ ]]; then
  pass "MinIO Console http://localhost:9001 → ${MINIO}"
else
  fail "MinIO Console http://localhost:9001 → ${MINIO}"
fi

CDN=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9080")
if [[ "$CDN" =~ ^(200|301|302|403|404)$ ]]; then
  pass "CDN (Nginx) http://localhost:9080 → ${CDN} (up)"
else
  fail "CDN (Nginx) http://localhost:9080 → ${CDN}"
fi

if grep -q "proxy_cache_valid" nginx/nginx.conf 2>/dev/null; then
  pass "nginx/nginx.conf: proxy_cache_valid present"
else
  fail "nginx/nginx.conf: proxy_cache_valid not found"
fi

manual "Click 'Download Report' in the app (first request). Then open http://localhost:9001 (minioadmin/minioadmin) and confirm a PDF object appears in the 'reports' bucket."

manual "Make a second PDF request. Then run: curl -I http://localhost:9080/\$(curl -s http://localhost:8001/api/reports/me/pdf --cookie 'session_id=<value>' | grep -o 'reports/[^\"]*') — check for X-Cache: HIT header in the response."

# ─────────────────────────────────────────────────────────────────────────────
section "Task 4 — CDC / Debezium"

CONNECTOR_STATE=$(curl -sf "$CONNECT_URL/connectors/crm-postgres-connector/status" \
  2>/dev/null | grep -o '"state":"[^"]*"' | head -1 | cut -d'"' -f4)
if [[ "$CONNECTOR_STATE" == "RUNNING" ]]; then
  pass "Debezium crm-postgres-connector: RUNNING"
else
  fail "Debezium crm-postgres-connector: '${CONNECTOR_STATE:-not found}'"
fi

RAW_CRM=$(curl -sf "$CH_URL" \
  --data "SELECT count() FROM raw_crm_customers FINAL" 2>/dev/null | tr -d '[:space:]')
if [[ "${RAW_CRM:-0}" -ge 3 ]]; then
  pass "raw_crm_customers FINAL: ${RAW_CRM} rows (>= 3)"
else
  fail "raw_crm_customers FINAL: ${RAW_CRM:-0} rows (expected >= 3)"
fi

RAW_ORDERS=$(curl -sf "$CH_URL" \
  --data "SELECT count() FROM raw_crm_orders FINAL" 2>/dev/null | tr -d '[:space:]')
if [[ "${RAW_ORDERS:-0}" -ge 3 ]]; then
  pass "raw_crm_orders FINAL: ${RAW_ORDERS} rows"
else
  fail "raw_crm_orders FINAL: ${RAW_ORDERS:-0} rows (expected >= 3)"
fi

manual "Run the end-to-end CDC test:
  1. docker compose exec crm_db psql -U crm_user crm -c \"INSERT INTO crm_customers (keycloak_id, first_name, last_name, email, phone) VALUES ('cdc-test-001','CDC','Test','cdc@test.com','+7-000-000-00-00');\"
  2. Wait 10 seconds.
  3. docker compose exec clickhouse clickhouse-client --query \"SELECT * FROM raw_crm_customers WHERE email='cdc@test.com'\"
  Confirm the new row appears in ClickHouse."

# ─────────────────────────────────────────────────────────────────────────────
section "Summary"

TOTAL=$((PASS + FAIL + MANUAL))
echo ""
echo -e "  ${GREEN}PASS${NC}   $PASS"
echo -e "  ${RED}FAIL${NC}   $FAIL"
echo -e "  ${YELLOW}MANUAL${NC} $MANUAL (confirmed interactively)"
echo -e "  Total  $TOTAL"
echo ""

if [[ "$FAIL" -eq 0 ]]; then
  echo -e "${GREEN}All checks passed. Ready to submit PR.${NC}"
  exit 0
else
  echo -e "${RED}${FAIL} check(s) failed.${NC} Fix the items above, then re-run."
  exit 1
fi
