#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-}"
EMAIL="${2:-}"
PASSWORD="${3:-}"

if [[ -z "${BASE_URL}" || -z "${EMAIL}" || -z "${PASSWORD}" ]]; then
  echo "Usage: bash scripts/smoke-test.sh <BASE_URL> <EMAIL> <PASSWORD>"
  echo "Example: bash scripts/smoke-test.sh https://veriscopeai.replit.app test@x.com Pass123!"
  exit 2
fi

need() { command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 2; }; }
need curl
need jq

echo "== Phase-1 Smoke Test =="
echo "BASE_URL=${BASE_URL}"
echo

# 1) Health (if you have it; strongly recommended)
echo "[1/8] GET /health (recommended)"
if curl -fsS "${BASE_URL}/health" >/dev/null 2>&1; then
  curl -fsS "${BASE_URL}/health" | jq .
  echo "PASS: /health"
else
  echo "WARN: /health not found (recommended to add)"
fi
echo

# 2) OpenAPI docs should load (not strict JSON requirement)
echo "[2/8] GET /docs"
curl -fsS "${BASE_URL}/docs" >/dev/null
echo "PASS: /docs loads"
echo

# 3) Login
echo "[3/8] POST /v1/auth/login (fallback /api/auth/login)"
LOGIN_JSON=$(curl -fsS -X POST "${BASE_URL}/v1/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${EMAIL}\",\"password\":\"${PASSWORD}\"}" \
  || curl -fsS -X POST "${BASE_URL}/api/auth/login" \
  -H "Content-Type: application/json" \
  -d "{\"email\":\"${EMAIL}\",\"password\":\"${PASSWORD}\"}")

TOKEN=$(echo "${LOGIN_JSON}" | jq -r '.access_token // .token // empty')
if [[ -z "${TOKEN}" || "${TOKEN}" == "null" ]]; then
  echo "FAIL: login did not return access_token/token"
  echo "${LOGIN_JSON}" | jq .
  exit 1
fi
echo "PASS: login returns access_token"
echo

AUTH_HEADER=("Authorization: Bearer ${TOKEN}")

# 4) Ports list
echo "[4/8] GET /v1/ports?limit=5"
PORTS_JSON=$(curl -fsS "${BASE_URL}/v1/ports?limit=5" -H "${AUTH_HEADER[@]}")
PORT_ID=$(echo "${PORTS_JSON}" | jq -r '.items[0].id // .data[0].id // .ports[0].id // empty')
if [[ -z "${PORT_ID}" || "${PORT_ID}" == "null" ]]; then
  echo "FAIL: /v1/ports did not return items/data with id"
  echo "${PORTS_JSON}" | jq .
  exit 1
fi
echo "PASS: ports list returns port_id=${PORT_ID}"
echo

# 5) Port detail KPIs
echo "[5/8] GET /v1/ports/${PORT_ID}"
PORT_DETAIL=$(curl -fsS "${BASE_URL}/v1/ports/${PORT_ID}" -H "${AUTH_HEADER[@]}")
ARRIVALS=$(echo "${PORT_DETAIL}" | jq -r '.metrics_7d.arrivals // empty')
if [[ -z "${ARRIVALS}" || "${ARRIVALS}" == "null" ]]; then
  echo "FAIL: port detail missing metrics_7d.arrivals"
  echo "${PORT_DETAIL}" | jq .
  exit 1
fi
echo "PASS: port detail has metrics_7d"
echo

# 6) Port calls
echo "[6/8] GET /v1/ports/${PORT_ID}/calls?limit=5"
CALLS=$(curl -fsS "${BASE_URL}/v1/ports/${PORT_ID}/calls?limit=5" -H "${AUTH_HEADER[@]}")
echo "${CALLS}" | jq .
echo "PASS: port calls endpoint responds"
echo

# 7) Vessels list (basic)
echo "[7/8] GET /v1/vessels?limit=5 (if supported)"
if curl -fsS "${BASE_URL}/v1/vessels?limit=5" -H "${AUTH_HEADER[@]}" >/dev/null 2>&1; then
  VESSELS=$(curl -fsS "${BASE_URL}/v1/vessels?limit=5" -H "${AUTH_HEADER[@]}")
  VESSEL_ID=$(echo "${VESSELS}" | jq -r '.items[0].id // .data[0].id // .items[0].vessel_id // empty')
  if [[ -n "${VESSEL_ID}" && "${VESSEL_ID}" != "null" ]]; then
    echo "PASS: vessels list returns vessel_id=${VESSEL_ID}"
    echo
    echo "[7b/8] GET /v1/vessels/${VESSEL_ID}/latest-position"
    curl -fsS "${BASE_URL}/v1/vessels/${VESSEL_ID}/latest-position" -H "${AUTH_HEADER[@]}" | jq .
    echo "PASS: latest-position returns"
  else
    echo "WARN: vessels list returned but no items[0].id"
    echo "${VESSELS}" | jq .
  fi
else
  echo "WARN: /v1/vessels?limit=5 not supported; skipping"
fi
echo

# 8) BBOX endpoint (required for Phase-1)
echo "[8/8] GET /v1/vessels/positions?bbox=... (REQUIRED)"
BBOX="3.0,51.7,4.8,52.2" # Rotterdam-ish; adjust if needed
POS_JSON=$(curl -fsS "${BASE_URL}/v1/vessels/positions?bbox=${BBOX}&sinceMinutes=180&limit=2000" -H "${AUTH_HEADER[@]}")

# accept {data:[]} or FeatureCollection/items; adjust to your final schema
FEATURES_LEN=$(echo "${POS_JSON}" | jq -r '(.features | length) // (.items | length) // (.data | length) // empty')
if [[ -z "${FEATURES_LEN}" || "${FEATURES_LEN}" == "null" ]]; then
  echo "FAIL: bbox endpoint response does not contain .features/.items/.data"
  echo "${POS_JSON}" | jq .
  exit 1
fi

echo "PASS: bbox endpoint returns count=${FEATURES_LEN}"
echo
echo "== ALL REQUIRED CHECKS PASSED =="
