#!/usr/bin/env bash

set -u
set -o pipefail

BASE_URL=${1:-}
EMAIL=${2:-}
PASSWORD=${3:-}

if [[ -z "$BASE_URL" || -z "$EMAIL" || -z "$PASSWORD" ]]; then
  echo "Usage: bash scripts/smoke-test.sh <BASE_URL> <EMAIL> <PASSWORD>"
  exit 2
fi

BASE_URL=${BASE_URL%/}

FAILED=0
TOKEN=""
RESPONSE_BODY=""
RESPONSE_CODE=""

log_pass() {
  echo "PASS: $1"
}

log_fail() {
  echo "FAIL: $1"
  FAILED=1
}

request() {
  local method=$1
  local url=$2
  local data=${3:-}
  local auth=${4:-1}
  local tmp

  tmp=$(mktemp)
  local args=(-sS -X "$method" -o "$tmp" -w "%{http_code}")

  if [[ "$auth" == "1" && -n "$TOKEN" ]]; then
    args+=(-H "Authorization: Bearer $TOKEN")
  fi

  if [[ -n "$data" ]]; then
    args+=(-H "Content-Type: application/json" -d "$data")
  fi

  RESPONSE_CODE=$(curl "${args[@]}" "$BASE_URL$url")
  RESPONSE_BODY=$(cat "$tmp")
  rm -f "$tmp"
}

require_http_200() {
  local label=$1
  if [[ "$RESPONSE_CODE" != "200" ]]; then
    log_fail "$label (HTTP $RESPONSE_CODE)"
    return 1
  fi
  return 0
}

# login works (v1)
request "POST" "/v1/auth/login" "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" 0
if require_http_200 "login works"; then
  TOKEN=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
token = data.get("access_token") or data.get("accessToken") or data.get("token")
if not isinstance(token, str) or not token.strip():
    sys.exit(1)
print(token)
PY
  )
  if [[ -n "$TOKEN" ]]; then
    log_pass "login works"
  else
    log_fail "login works"
  fi
fi

# ports list works (v1)
request "GET" "/v1/ports?limit=50"
if require_http_200 "ports list works"; then
  PORTS_COUNT=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
ports = data.get("items")
if not isinstance(ports, list):
    sys.exit(1)
print(len(ports))
PY
  )
  if [[ -n "$PORTS_COUNT" && "$PORTS_COUNT" -ge 1 ]]; then
    log_pass "ports list works"
  else
    log_fail "ports list works"
  fi
fi

PORT_ID=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ.get("RESPONSE_BODY", "{}"))
ports = data.get("items")
if isinstance(ports, list) and ports:
    port_id = ports[0].get("id")
    if port_id:
        print(port_id)
        sys.exit(0)
print("")
PY
)

# port detail returns KPIs
if [[ -n "$PORT_ID" ]]; then
  request "GET" "/v1/ports/$PORT_ID"
  if require_http_200 "port detail returns KPIs"; then
    HAS_KPIS=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
kpis = data.get("metrics_7d")
if isinstance(kpis, dict):
    print("yes")
    sys.exit(0)
sys.exit(1)
PY
    )
    if [[ "$HAS_KPIS" == "yes" ]]; then
      log_pass "port detail returns KPIs"
    else
      log_fail "port detail returns KPIs"
    fi
  fi
else
  log_fail "port detail returns KPIs (no port id)"
fi

# port calls endpoint returns array
if [[ -n "$PORT_ID" ]]; then
  request "GET" "/v1/ports/$PORT_ID/calls"
  if require_http_200 "port calls endpoint returns array"; then
    CALLS_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
items = data.get("items")
if isinstance(items, list):
    print("yes")
    sys.exit(0)
sys.exit(1)
PY
    )
    if [[ "$CALLS_OK" == "yes" ]]; then
      log_pass "port calls endpoint returns array"
    else
      log_fail "port calls endpoint returns array"
    fi
  fi
else
  log_fail "port calls endpoint returns array (no port id)"
fi

# bbox map endpoint returns features array
request "GET" "/v1/vessels/positions?bbox=-180,-90,180,90"
if require_http_200 "bbox map endpoint returns features array"; then
  FEATURES_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
features = data.get("features")
if isinstance(features, list):
    print("yes")
    sys.exit(0)
sys.exit(1)
PY
  )
  if [[ "$FEATURES_OK" == "yes" ]]; then
    log_pass "bbox map endpoint returns features array"
  else
    log_fail "bbox map endpoint returns features array"
  fi
fi

# latest AIS timestamp is recent (endpoint exposes this)
request "GET" "/api/ais/status"
if require_http_200 "latest AIS timestamp is recent (endpoint exposes this)"; then
  AIS_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
healthy = data.get("isHealthy")
if healthy is True:
    print("yes")
    sys.exit(0)
sys.exit(1)
PY
  )
  if [[ "$AIS_OK" == "yes" ]]; then
    log_pass "latest AIS timestamp is recent (endpoint exposes this)"
  else
    log_fail "latest AIS timestamp is recent (endpoint exposes this)"
  fi
fi

# vessels list works / minimum dataset requirement
request "GET" "/v1/vessels?limit=50"
if require_http_200 "vessels list works"; then
  POSITIONS_COUNT=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
items = data.get("items")
if not isinstance(items, list):
    sys.exit(1)
print(len(items))
PY
  )
  if [[ -n "$POSITIONS_COUNT" && "$POSITIONS_COUNT" -ge 1 ]]; then
    log_pass "vessels list works"
  else
    log_fail "vessels list works"
  fi
fi

if [[ "$FAILED" -eq 0 ]]; then
  exit 0
fi

exit 1
