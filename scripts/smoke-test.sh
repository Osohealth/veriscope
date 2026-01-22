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

# login works
request "POST" "/api/auth/login" "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" 0
if require_http_200 "login works"; then
  TOKEN=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
token = data.get("token")
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

# health endpoint works
request "GET" "/health" "" 0
if require_http_200 "health endpoint works"; then
  HEALTH_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
if data.get("status") != "ok":
    sys.exit(1)
if not isinstance(data.get("timeUtc"), str):
    sys.exit(1)
db = data.get("db")
ais = data.get("ais")
if not isinstance(db, dict) or not isinstance(db.get("ok"), bool):
    sys.exit(1)
if not isinstance(ais, dict) or not isinstance(ais.get("mode"), str):
    sys.exit(1)
if not isinstance(data.get("version"), str):
    sys.exit(1)
print("yes")
PY
  )
  if [[ "$HEALTH_OK" == "yes" ]]; then
    log_pass "health endpoint works"
  else
    log_fail "health endpoint works"
  fi
fi

# ports list works
request "GET" "/api/ports"
if require_http_200 "ports list works"; then
  PORTS_COUNT=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
ports = data.get("ports")
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
ports = data.get("ports")
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
  request "GET" "/api/ports/$PORT_ID"
  if require_http_200 "port detail returns KPIs"; then
    HAS_KPIS=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
kpis = data.get("kpis")
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
  request "GET" "/api/ports/$PORT_ID/calls"
  if require_http_200 "port calls endpoint returns array"; then
    CALLS_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
if isinstance(data, list):
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
request "GET" "/api/map/bbox?west=-180&south=-90&east=180&north=90"
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
request "GET" "/api/ais/latest"
if require_http_200 "latest AIS timestamp is recent (endpoint exposes this)"; then
  AIS_OK=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

data = json.loads(os.environ["RESPONSE_BODY"])
ts = data.get("timestamp")
if not isinstance(ts, str) or not ts:
    sys.exit(1)
if ts.endswith("Z"):
    ts = ts[:-1] + "+00:00"
try:
    dt = datetime.fromisoformat(ts)
except ValueError:
    sys.exit(1)
if dt.tzinfo is None:
    dt = dt.replace(tzinfo=timezone.utc)
now = datetime.now(timezone.utc)
age_seconds = (now - dt).total_seconds()
if age_seconds <= 900:
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

# positions list works / minimum dataset requirement
request "GET" "/api/positions"
if require_http_200 "positions list works"; then
  POSITIONS_COUNT=$(RESPONSE_BODY="$RESPONSE_BODY" python - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE_BODY"])
positions = data.get("positions")
if not isinstance(positions, list):
    sys.exit(1)
print(len(positions))
PY
  )
  if [[ -n "$POSITIONS_COUNT" && "$POSITIONS_COUNT" -ge 1 ]]; then
    log_pass "positions list works"
  else
    log_fail "positions list works"
  fi
fi

if [[ "$FAILED" -eq 0 ]]; then
  exit 0
fi

exit 1
