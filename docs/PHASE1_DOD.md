# Phase 1 Definition of Done (DoD)

This document defines the **Phase-1 correctness gates** for VeriScope. Phase-1 is considered complete only when every check below passes and the smoke test exits successfully.

## Required endpoints and response shapes

> All endpoints are relative to `BASE_URL` (e.g., `https://api.example.com`).

| Check | Endpoint | Method | Expected response shape | Pass criteria |
| --- | --- | --- | --- | --- |
| Login works | `/api/auth/login` | `POST` | `{ "token": "<jwt>" }` | HTTP 200 and `token` is a non-empty string. |
| Health endpoint works | `/health` | `GET` | `{ "status": "ok", "timeUtc": "<ISO-8601>", "db": { "ok": <bool> }, "ais": { "mode": "<string>" }, "version": "<string>" }` | HTTP 200 and fields are present, with `db.ok` reflecting DB reachability. |
| Ports list works | `/api/ports` | `GET` | `{ "ports": [ { "id": "<id>", "name": "<string>" } ] }` | HTTP 200 and `ports` is an array. |
| Port detail returns KPIs | `/api/ports/{portId}` | `GET` | `{ "id": "<id>", "name": "<string>", "kpis": { ... } }` | HTTP 200 and `kpis` is an object. |
| Port calls endpoint returns array | `/api/ports/{portId}/calls` | `GET` | `[ { ... } ]` | HTTP 200 and the root response is an array. |
| BBox map endpoint returns features array | `/api/map/bbox?west=<w>&south=<s>&east=<e>&north=<n>` | `GET` | `{ "features": [ { ... } ] }` | HTTP 200 and `features` is an array. |
| Latest AIS timestamp is recent | `/api/ais/latest` | `GET` | `{ "timestamp": "<ISO-8601>" }` | HTTP 200 and `timestamp` is within the last **15 minutes**. |
| Positions list works | `/api/positions` | `GET` | `{ "positions": [ { ... } ] }` | HTTP 200 and `positions` is an array. |

## Data freshness requirements

* **Latest AIS timestamp** must be **less than 15 minutes old** compared to the current UTC time.

## Minimum dataset requirements

* **Ports count**: `ports.length >= 1`
* **Positions count**: `positions.length >= 1`

## Phase-1 checks (exact list)

* login works
* health endpoint works
* ports list works
* port detail returns KPIs
* port calls endpoint returns array
* bbox map endpoint returns features array
* latest AIS timestamp is recent (endpoint exposes this)

## Smoke test

Run:

```bash
bash scripts/smoke-test.sh $BASE_URL $EMAIL $PASSWORD
```

The script logs `PASS`/`FAIL` for each check, and exits **0** only if all checks pass.
