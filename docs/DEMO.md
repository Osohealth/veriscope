# Demo Loop ? Escalation Golden Path

## Prerequisites
- DB reachable (uses the same DB as dev server)
- Optional env overrides:
  - `DEMO_BASE_URL` (default `http://localhost:5000`)
  - `DEMO_API_KEY` (default `vs_demo_key`)
  - `DEMO_TENANT_ID`
  - `DEMO_OWNER_USER_ID` / `DEMO_OWNER_EMAIL`
  - `DEMO_ALERT_EMAIL` / `DEMO_ALERT_WEBHOOK`
  - `DEMO_POLICY_NAME_EMAIL` / `DEMO_POLICY_NAME_WEBHOOK`

## Quickstart (Investor Mode)
Terminal A:
```bash
npm run demo:server
```

Terminal B:
```bash
npm run demo:all
```

Expected:
- Baseline: EMAIL + WEBHOOK deliveries created
- Cooldown: run #2 increments `deliveries_blocked_cooldown_total`
- DLQ: run #2 increments `deliveries_blocked_dlq_total`

## Run
Baseline:
```bash
npm run demo:escalations
```

Cooldown demo (runs twice back-to-back):
```bash
DEMO_MODE=cooldown npm run demo:escalations
```

DLQ demo (forces DLQ block on second run):
```bash
DEMO_MODE=dlq npm run demo:escalations
```

Cleanup only:
```bash
DEMO_CLEANUP=1 npm run demo:escalations
```

## What it does
- Ensures demo OWNER user + API key
- Adds a demo contact method (email)
- Creates EMAIL + WEBHOOK alert subscriptions for OWNER
- Upserts two Level 1 escalation policies (EMAIL + WEBHOOK targets)
- Inserts a new OPEN incident
- Resets ops counters (dev-only endpoint) if available
- Runs escalation (admin endpoint if available, else direct)
- Prints deliveries + DLQ status
- Prints `/metrics/ops` delta (or in-process counters if server is down)

## Expected output (example)
```
[ok] Incident created: <id>
[ok] Policy upserted: <id> (L1, target EMAIL)
[ok] Policy upserted: <id> (L1, target WEBHOOK)
[ok] run #1: escalated=1

Deliveries:
- EMAIL demo+alerts@veriscope.dev -> SENT (attempts=1)
- WEBHOOK http://127.0.0.1:1/demo -> FAILED (attempts=1) DLQ attempts=1/5

Metrics delta:
- deliveries_created_total: +2
- deliveries_success_total: +1
- deliveries_failure_total: +1
- escalation_runs_total: +1
```

## Reset / cleanup
Set `DEMO_CLEANUP=1` to delete demo artifacts (policies, incidents, subscriptions, contact methods, deliveries).

## Demo server mode
Start the server with auto-seeding and dev-only routes enabled:
```bash
npm run demo:server
```

Run against the live server (requires server):
```bash
npm run demo:run
```

Run all three modes (baseline + cooldown + dlq):
```bash
npm run demo:all
```
