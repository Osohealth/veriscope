# Alerting Runbook

## What happens when alerts don’t send
1) Check recent runs: `POST /api/alerts/run?day=YYYY-MM-DD` response summary.
2) Inspect delivery log: `GET /v1/alert-deliveries?day=YYYY-MM-DD&status=FAILED`.
3) Check DLQ depth: `GET /api/alerts/dlq-health`.

## How to inspect failures
- Look at `alert_deliveries.error`, `last_http_status`, and `latency_ms`.
- Use `/api/alerts/metrics` for failure counts by endpoint/day.
- If `status=FAILED` and DLQ has rows, the retry path is available.

## How to retry DLQ safely
1) Confirm DLQ has due rows: `/api/alerts/dlq-health`.
2) Trigger retries: `POST /api/alerts/retry-dlq?limit=50`.
3) Verify delivery status flips to `SENT` and DLQ rows are removed.

## When to increase rate limits
- If `SKIPPED_RATE_LIMIT` appears in delivery logs and volume is expected.
- Increase `ALERT_RATE_LIMIT_PER_ENDPOINT` gradually and monitor metrics.

## Dedupe TTL & rate limits (operational meaning)
- `ALERT_DEDUPE_TTL_HOURS`: suppresses repeat alerts for same cluster+endpoint within the TTL window.
- `ALERT_RATE_LIMIT_PER_ENDPOINT`: caps per-run sends to protect endpoints.

## How to rotate webhook secrets
1) Update subscription secret in `alert_subscriptions.secret`.
2) Verify receiver accepts new signatures using:
   - `X-Veriscope-Timestamp`
   - `X-Veriscope-Signature`
3) Monitor for failures and rollback if needed.

## Health checks
- Alerts: `GET /health/alerts`
- Webhooks: `GET /health/webhooks`
