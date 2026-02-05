# Phase 2 Definition of Done

## Daily Port Baselines

Baseline rows are computed per-port per UTC day.

- **Day definition**: UTC calendar day from 00:00:00 to 23:59:59.999.
- **Arrivals**: count of `port_calls` with `arrival_time` within the day.
- **Departures**: count of `port_calls` with `departure_time` within the day.
- **Open calls**: end-of-day snapshot count where `arrival_time` < day_end AND (`departure_time` IS NULL OR `departure_time` >= day_end).
- **Avg dwell hours**: average of `(departure_time - arrival_time)` (hours) for calls with `departure_time` within the day.
- **Rolling 30d metrics**: computed over the prior 30 days **excluding** the current day.

## Signal Engine (Phase 2)

Signals are evaluated daily from `port_daily_baselines` for the prior UTC day and persisted in `signals`.

### Signal Definitions

**PORT_ARRIVALS_ANOMALY**
- Metric: `arrivals` vs `arrivals_30d_avg` / `arrivals_30d_std`.
- Trigger: `abs(zscore) >= 2` where `zscore = (arrivals - avg) / std`.
- Severity:
  - MEDIUM: `2 <= abs(zscore) < 3`
  - HIGH: `3 <= abs(zscore) < 5`
  - CRITICAL: `abs(zscore) >= 5`

**PORT_DWELL_SPIKE**
- Metric: `avg_dwell_hours` vs `dwell_30d_avg` / `dwell_30d_std`.
- Trigger: `zscore >= 2` (spikes only).
- Severity:
  - MEDIUM: `2 <= zscore < 3`
  - HIGH: `3 <= zscore < 5`
  - CRITICAL: `zscore >= 5`

**PORT_CONGESTION_BUILDUP**
- Metric: `open_calls` vs `open_calls_30d_avg`.
- Trigger: `open_calls >= open_calls_30d_avg * 1.5` AND `open_calls_30d_avg >= 5`.
- Severity:
  - MEDIUM: `1.5x <= open_calls/open_calls_30d_avg < 2.0x`
  - HIGH: `2.0x <= open_calls/open_calls_30d_avg < 4.0x`
  - CRITICAL: `open_calls/open_calls_30d_avg >= 4.0x`

### Signal Explanation Requirements

Each signal explanation includes:
- The change (value vs baseline, including % delta or multiplier).
- Why it matters (one sentence on operational impact).
- A confidence proxy (z-score or multiplier).
- A potential impact line (schedule slippage -> freight spreads / prompt supply timing risk).
- Next checks (weather, strikes, terminal outages, river/lock constraints).

### Confidence Fields

Signals include structured confidence fields:
- `confidence_score`: 0-1 scale.
- `confidence_band`: LOW | MEDIUM | HIGH.
- `method`: zscore_30d | multiplier_30d.

### Clustering

Signals include derived clustering fields to enable event cards:
- `cluster_id`: deterministic per port/day.
- `cluster_type`: PORT_DISRUPTION.
- `cluster_summary`: brief combined metric summary (e.g. "Arrivals -40%, Dwell +100%, Open calls 4x").

### Baseline Quality Guardrails

- z-score signals require non-null, non-zero stddev.
- Minimum history required: at least 10 prior days within the 30-day lookback.
- Multiplier-based signals (open calls) still require the same minimum history and `open_calls_30d_avg >= 5`.

## Dev Signal Seeding (Deterministic Demo)

Dev environments can legitimately return zero signals if baselines are stable or lack sufficient history. To demo signals deterministically without changing production thresholds, use the dev-only seed endpoint.

**Seed a deterministic anomaly**
- `POST /api/dev/seed-anomaly?day=YYYY-MM-DD&port_id=<optional>`
- If `port_id` is omitted, Rotterdam is preferred; otherwise the first port is used.
- Inserts/updates a `port_daily_baselines` row with values that guarantee signal triggers.

**Suggested demo flow**
1. `POST /api/init`
2. `POST /api/dev/seed-anomaly?day=<yesterday>`
3. `POST /api/signals/run?day=<yesterday>`
4. `GET /v1/signals?limit=5`

The seed endpoint is guarded to be unusable in production.
