# Veriscope — Maritime & Commodity Intelligence Platform

Veriscope is an enterprise-grade, full-stack maritime and commodity intelligence platform. It delivers real-time vessel tracking, port monitoring, AI-powered market predictions, statistical anomaly signals, multi-tenant webhook alerting, and comprehensive commodity analytics across oil, gas, LNG, dry bulk, petrochemicals, and agri-biofuels.

---

## Table of Contents

- [Overview](#overview)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Architecture](#architecture)
  - [Frontend](#frontend-architecture)
  - [Backend](#backend-architecture)
  - [Database](#database-design)
  - [Signal Engine](#signal-engine)
  - [Alerting Pipeline](#alerting-pipeline)
  - [AIS & Vessel Tracking](#ais--vessel-tracking)
- [Environment Variables](#environment-variables)
- [Setup & Running Locally](#setup--running-locally)
- [Database Migrations](#database-migrations)
- [API Reference](#api-reference)
- [Authentication](#authentication)
- [Multi-Tenancy](#multi-tenancy)
- [Observability & Health](#observability--health)
- [Background Services](#background-services)
- [Testing](#testing)
- [Scripts](#scripts)

---

## Overview

Veriscope combines live AIS vessel data, port call detection via Haversine geofencing, statistical z-score signal generation, and ML-based price forecasting into a unified intelligence platform. Analysts get:

- **Live vessel map** with real-time position updates over WebSocket
- **Port signals** — automated statistical alerts for arrivals, dwell spikes, and congestion build-up
- **Commodity intelligence** — crude & products, LNG/LPG, dry bulk, petrochemicals, agri-biofuels, supply-demand balances, refinery intelligence
- **Webhook alert subscriptions** — multi-tenant, deduplicated, HMAC-signed, with dead-letter queue and retry infrastructure
- **ML price predictions** — extracted vessel/port congestion features fed into per-commodity models
- **OpenAPI documentation** at `/docs`

---

## Tech Stack

### Frontend
| Library | Role |
|---|---|
| React 18 | UI framework |
| Vite | Build tool & dev server |
| TypeScript | Type safety |
| Wouter | Client-side routing (lightweight) |
| TanStack React Query v5 | Server state / caching / background refetch |
| Tailwind CSS | Utility-first styling |
| shadcn/ui + Radix UI | Accessible headless component library |
| Leaflet | Interactive vessel map |
| Framer Motion | Animations |
| Lucide React | Icon set |
| date-fns | Date formatting |
| Recharts / Embla Carousel | Data visualisations and carousels |

### Backend
| Library | Role |
|---|---|
| Express.js | HTTP server |
| TypeScript + tsx | Type-safe Node.js execution |
| Drizzle ORM | Type-safe DB query builder |
| PostgreSQL (Neon) | Primary database |
| WebSocket (`ws`) | Real-time push to clients |
| JWT (`jsonwebtoken`) | Access + refresh token auth |
| bcryptjs | Password hashing |
| Passport.js | Auth strategy orchestration |
| Helmet | HTTP security headers |
| OpenAI SDK | ML/AI integration hook |
| Zod | Runtime schema validation |
| swagger-ui-express | Interactive API docs |
| csv-parse | CSV data ingestion |

### Shared (`/shared`)
- `schema.ts` — Single source of truth for all Drizzle table definitions and inferred TypeScript types
- `signalTypes.ts` — `SignalSeverity` and `SEVERITY_RANK` enums used on both sides
- `alertSubscriptionDto.ts` — Alert subscription DTOs
- `metrics.ts` — Shared metric label/unit constants

---

## Project Structure

```
veriscope/
├── client/                    # React frontend (Vite root)
│   └── src/
│       ├── App.tsx            # Router + providers
│       ├── components/        # Shared UI components
│       │   └── ui/            # shadcn/ui primitives (do not rename)
│       ├── hooks/             # React Query hooks (useXxx naming)
│       ├── lib/               # queryClient, utils
│       ├── pages/             # Route-level page components (PascalCase)
│       │   ├── auth/
│       │   ├── blog/
│       │   ├── commodities/
│       │   ├── energy/
│       │   └── maritime/
│       └── types/             # Frontend-only TypeScript types
│
├── server/                    # Express backend
│   ├── index.ts               # App bootstrap, middleware, error handler
│   ├── routes.ts              # Router orchestrator (imports all feature routers)
│   ├── db.ts                  # Drizzle db instance (Neon serverless)
│   ├── storage.ts             # IStorage interface + DrizzleStorage implementation
│   ├── openapi.ts             # OpenAPI 3.0 spec
│   ├── vite.ts                # Dev-mode Vite middleware integration
│   ├── config/
│   │   ├── alerting.ts        # Alert rate limits, dedupe TTL, webhook config
│   │   └── tenancy.ts         # TENANT_DEMO_ID + resolveTenantId()
│   ├── middleware/
│   │   ├── apiKeyAuth.ts      # Bearer/X-Api-Key authentication
│   │   ├── observability.ts   # Structured logger, metrics collector, health status
│   │   ├── rateLimiter.ts     # In-memory IP-based rate limiter
│   │   └── rbac.ts            # JWT auth + role/permission guards
│   ├── routers/               # Feature routers (one file per domain)
│   │   ├── health.router.ts
│   │   ├── auth.router.ts
│   │   ├── admin.router.ts
│   │   ├── dev.router.ts      # Dev-only endpoints (not mounted in production)
│   │   ├── signals.router.ts
│   │   ├── ports.router.ts
│   │   ├── alerts.router.ts
│   │   ├── vessels.router.ts
│   │   └── commodities.router.ts
│   ├── services/              # Business logic (xxxService.ts naming)
│   │   ├── aisService.ts
│   │   ├── authService.ts
│   │   ├── signalEngineService.ts
│   │   ├── alertDispatcherService.ts
│   │   ├── alertDlqQueueService.ts
│   │   ├── alertDedupeService.ts
│   │   ├── alertDeliveriesService.ts
│   │   ├── alertMetricsService.ts
│   │   ├── alertQueryService.ts
│   │   ├── alertScopeService.ts
│   │   ├── alertSubscriptionService.ts
│   │   ├── mlPredictionService.ts
│   │   ├── portCallService.ts
│   │   ├── portDailyBaselineService.ts
│   │   ├── webhookService.ts
│   │   ├── apiKeyService.ts
│   │   ├── auditService.ts
│   │   ├── cacheService.ts
│   │   ├── mockDataService.ts
│   │   └── ... (30+ services total)
│   └── utils/
│       ├── pagination.ts      # parseSafeLimit, parsePaginationParams
│       └── AppError.ts        # Structured error class
│
├── shared/                    # Isomorphic code (used by both client and server)
│   ├── schema.ts              # Drizzle table definitions + exported types
│   ├── signalTypes.ts         # SignalSeverity enum + SEVERITY_RANK
│   ├── alertSubscriptionDto.ts
│   ├── signalDto.ts
│   └── metrics.ts
│
├── drizzle/
│   ├── schema.ts              # Re-export (points to shared/schema.ts)
│   └── migrations/            # 21 sequential SQL migration files
│
├── scripts/                   # Operational scripts
│   ├── port-daily-baselines.test.ts
│   ├── signal-engine.test.ts
│   └── smoke-test.sh
│
├── docs/
│   └── PHASE1_DOD.md
│   └── PHASE2_DOD.md
│
├── drizzle.config.ts          # Drizzle Kit configuration
├── vite.config.ts             # Vite configuration (path aliases)
├── tsconfig.json              # TypeScript project config
├── tailwind.config.ts
├── postcss.config.js
└── package.json
```

---

## Architecture

### Frontend Architecture

The frontend is a **single-page application** served by the Express backend in both development (via Vite middleware) and production (static build from `dist/public/`).

#### Routing
Wouter handles client-side routing. All routes are declared in `client/src/App.tsx`. There is no file-based routing — routes are explicit imports of page components.

Key route groups:
- `/` — Landing / Home
- `/dashboard` — Main intelligence dashboard
- `/signals` — Signal feed with severity/cluster filters
- `/maritime`, `/maritime/ais`, `/maritime/port-events`, etc. — Maritime intelligence modules
- `/commodities`, `/commodities/crude-products`, etc. — Commodity modules
- `/energy`, `/energy/emissions`, etc. — Energy transition modules
- `/alerts`, `/alert-subscriptions`, `/alerts-health` — Alert management UI
- `/tankscope`, `/refinery-satellite`, `/shipscope`, `/flightscope` — Specialised analytics pages
- `/login`, `/register` — Auth pages

#### Data Fetching
TanStack React Query manages all server state. Queries are defined in custom hooks under `client/src/hooks/` (`useSignals`, `useVessels`, `usePredictions`, `usePortStats`, etc.). The global `queryClient` is configured in `client/src/lib/queryClient.ts`.

#### WebSocket
A persistent WebSocket connection to `/ws` is managed by `hooks/useWebSocket.ts`. The server pushes AIS position updates, signal notifications, and other live events to connected clients.

#### Path Aliases
Three Vite path aliases are configured:
- `@/` → `client/src/`
- `@shared/` → `shared/`
- `@assets/` → `attached_assets/`

---

### Backend Architecture

The Express application is bootstrapped in `server/index.ts` and uses a feature-router architecture:

```
Request → Helmet → cookieParser → json → observability → Router → Service → Drizzle → PostgreSQL
```

#### Route Structure
`server/routes.ts` acts as a pure orchestrator — it imports all feature routers and mounts them on the Express app:

| Router | Responsibility |
|---|---|
| `health.router.ts` | `GET /health`, `/health/alerts`, `/health/webhooks`, `/ready`, `/live`, `/metrics` |
| `auth.router.ts` | `POST /api/auth/register`, `/api/auth/login`, `/api/auth/refresh`, `/api/auth/logout` |
| `admin.router.ts` | Admin-only endpoints with WebSocket server reference |
| `dev.router.ts` | Dev-only seed endpoints (**not mounted in production**) |
| `signals.router.ts` | `GET /api/signals`, signal cluster endpoints, signal detail |
| `ports.router.ts` | `GET /v1/ports`, port detail, port calls, baselines, daily stats |
| `alerts.router.ts` | Alert subscriptions, deliveries, DLQ, metrics, manual run trigger |
| `vessels.router.ts` | `GET /v1/vessels`, vessel detail, positions, predictions |
| `commodities.router.ts` | Commodity data, market data, predictions, refinery, supply-demand |

#### Middleware Stack
1. **Helmet** — Sets security headers (CSP, HSTS, X-Frame-Options, etc.)
2. **cookieParser** — Parses cookies for session tokens
3. **observability middleware** (`requestTrackingMiddleware`) — Attaches `requestId` (UUID) and `startTime` to every request
4. **rateLimiter** — In-memory IP-based rate limiting with configurable windows and block durations
5. **apiKeyAuth** — Validates `Authorization: Bearer <key>` or `X-Api-Key: <key>` headers against HMAC-SHA256 hashed keys in the DB (peppered)
6. **rbac** — Decodes JWT and attaches `req.user` with role/permission guards (`requirePermission`, `requireRole`)

#### Error Handling
A global error handler in `server/index.ts` distinguishes between `AppError` (structured 4xx) and unexpected 5xx errors. Stack traces and internal messages are **never** sent to the client in production — they are logged internally only.

---

### Database Design

All table definitions live in `shared/schema.ts` and are imported by both the server (Drizzle queries) and the frontend (TypeScript type inference via `$inferSelect`/`$inferInsert`).

#### Core Entity Tables
| Table | Description |
|---|---|
| `commodities` | Oil, gas, LNG, dry bulk, chemicals with category, unit, and quality specifications |
| `markets` | Physical, power, financial, and shipping market definitions |
| `ports` | Global port/terminal registry with UN/LOCODE, coordinates, geofence radius, and type |
| `vessels` | Vessel registry: MMSI, IMO, type (VLCC/Suezmax/Aframax/LNG carrier), deadweight |
| `vessel_positions` | AIS position time-series: lat/lon, SOG, COG, nav status, source, timestamp |
| `port_calls` | Detected port call records: arrival, departure, dwell hours, computed by geofencing |
| `port_daily_baselines` | Daily aggregated baseline metrics per port: arrivals, departures, dwell, open calls |

#### Signal & Alerting Tables
| Table | Description |
|---|---|
| `signals` | Anomaly signals with type, severity, z-score, delta%, cluster metadata |
| `alert_subscriptions` | Per-user webhook/email subscriptions with scope, severity_min, secret |
| `alert_runs` | Audit log of each alert dispatch run |
| `alert_deliveries` | Individual delivery records per subscription per cluster per day |
| `alert_delivery_attempts` | Per-attempt records for retry tracking |
| `alert_dlq` | Dead-letter queue for failed deliveries awaiting retry |
| `alert_dedupe` | Deduplication store: prevents repeat alerts within TTL window |

#### Intelligence & Analytics Tables
| Table | Description |
|---|---|
| `storage_facilities` | Storage terminal capacity and utilisation |
| `storage_fill_data` | SAR-derived fill index time-series |
| `ml_price_predictions` | ML-generated price predictions with features as JSONB |
| `predictions` | General predictions table (commodity/market scoped) |
| `port_stats` | Aggregated port statistics (arrivals, departures, throughput) |
| `port_delay_events` | Individual vessel delay events at ports |
| `refineries` | Refinery/plant registry with capacity and maintenance status |
| `crude_grades` | Crude grade specifications |
| `lng_cargoes` | LNG/LPG cargo tracking |
| `dry_bulk_fixtures` | Dry bulk fixture records |
| `supply_demand_balances` | Regional supply/demand balance sheets |
| `research_reports` | Research and insight documents |
| `cargo_legs` | Multi-leg cargo chain records |
| `sts_events` | Ship-to-ship transfer events |

#### User & Auth Tables
| Table | Description |
|---|---|
| `users` | User accounts with bcrypt-hashed passwords and roles |
| `organizations` | Organisation entities for multi-tenant grouping |
| `api_keys` | API keys (pepper+SHA-256 hashed, never stored in plaintext) |
| `audit_logs` | Immutable audit trail: action, resource, IP, user agent |
| `watchlists` | User-defined entity watchlists |
| `alert_rules` | User-configured alert rule definitions |

#### Migration History
21 sequential migrations in `drizzle/migrations/` managed by Drizzle Kit. Key milestones:
- `0000` — Phase 1 baseline schema (ports, vessels, positions, commodities, predictions)
- `0001`–`0006` — Port daily baselines and signal tables with clustering support
- `0007`–`0011` — Alert subscription, delivery, and DLQ infrastructure
- `0012`–`0020` — Test fields, tenant isolation, delivery attempts, API keys, timestamptz fixes

---

### Signal Engine

The signal engine (`server/services/signalEngineService.ts`) is the statistical anomaly detection layer. It operates on pre-computed daily baselines.

#### How It Works

1. **Baselines** — `portDailyBaselineService.ts` runs SQL to aggregate `port_calls` per port per day (30-day rolling window) into `port_daily_baselines`.

2. **Signal Evaluation** — `evaluatePortSignalsForDay(day)` computes z-scores for three metrics per port:
   - **Arrivals** (`PORT_ARRIVALS_ANOMALY`) — z ≥ 2 triggers signal
   - **Dwell hours** (`PORT_DWELL_SPIKE`) — z ≥ 2 triggers signal
   - **Open calls** (`PORT_CONGESTION_BUILDUP`) — 1.5× multiplier vs rolling average

3. **Severity Classification**
   | Z-score | Severity |
   |---|---|
   | 2–3 | LOW |
   | 3–4 | MEDIUM |
   | 4–5 | HIGH |
   | ≥ 5 | CRITICAL |

4. **Confidence Scoring** — Adjusted by data completeness (penalised when `< 90%` of 30-day history is available), producing a `confidenceBand` of LOW / MEDIUM / HIGH.

5. **Clustering** — Signals sharing the same port and day are grouped into a cluster (`cluster_id`, `cluster_key`, `clusterSeverity`) for deduplicated alerting.

6. **Upsert** — Results are written with `ON CONFLICT DO UPDATE` so re-runs are idempotent.

---

### Alerting Pipeline

The alerting system is a fully async, multi-tenant, deduplicated webhook and email delivery pipeline.

#### Flow

```
POST /api/alerts/run
        │
        ▼
runAlerts()  ────────────────► getAlertCandidates() ──► signals table (filtered by day/entity/severity)
        │
        ├── Per subscription + candidate:
        │       ├── shouldSendAlert()   ──► alert_dedupe (TTL-based)
        │       ├── Rate limit check    ──► perEndpointCount map
        │       ├── sendWebhook()       ──► HTTPS endpoint (retry up to 3×)
        │       │       HMAC-SHA256 signed: X-Veriscope-Signature
        │       │       Idempotency key: X-Veriscope-Idempotency-Key
        │       ├── markAlertSent()     ──► alert_dedupe upsert
        │       └── Write to alert_deliveries + alert_delivery_attempts
        │
        ├── On failure: write to alert_dlq
        │
        └── Return summary JSON
```

#### Dead-Letter Queue (DLQ)
Failed deliveries land in `alert_dlq`. `POST /api/alerts/retry-dlq` runs `alertDlqQueueService`, which retries due DLQ rows, honouring exponential back-off via `computeNextAttempt`.

#### Deduplication
`alert_dedupe` stores `(tenantId, clusterId, channel, endpoint)` with `lastSentAt` and `ttlHours`. A delivery is skipped if `now < lastSentAt + ttlHours`. Default TTL is 24 hours (configurable via `ALERT_DEDUPE_TTL_HOURS`).

#### Webhook Security
Each webhook request includes:
- `X-Veriscope-Signature: sha256=<HMAC>` — HMAC-SHA256 of the JSON body using the subscription secret
- `X-Veriscope-Timestamp` — ISO timestamp for replay attack prevention
- `X-Veriscope-Idempotency-Key` — Deterministic key from `sha1(subscriptionId|clusterId|day)`

#### Configuration (env vars)
| Variable | Default | Description |
|---|---|---|
| `ALERT_RATE_LIMIT_PER_ENDPOINT` | `50` | Max deliveries per endpoint per run |
| `ALERT_DEDUPE_TTL_HOURS` | `24` | Hours before the same cluster can re-alert |
| `WEBHOOK_TIMEOUT_MS` | `5000` | HTTP timeout per webhook attempt |
| `WEBHOOK_RETRY_ATTEMPTS` | `3` | Max retry attempts per webhook call |
| `DLQ_MAX_ATTEMPTS` | `10` | Max total DLQ retry attempts before abandoning |

---

### AIS & Vessel Tracking

`server/services/aisService.ts` operates in two modes:

- **Simulation mode** (default) — Runs a 30-second `setInterval` loop, generating synthetic position updates for all known vessels using random walks from their last known position.
- **Live mode** — If `AISSTREAM_API_KEY` is set, connects to AISStream.io over WebSocket for real-time global AIS data ingestion.

Either way, messages are deduplicated via a `Set<hash>` (capped at 10,000) and queued for batch processing at 100ms intervals. Positions are persisted to `vessel_positions`.

`server/services/portCallService.ts` runs every 60 seconds, scanning latest positions against all port geofences (Haversine distance ≤ geofence radius km). When a vessel enters a port, an `arrival_time` is recorded in `port_calls`; when it exits, `departure_time` and computed `dwell_hours` are added.

Three ports are pre-seeded with geofences:

| Port | UN/LOCODE | Geofence |
|---|---|---|
| Fujairah | AEFJR | 10 km |
| Rotterdam | NLRTM | 15 km |
| Singapore | SGSIN | 12 km |

---

## Environment Variables

Create a `.env` file in the project root. All variables must be set for the server to start — the application **refuses to start** if any required variable is missing.

```env
# ── Required ──────────────────────────────────────────────────────────────────

# PostgreSQL connection string (Neon or standard Postgres)
DATABASE_URL=postgresql://user:password@host/dbname?sslmode=require

# JWT signing secret — must be a strong random string (≥ 32 chars)
JWT_SECRET=your-strong-random-secret-here

# API key HMAC pepper — must be a strong random string (≥ 32 chars)
API_KEY_PEPPER=your-strong-pepper-here

# ── Optional ──────────────────────────────────────────────────────────────────

# AISStream.io API key — enables live mode; simulation used when absent
AISSTREAM_API_KEY=

# Static API key for the alerting endpoints (alternative to DB-stored keys)
ALERTS_API_KEY=
ALERTS_USER_ID=            # Required when ALERTS_API_KEY is set
ALERTS_TENANT_ID=          # Defaults to TENANT_DEMO_ID if absent

# Alerting tuning
ALERT_RATE_LIMIT_PER_ENDPOINT=50
ALERT_DEDUPE_TTL_HOURS=24
WEBHOOK_TIMEOUT_MS=5000
WEBHOOK_RETRY_ATTEMPTS=3
DLQ_MAX_ATTEMPTS=10

# Node environment
NODE_ENV=development       # Set to 'production' to disable dev endpoints
```

---

## Setup & Running Locally

### Prerequisites
- Node.js 20+
- npm 10+
- A PostgreSQL database (Neon serverless or any standard PostgreSQL instance)

### Installation

```bash
# 1. Clone the repository
git clone <repo-url>
cd veriscope

# 2. Install dependencies
npm install

# 3. Configure environment
cp .env.example .env
# Then edit .env with your DATABASE_URL, JWT_SECRET, and API_KEY_PEPPER
```

### Development

```bash
npm run dev
```

This starts **a single process** that:
1. Runs the Express server on port 5000 (default)
2. Mounts Vite's development middleware for hot module replacement
3. Serves the React app at `/`
4. Exposes the API at `/api/*` and `/v1/*`
5. Auto-seeds a demo API key (dev-only) on startup

Open `http://localhost:5000`.

### Production Build

```bash
# Build both frontend and backend
npm run build

# Start the production server
npm start
```

The build step:
1. Runs `vite build` — outputs the React bundle to `dist/public/`
2. Runs `esbuild` — bundles `server/index.ts` to `dist/index.js` (ESM, external packages)

In production, Express serves the static React bundle and handles all `/api` routes.

### Type Check

```bash
npm run check
```

Runs `tsc --noEmit` across the full monorepo. Should produce zero errors.

---

## Database Migrations

Drizzle Kit manages the database schema. The source of truth is `shared/schema.ts`.

```bash
# Apply all pending migrations to the database
npm run db:push
```

This runs `drizzle-kit push`, which introspects `shared/schema.ts` and applies any schema differences directly to the database.

To generate SQL migration files instead:

```bash
npx drizzle-kit generate
```

Migration files are stored in `drizzle/migrations/`.

### Initial Data Seed

On first startup in development, `MockDataService` automatically seeds:
- 3 ports (Fujairah, Rotterdam, Singapore)
- Sample vessels (VLCC, Suezmax, Aframax types)
- Storage sites and initial fill data
- Port statistics
- Admin user: `admin@example.com` / `admin123`

The signal engine baselines can be backfilled by calling:

```
POST /api/admin/backfill-baselines?days=60
```

---

## API Reference

Interactive Swagger/OpenAPI documentation is available at `/docs` when the server is running.

The raw OpenAPI spec is available at `GET /openapi.json`.

### Key Endpoints

#### Health & Observability
```
GET  /health              — Overall health status (db, AIS, services)
GET  /ready               — Kubernetes readiness probe
GET  /live                — Kubernetes liveness probe
GET  /metrics             — Prometheus-style metrics (AIS messages, errors, latency)
GET  /health/alerts       — Alert subsystem health check
GET  /health/webhooks     — Webhook connectivity check
```

#### Authentication (`/api/auth`)
```
POST /api/auth/register   — Register new user + organisation
POST /api/auth/login      — Login → returns access + refresh tokens
POST /api/auth/refresh    — Exchange refresh token for new access token
POST /api/auth/logout     — Invalidate session
```

#### Ports (`/v1/ports`)
```
GET  /v1/ports            — List all ports
GET  /v1/ports/:id        — Port detail with 7-day metrics
GET  /v1/ports/:id/calls  — Port call history with pagination
GET  /v1/ports/:id/baselines  — Daily baseline metrics
```

#### Vessels (`/v1/vessels`)
```
GET  /v1/vessels              — List all vessels
GET  /v1/vessels/:id          — Vessel detail
GET  /v1/vessels/:id/positions — Historical position track
GET  /v1/vessels/:id/predictions — ML price predictions for vessel
```

#### Signals (`/api/signals`)
```
GET  /api/signals             — Paginated signal feed (filter by port, severity, date range, cluster)
GET  /api/signals/:id         — Signal detail
GET  /api/signals/clusters    — Clustered signal view
```

#### Alerts (`/api/alerts`, authenticated via API key)
```
POST /api/alerts/run                    — Trigger an alert dispatch run
GET  /api/alerts/subscriptions          — List alert subscriptions
POST /api/alerts/subscriptions          — Create subscription
PATCH /api/alerts/subscriptions/:id     — Update subscription
DELETE /api/alerts/subscriptions/:id    — Delete subscription
POST /api/alerts/subscriptions/:id/test — Send test alert
GET  /api/alerts/deliveries             — List delivery records
GET  /api/alerts/metrics                — Delivery metrics by day/endpoint
GET  /api/alerts/dlq-health             — DLQ depth and overdue count
POST /api/alerts/retry-dlq              — Trigger DLQ retry batch
```

---

## Authentication

The platform supports two authentication mechanisms:

### 1. JWT (Session Auth — UI users)
- `POST /api/auth/login` returns a short-lived **access token** (15 min) and long-lived **refresh token** (7 days)
- Access token is sent as `Authorization: Bearer <token>` on API requests
- RBAC is enforced via `requirePermission()` / `requireRole()` middleware guards

**Roles and permissions:**

| Role | Key permissions |
|---|---|
| `admin` | All permissions including `admin:system`, `write:users`, `read:audit` |
| `analyst` | Read/write signals, predictions, models; read vessels/ports/storage |
| `operator` | Read/write vessels, ports, storage; read signals, predictions |
| `viewer` | Read-only access to all resources |

### 2. API Keys (Machine-to-machine — webhooks and integrations)
- Generated with `generateApiKey()` — format `vs_demo_<base64url token>`
- Stored as `sha256(pepper + rawKey)` — plaintext is **never** stored
- Passed via `Authorization: Bearer <key>` or `X-Api-Key: <key>` header
- Keys are scoped to a `tenantId` and `userId`

A demo API key is automatically seeded on dev startup via `seedDemoApiKeyIfDev()`.

---

## Multi-Tenancy

Veriscope supports multi-tenant data isolation via a `tenantId` column on all alert-related tables (`alert_subscriptions`, `alert_deliveries`, `alert_runs`, `alert_dlq`, `alert_dedupe`, `alert_delivery_attempts`, `api_keys`).

- `TENANT_DEMO_ID = "00000000-0000-0000-0000-000000000001"` is the default demo tenant
- All alert queries filter by `tenantId` — tenant data is never cross-readable
- `resolveTenantId()` reads `ALERTS_TENANT_ID` from the environment, falling back to `TENANT_DEMO_ID`
- Indexed on all tenant-scoped tables for query performance

---

## Observability & Health

### Structured Logging
`server/middleware/observability.ts` exports a `logger` singleton that emits JSON-structured logs:

```json
{
  "timestamp": "2026-03-18T10:00:00.000Z",
  "level": "info",
  "requestId": "550e8400-e29b-41d4-a716-446655440000",
  "message": "Port call detected",
  "metadata": { "vesselId": "...", "portId": "..." }
}
```

Every request gets a unique `requestId` (UUID v4) attached via `requestTrackingMiddleware`.

### Metrics
`GET /metrics` exposes internal counters:
- `ais_messages_total` — AIS messages processed
- `api_requests_total` — HTTP requests served
- `errors_total` — Unhandled errors caught
- `avg_latency_ms` — Rolling average API latency
- `active_connections` — Current WebSocket connections

### Health Endpoints
```
GET /health         — { status: 'healthy'|'degraded'|'unhealthy', db, ais, services }
GET /ready          — 200 when ready to accept traffic
GET /live           — 200 as long as process is alive
GET /health/alerts  — Tests DB write/read on alert tables
GET /health/webhooks — Tests fetch availability + timeout config
```

### Audit Logging
`auditService.ts` writes to the `audit_logs` table on every auth event (login, logout, failed login) and security-sensitive operations. Records include `userId`, `action`, `resource`, `ipAddress`, `userAgent`, and `status`.

---

## Background Services

These services are started automatically when the server boots:

| Service | Interval | Description |
|---|---|---|
| `aisService` | 30s (sim) / live stream | Generates/ingests vessel positions, broadcasts over WebSocket |
| `portCallService` | 60s | Haversine geofencing to detect port arrivals and departures |
| `portDailyBaselineService` | On demand (admin trigger or cron) | Aggregates port call data into daily baseline rows |
| `signalEngineService` | On demand | Runs z-score anomaly detection on baselines, upserts signals |
| `alertDispatcherService` | On demand (`POST /api/alerts/run`) | Dispatches alerts for matching signals to subscribers |
| `alertDlqQueueService` | On demand (`POST /api/alerts/retry-dlq`) | Retries failed DLQ deliveries with exponential back-off |
| `cacheService` | 60s cleanup | Evicts expired in-memory cache entries |
| `rateLimiter` | 60s cleanup | Evicts expired rate limit entries |

---

## Testing

### Type Checking
```bash
npm run check    # npx tsc --noEmit — must produce 0 errors
```

### Integration Scripts
```bash
# Test port daily baseline generation logic
npm run test:baselines

# Test signal engine evaluation logic
npm run test:signals
```

### Smoke Test
```bash
bash scripts/smoke-test.sh
```
Runs basic HTTP checks against a running server.

---

## Scripts

| Script | Purpose |
|---|---|
| `scripts/port-daily-baselines.test.ts` | Validates baseline aggregation logic |
| `scripts/signal-engine.test.ts` | Validates signal z-score generation |
| `scripts/smoke-test.sh` | HTTP smoke tests for key endpoints |
| `scripts/tmp-reset-seed.ts` | Dev utility to reset and re-seed the database |
| `scripts/tmp-verify-signals.ts` | Dev utility to inspect generated signals |
| `scripts/tmp-ports-ambig.ts` | Dev utility to detect ambiguous port geofence overlaps |

---

## Alerting Runbook

See [RUNBOOK_ALERTING.md](RUNBOOK_ALERTING.md) for operational procedures including:
- Diagnosing failed deliveries
- Retrying DLQ safely
- Rotating webhook secrets
- Interpreting dedupe TTL and rate limit config
- Health check endpoints
