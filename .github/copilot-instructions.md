# Veriscope — Copilot Instructions

## Agent Maintenance Directive

Keep this file current. After every conversation: append mistakes to the log, update patterns if changed, correct stale entries. Do this proactively.

---

## Project Identity

Enterprise maritime & commodity intelligence platform. Full-stack TypeScript monorepo: React 18 (Vite) + Express, shared code under `/shared`. PostgreSQL via Neon serverless, Drizzle ORM.

---

## Repository Layout

```
client/src/          — React SPA (Vite)
  components/        — PascalCase .tsx (ui/ is kebab-case, shadcn-owned)
  hooks/             — useXxx.ts camelCase
  pages/             — PascalCase .tsx (registered in App.tsx — no file routing)
  lib/               — queryClient.ts, utils
server/
  index.ts           — bootstrap, Helmet, middleware chain, global error handler
  routes.ts          — orchestrator only (mounts routers, nothing else)
  db.ts              — Drizzle singleton
  config/            — alerting.ts, tenancy.ts
  middleware/        — apiKeyAuth.ts, observability.ts, rateLimiter.ts, rbac.ts
  routers/           — *.router.ts (one per domain)
  services/          — *Service.ts (one concern per file)
  utils/             — pagination.ts, AppError
shared/
  schema.ts          — single source of truth for ALL Drizzle tables + inferred types
  signalTypes.ts     — SignalSeverity, SEVERITY_RANK, ConfidenceBand
drizzle/migrations/  — sequential SQL files, never edit after applying
```

---

## Naming Conventions

| Location | Convention | Example |
|---|---|---|
| `client/src/pages/` | PascalCase `.tsx` | `Dashboard.tsx` |
| `client/src/components/` | PascalCase `.tsx` | `MapPanel.tsx` |
| `client/src/components/ui/` | kebab-case (shadcn) | `button.tsx` |
| `client/src/hooks/` | `useXxx.ts` | `useSignals.ts` |
| `server/routers/` | `*.router.ts` | `signals.router.ts` |
| `server/services/` | `*Service.ts` | `signalEngineService.ts` |
| Drizzle tables | `snake_case` plural | `vessel_positions` |
| Drizzle inferred types | PascalCase | `VesselPosition` |

---

## Architecture Rules

- **`routes.ts` is an orchestrator only** — never add inline handlers or middleware there
- **Dev endpoints** go in `dev.router.ts`, mounted only when `NODE_ENV !== 'production'`
- **`shared/schema.ts` is the single source of truth** — never define tables elsewhere
- **Pagination** — always use `parseSafeLimit(req.query.limit, default, max)` from `server/utils/pagination.ts`; never raw `parseInt`
- **Error handling** — throw `AppError` for 4xx; never send raw `error.message` in 500s; global handler in `server/index.ts` sanitises responses
- **Logging** — always use `logger` from `server/middleware/observability.ts`; signature is `logger.error(message, { metadata })` — **message first**; never `console.log`

---

## Security (OWASP Top 10)

- `JWT_SECRET` and `API_KEY_PEPPER` must be present — server refuses to start if absent
- API keys stored as `sha256(pepper + rawKey)` — plaintext never persisted
- JWT access tokens: 15 min; refresh: 7 days; passwords: bcryptjs 10 rounds
- All `req.query` pagination through `parseSafeLimit`; all write bodies through Zod
- Never raw string interpolation in SQL — Drizzle query builder or `sql\`...\`` tagged templates only
- Every query against tenant-scoped tables must filter by `tenantId`; use `resolveTenantId()` from `server/config/tenancy.ts`
- Outgoing webhooks include `X-Veriscope-Signature: sha256=<HMAC-SHA256>`
- Helmet stays at the top of the middleware chain — never move or remove it

---

## Environment Variables

| Variable | Req | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | Neon/Postgres |
| `JWT_SECRET` | ✅ | — | Startup guard |
| `API_KEY_PEPPER` | ✅ | — | Startup guard |
| `NODE_ENV` | — | `development` | Gates dev routes |
| `AISSTREAM_API_KEY` | — | — | Absent → simulation |
| `ALERTS_API_KEY` | — | — | Env shortcut |
| `ALERTS_USER_ID` | — | — | Required with `ALERTS_API_KEY` |
| `ALERTS_TENANT_ID` | — | `TENANT_DEMO_ID` | |
| `ALERT_RATE_LIMIT_PER_ENDPOINT` | — | `50` | |
| `ALERT_DEDUPE_TTL_HOURS` | — | `24` | |
| `WEBHOOK_TIMEOUT_MS` | — | `5000` | |
| `WEBHOOK_RETRY_ATTEMPTS` | — | `3` | |
| `DLQ_MAX_ATTEMPTS` | — | `10` | |
| `OPENAI_API_KEY` | — | — | ML/AI |

**Key constants:** `TENANT_DEMO_ID = "00000000-0000-0000-0000-000000000001"` · Signal z-scores: `[2,3)=LOW [3,4)=MEDIUM [4,5)=HIGH >=5=CRITICAL` · Port radii: Rotterdam 15 km, Singapore 12 km, Fujairah 10 km

---

## RBAC

| Role | Key permissions |
|---|---|
| `admin` | All + `admin:system`, `write:users`, `read:audit` |
| `analyst` | read+write signals, predictions, models, watchlists, alerts |
| `operator` | read+write vessels, ports, storage, watchlists, alerts |
| `viewer` | Read-only |

Use `requirePermission('...')` / `requireRole('...')` from `server/middleware/rbac.ts`. Never inline permission checks.

---

## Database Conventions

- Schema-first: edit `shared/schema.ts` then `npx drizzle-kit generate` + `npm run db:push`
- Use `createInsertSchema`/`createSelectSchema` from `drizzle-zod` — don't duplicate Zod schemas
- All timestamps: `timestamptz` — never plain `timestamp`
- Idempotent writes use `ON CONFLICT DO UPDATE`
- Tenant-scoped tables need `tenantId` column + composite index `(tenantId, createdAt DESC)`

---

## Frontend Conventions

- All routes declared in `client/src/App.tsx` (Wouter) — no file routing
- All server state through TanStack Query hooks in `hooks/`; never `fetch()` directly in components
- Auth state: use `useCurrentUser()` (calls `GET /api/auth/me`); never `getAuthToken()` — always null (httpOnly cookies)
- Single WebSocket via `hooks/useWebSocket.ts` — don't open more
- Compose from `components/ui/` shadcn primitives; never edit generated primitives
- Dark mode via `dark:` Tailwind variants — never inline style for colour

---

## TypeScript Rules

- `strict: true` — `npx tsc --noEmit` must exit 0 before any commit
- No `as any`; no `!` on genuinely nullable values
- Use Drizzle-inferred types from `shared/schema.ts` — don't redeclare equivalents
- Neon driver returns timestamps as strings — cast explicitly
- Always narrow `req.user` before use; annotate async service return types
- Map/Set iteration: use `Array.from(map.entries())` not `[...map]` (TS target compatibility)

---

## Dev Workflow

```bash
npm run dev       # full-stack (port 5000, Vite HMR)
npm run check     # tsc --noEmit
npm run build     # production build
npm run db:push   # apply schema
bash scripts/smoke-test.sh
```

Default dev credentials: `admin@example.com` / `admin123`

---

## Files Never to Edit Directly

| File | Reason |
|---|---|
| `client/src/components/ui/*.tsx` | shadcn — update via CLI only |
| `drizzle/migrations/*.sql` | immutable migration history |
| `shared/schema.ts` | never remove/rename columns without a migration |

---

## Known Past Mistakes Log

| # | File(s) | Mistake → Fix |
|---|---|---|
| 1 | `server/routes.ts` | 700-line monolith → 51-line orchestrator + routers |
| 2 | `server/services/` | kebab-case filenames → camelCase + `Service` suffix |
| 3 | `client/src/hooks/` | kebab-case filenames → `useXxx.ts` |
| 4 | `client/src/pages/` | kebab-case filenames → PascalCase |
| 5 | Multiple routers | `parseInt(req.query.limit)` → `parseSafeLimit()` |
| 6 | `server/routes.ts` | dev endpoints unconditional → moved to `dev.router.ts` guarded by env |
| 7 | `server/graphql.ts` | dead file → deleted |
| 8 | 10 files | 52 TypeScript errors → all fixed |
| 9 | Migration 0020 | `timestamp` columns → `timestamptz` |
| 10 | All services/routers | `console.log` → `logger.info/error/warn` |
| 11 | Security audit | 20 OWASP issues → all resolved |
| 12 | `server/index.ts` | `dotenv` never imported → `import "dotenv/config"` as first line |
| 13 | `server/index.ts` | `reusePort: true` crashed macOS Node 25 → guarded to `platform === 'linux'` |
| 14 | Root | no `.env.example` → created |
| 15 | `server/routes.ts` | `initializeBaseData()` never called → added idempotent call on startup (non-prod) |
| 16 | `client/` | `getAuthToken()` always null → `GET /api/auth/me` + `useCurrentUser` hook |
| 17 | `client/src/components/` | dead files (`OnboardingModal`, `VesselMarker`, `Flightscope`, `Shipscope`) → deleted |
| 18 | All routers/services | 194+ raw `console.*` → structured `logger` calls |
| 19 | `server/middleware/rateLimiter.ts` | `publicDataRateLimiter` not exported → added |
| 20 | Multiple services | wrong import paths in `alertDispatcher`, `alertDlqQueue`, `alertQuery` → fixed to correct service filenames |
| 21 | `alertDispatcherService.ts` + `alerts.router.ts` | `db.insert(alertDeliveries)` missing `destinationKey` → added `makeDestinationKey(channel, endpoint)` |
| 22 | `alertRoutingHealthService.ts` + `incidentEscalationService.ts` | `[...map]` spread → `Array.from(map.entries())` |
| 23 | `server/services/emailService.ts` | `renderAlertBundleEmail` missing → added |
| 24 | `client/src/pages/` + subdirs | lowercase filenames (`alerts.tsx`, `login.tsx`, etc.) caused TS1261 → renamed to PascalCase |
| 25 | `server/services/modelRegistryService.ts` | 12 `console.error` → `logger.error` with `{ error }` metadata |
| 26 | `server/routers/admin.router.ts` | raw `parseInt(days)` → `parseSafeLimit`; GitHub routes had no auth → added `authenticate, requireAdmin` |
| 27 | `server/routers/commodities.router.ts` | raw `parseInt(weeks)` and `parseInt(limit)` → `parseSafeLimit` |
| 28 | `server/routers/health.router.ts` | `/metrics` had no auth → added `authenticateApiKey` middleware |
| 29 | `server/routers/escalations.router.ts` | PATCH body unvalidated → added `patchPolicySchema` Zod guard before `validateRoutingPolicyDraft` |
| 30 | `server/routers/alerts.router.ts` | Added `authenticate`/`requirePermission` to watchlist & alert-rule mutations but forgot to add them to the import from `../middleware/rbac` → always update imports when adding middleware |
| 31 | `server/routers/alerts.router.ts` | `GET /api/watchlists` and `GET /api/alert-rules` used `optionalAuth` with `(req as any).user?.id \|\| "demo-user"` fallback → changed to `authenticate` + `req.user!.userId`; also added ownership checks to all mutation endpoints (PATCH, DELETE, snooze, unsnooze, mute) |
| 32 | `shared/schema.ts` | `alertDeliveries.userId` was nullable → made `.notNull()`; `watchlists` and `alertRules` had no `tenantId` → added with safe default; migration 0052 created |
| 33 | `server/routers/*.router.ts` (flows, team, audit, alert-ops) | raw `parseInt()` — no parseSafeLimit import at all → added import + converted all 14 remaining occurrences to `parseSafeLimit()` |
| 34 | `server/routers/*.router.ts`, `client/src/**` | `error.message` leaked in 500 responses (25 occurrences) → replaced with static messages + `logger.error(message, { error })` |
| 35 | `client/src/hooks/*.ts`, `client/src/pages/*.tsx` | `getAuthToken()` always null (dead code) → replaced with `useCurrentUser()` in 5 files |
| 36 | `shared/schema.ts` | 113 plain `timestamp()` columns without `{ withTimezone: true }` → converted all to `timestamp(col, { withTimezone: true })`; migration 0053 created |
| 37 | 6 write endpoints (vessels, ports, commodities, comms) | No Zod validation on POST bodies (mass assignment) → added `createInsertSchema` parse + 400 on failure |
| 38 | `server/routers/alerts.router.ts` | IDOR on `PATCH /communications/:id/read` — no ownership check; `userId` forceable from body → forced `userId = req.user!.userId`, added ownership fetch+403 |
| 39 | `server/routers/commodities.router.ts` | `POST /refinery/refresh` used `optionalAuth` (unauthenticated access) → changed to `authenticate` |
| 40 | `server/routers/auth.router.ts` | `/v1/auth/refresh` returned `refresh_token` in JSON body → removed; now only sets httpOnly cookie; both register endpoints lacked password complexity → added uppercase+digit+special regex rules |
| 41 | `shared/schema.ts` + `flows.router.ts` | `tradeFlows` table had no `tenantId` → added column + index + filter in all 3 flows endpoints; migration 0054 created |
| 42 | `shared/schema.ts` | `alertSubscriptions.secret` was nullable → made `.notNull()`; all insert sites fixed to always generate a secret; migration 0055 created |
| 43 | `server/services/authService.ts` | No per-account login lockout; refresh tokens reusable (no JTI rotation) → added `failedLoginAttempts` + `revokedRefreshTokens` tables; `login()` checks 5 failures/15min window; `refreshTokens()` records and rotates JTI; migrations 0056+0057 created |
| 44 | `server/services/alert*Service.ts` + `alerts.router.ts` | IDN homograph SSRF bypass — `xn--` hostnames bypassed ASCII allowlist regex → added explicit `/xn--/i` rejection in all 5 webhook validation paths |
| 45 | `server/index.ts` | `app.set('trust proxy', 1)` accepted any proxy (IP spoofing) → scoped to `TRUSTED_PROXIES` env var or `false` |
| 46 | `client/src/pages/Team.tsx` | Invite tokens stored in `localStorage` (XSS-accessible) → replaced with React `useState` (`cachedInviteTokens`); removed `inviteStoragePrefix` useMemo |
| 47 | Multiple `client/src/` files | Stray `console.log` in hooks, pages, App.tsx (info leakage) → removed all |
| 48 | `server/routers/alerts.router.ts` + `alert-ops.router.ts` + `dev.router.ts` + `demoServerSeed.ts` | EMAIL channel subscriptions passed `secret: null` after `.notNull()` change → fixed to always call `generateSecret()` / `randomBytes(24)` |
