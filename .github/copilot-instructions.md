# Veriscope — Copilot Instructions

This file provides GitHub Copilot with authoritative context about the Veriscope codebase.

## Agent Maintenance Directive

**The agent is responsible for keeping this file up to date.** At the end of every conversation:

1. **Mistakes** — If any bug, wrong pattern, or incorrect assumption was discovered and corrected during the conversation, append a new row to the bottom of the Known Past Mistakes Log.
2. **New patterns** — If a new convention, architectural decision, or recurring pattern was established, add or update the appropriate section above.
3. **Corrections** — If an existing entry in this file turns out to be wrong or outdated, update it in place.
4. **New env vars / constants** — If a new environment variable or key constant was introduced, add it to the relevant table.

Do this proactively — do not wait for the user to ask. The goal is that this file always reflects the current state of the codebase so future agent sessions start with accurate context.

---

---

## Project Identity

**Veriscope** is an enterprise maritime and commodity intelligence platform.
Full-stack TypeScript monorepo: React 18 (Vite) frontend + Express backend, sharing code under `/shared`.
Database: PostgreSQL via Neon serverless, managed with Drizzle ORM.

---

## Repository Layout (canonical)

```
client/src/        — React SPA (Vite)
  components/      — Shared UI (PascalCase filenames)
    ui/            — shadcn/ui primitives — do NOT rename files here (shadcn owns naming)
  hooks/           — Custom React hooks (useXxx.ts camelCase filenames)
  pages/           — Route-level components (PascalCase filenames)
  lib/             — queryClient.ts, utils
  types/           — Frontend-only TS types

server/            — Express API
  index.ts         — App bootstrap, Helmet, middleware chain, global error handler
  routes.ts        — 51-line orchestrator — only mounts routers, nothing else
  db.ts            — Drizzle db singleton (Neon serverless)
  storage.ts       — IStorage interface + DrizzleStorage class
  openapi.ts       — OpenAPI 3.0.3 spec object
  config/          — alerting.ts, tenancy.ts (constants from env)
  middleware/      — apiKeyAuth.ts, observability.ts, rateLimiter.ts, rbac.ts
  routers/         — xxxRouter.ts  (one per domain; PascalCase except kebab-case suffix)
  services/        — xxxService.ts (business logic, one concern per file)
  utils/           — pagination.ts, AppError

shared/            — Isomorphic code imported by both client and server
  schema.ts        — Single source of truth for ALL Drizzle table definitions + types
  signalTypes.ts   — SignalSeverity enum, SEVERITY_RANK, ConfidenceBand
  alertSubscriptionDto.ts
  signalDto.ts
  metrics.ts

drizzle/migrations/ — 21 sequential SQL migration files managed by drizzle-kit
scripts/           — Operational/test scripts (not bundled into server build)
docs/              — Phase DoD documents
```

---

## Naming Conventions (enforced — never break these)

| Location | Convention | Example |
|---|---|---|
| `client/src/pages/` | PascalCase `.tsx` | `Dashboard.tsx`, `AlertSubscriptions.tsx` |
| `client/src/components/` | PascalCase `.tsx` | `MapPanel.tsx`, `MlPredictionCard.tsx` |
| `client/src/components/ui/` | **kebab-case** (shadcn owns this) | `button.tsx`, `dialog.tsx` |
| `client/src/hooks/` | camelCase starting with `use` | `useSignals.ts`, `useWebSocket.ts` |
| `server/routers/` | camelCase + `.router.ts` suffix | `signals.router.ts`, `alerts.router.ts` |
| `server/services/` | PascalCase + `Service` suffix | `signalEngineService.ts`, `alertDlqQueueService.ts` |
| `server/middleware/` | camelCase + `.ts` | `apiKeyAuth.ts`, `rateLimiter.ts` |
| Drizzle table names | `snake_case` plural | `vessel_positions`, `alert_subscriptions` |
| TypeScript types (inferred from Drizzle) | PascalCase | `VesselPosition`, `AlertSubscription` |

**Mistakes that were corrected (do not reintroduce):**
- ❌ `use-mobile.tsx` — was renamed to `useWebSocket.ts` etc. (use-xxx → useXxx)
- ❌ `signals-engine.ts` — was renamed to `signalEngineService.ts` (kebab-case in services)
- ❌ Page files that were kebab-case (`alert-subscriptions.tsx`) — renamed to PascalCase
- ❌ `graphql.ts` — dead file that was deleted; do not recreate it

---

## Architecture Rules

### `server/routes.ts` is an orchestrator only
It imports and mounts routers. **Never add business logic, inline route handlers, or middleware directly in `routes.ts`.** Every feature lives in its router file under `server/routers/`.

### Dev endpoints must be guarded
The `devRouter` is only mounted when `NODE_ENV !== 'production'`. Any dev-only endpoint (seed data, debug dumps) must go into `dev.router.ts` and never be added to another router.

### Shared schema is the single source of truth
All Drizzle table definitions live in `shared/schema.ts`. **Never** define a table in `server/db.ts`, `drizzle/schema.ts`, or any other file. `drizzle/schema.ts` only re-exports from `shared/schema.ts`.

### Pagination must use `parseSafeLimit` / `parsePaginationParams`
Never use bare `parseInt(req.query.limit)` — this bypasses bounds checking.
Use `parseSafeLimit(req.query.limit, defaultLimit, maxLimit)` from `server/utils/pagination.ts`.

**Mistake fixed:** Multiple routes used raw `parseInt(req.query.limit as string)` which allowed arbitrarily large DB queries. All have been replaced with `parseSafeLimit`.

### Error handling
- Throw `AppError` for structured 4xx errors (`new AppError(message, statusCode)`)
- The global error handler in `server/index.ts` catches `AppError` and formats the response
- **Never** send raw `error.message` from unexpected errors to the client in production — the global handler sanitises 5xx responses already
- **Never** use `console.log` for logging — use the `logger` singleton from `server/middleware/observability.ts`

---

## Security Requirements (OWASP Top 10 — always enforce)

### Authentication & secrets
- `JWT_SECRET` must be present on startup; server **refuses to start** if absent
- `API_KEY_PEPPER` must be present on startup; server **refuses to start** if absent
- API keys are stored as `sha256(pepper + rawKey)` — **plaintext is never persisted**
- JWT access tokens expire in 15 minutes; refresh tokens in 7 days
- Passwords are hashed with `bcryptjs` at 10 salt rounds

### Input validation
- All user-supplied IDs in route params must be validated before DB lookup
- All `req.query` page/limit parameters must go through `parseSafeLimit` / `parsePaginationParams`
- Use Zod for request body validation on all write endpoints

### SQL injection
- **Never** use raw string interpolation in SQL — always use Drizzle's query builder or parameterised `sql` tagged template literals
- The only acceptable raw SQL is inside `sql\`...\`` tagged templates where values are bound parameters

### Tenant isolation
- All alert-related tables have a `tenantId` column
- Every query against `alert_subscriptions`, `alert_deliveries`, `alert_runs`, `alert_dlq`, `alert_dedupe`, `alert_delivery_attempts`, `api_keys` **must** filter by `tenantId`
- `resolveTenantId()` from `server/config/tenancy.ts` is the canonical way to get the tenant for a request

### Webhook security
- Outgoing webhooks include `X-Veriscope-Signature: sha256=<HMAC-SHA256>` using the subscription's secret
- Secrets are stored encrypted; never log a subscription secret
- Idempotency keys prevent duplicate deliveries: deterministic `sha1(subscriptionId|clusterId|day)`

### Rate limiting
- The in-memory rate limiter in `server/middleware/rateLimiter.ts` must remain on all public endpoints
- Alert delivery is additionally rate-limited per endpoint via `ALERT_RATE_LIMIT_PER_ENDPOINT` (default 50/run)

### Headers
- Helmet is applied at the top of the middleware chain in `server/index.ts` — never remove it or move it below body-parsing middleware

---

## Environment Variables

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DATABASE_URL` | ✅ | — | Neon or Postgres connection string |
| `JWT_SECRET` | ✅ | — | Refuse to start if missing |
| `API_KEY_PEPPER` | ✅ | — | Refuse to start if missing |
| `NODE_ENV` | — | `development` | Gates dev endpoints |
| `AISSTREAM_API_KEY` | — | — | Absent → simulation mode |
| `ALERTS_API_KEY` | — | — | Env shortcut for API key auth |
| `ALERTS_USER_ID` | — | — | Required when `ALERTS_API_KEY` is set |
| `ALERTS_TENANT_ID` | — | `TENANT_DEMO_ID` | Multi-tenant scoping |
| `ALERT_RATE_LIMIT_PER_ENDPOINT` | — | `50` | |
| `ALERT_DEDUPE_TTL_HOURS` | — | `24` | |
| `WEBHOOK_TIMEOUT_MS` | — | `5000` | |
| `WEBHOOK_RETRY_ATTEMPTS` | — | `3` | |
| `DLQ_MAX_ATTEMPTS` | — | `10` | |
| `OPENAI_API_KEY` | — | — | ML/AI integrations |

---

## Key Constants

```typescript
// server/config/tenancy.ts
TENANT_DEMO_ID = "00000000-0000-0000-0000-000000000001"

// Signal severity z-score thresholds (signalEngineService.ts)
z ∈ [2, 3)  → LOW
z ∈ [3, 4)  → MEDIUM
z ∈ [4, 5)  → HIGH
z ≥ 5       → CRITICAL

// Port geofence radii
Rotterdam (NLRTM): 15 km
Singapore (SGSIN): 12 km
Fujairah  (AEFJR): 10 km
```

---

## RBAC Roles & Permissions

| Role | Key permissions |
|---|---|
| `admin` | All permissions + `admin:system`, `write:users`, `read:audit` |
| `analyst` | read+write signals, predictions, models, watchlists, alerts; read vessels/ports/storage |
| `operator` | read+write vessels, ports, storage, watchlists, alerts; read signals, predictions |
| `viewer` | Read-only across all resources |

Use `requirePermission('read:signals')` or `requireRole('admin')` guards from `server/middleware/rbac.ts`.
Never hand-roll permission checks inline in route handlers.

---

## Database Conventions

- **Schema first**: add columns/tables to `shared/schema.ts`, then run `npm run db:push` or `npx drizzle-kit generate`
- **Never** write manual SQL that is the equivalent of a Drizzle schema change — always go through the migration process
- **Drizzle-Zod**: use `createInsertSchema` / `createSelectSchema` from `drizzle-zod` for request validation, don't write separate Zod schemas for tables that already have Drizzle definitions
- All timestamps use `timestamptz` (timezone-aware); **never** use `timestamp` without timezone — migration `0020` fixed this for alert tables; do not regress
- `ON CONFLICT DO UPDATE` (upsert) is the correct pattern for idempotent writes (signals, baselines, dedupe)
- All tenant-scoped tables have a composite index on `(tenantId, createdAt DESC)` — keep this pattern for new tables

**Mistakes fixed (do not reintroduce):**
- ❌ Using `timestamp` instead of `timestamptz` for alert subscription columns (fixed in migration `0020`)

---

## Frontend Conventions

### Routing
All routes are declared explicitly in `client/src/App.tsx` using Wouter's `<Switch>` + `<Route>`. There is **no** file-based routing. When adding a page, add its import and `<Route>` to `App.tsx`.

### Data fetching
- All server state goes through TanStack React Query hooks in `client/src/hooks/`
- Never `fetch()` directly in a component — create a `useXxx` hook
- The global `queryClient` is in `client/src/lib/queryClient.ts`

### WebSocket
The WebSocket hook (`hooks/useWebSocket.ts`) manages the single `/ws` connection. Do not open additional WebSocket connections from components.

### UI components
- Use existing shadcn/ui primitives from `client/src/components/ui/` before creating anything new
- Follow the [shadcn/ui](https://ui.shadcn.com/) pattern: do not edit generated primitive files; compose new components from them instead
- Dark mode is handled by `next-themes` — use `dark:` Tailwind variants, never inline style for colour

---

## TypeScript Rules

- `strict: true` is on — all code must typecheck with zero errors under `npx tsc --noEmit`
- Never use `as any` as an escape hatch; find the correct type
- Never use non-null assertion (`!`) on values that may genuinely be null/undefined at runtime
- Types exported from `shared/schema.ts` (e.g. `type Port = typeof ports.$inferSelect`) are the canonical types — use them everywhere; don't redeclare equivalent types elsewhere

**Mistakes fixed during Phase 2 (52 TypeScript errors → 0):**
- ❌ Missing return type annotations on async service methods
- ❌ `req.user` accessed without narrowing — always check `if (!req.user)` before using
- ❌ Drizzle `eq()` called with wrong column type (string vs number)
- ❌ `res.json()` called with values that had mismatched shape vs OpenAPI spec types
- ❌ `Date` vs `string` confusion on timestamp fields from Drizzle — Neon driver returns strings; cast explicitly

---

## Common Patterns

### Checking auth state in frontend components
- **Never** use `getAuthToken()` — it always returns `null` (tokens are httpOnly cookies, not JS-accessible)
- Use the `useCurrentUser()` hook from `client/src/hooks/useCurrentUser.ts` — it calls `GET /api/auth/me` which reads the cookie server-side
- Gate query `enabled` on `!authLoading && !!currentUser` so queries don't fire before auth is resolved

```tsx
const { data: currentUser, isLoading: authLoading } = useCurrentUser();
const isLoggedIn = !!currentUser;
```

### Adding a new API endpoint
1. Identify the correct router in `server/routers/`
2. Add the route handler inside that router file
3. Use `requirePermission('...')` as the auth guard
4. Validate request body with Zod (use Drizzle-Zod schema where possible)
5. Use `parseSafeLimit` for any pagination
6. Update `server/openapi.ts` with the new path

### Adding a new table
1. Add the Drizzle table definition to `shared/schema.ts`
2. Export the inferred types
3. Run `npx drizzle-kit generate` to produce the SQL migration file
4. Run `npm run db:push` to apply to DB
5. If the table is tenant-scoped, add `tenantId` column and index on `(tenantId, createdAt DESC)`

### Adding a new page
1. Create `client/src/pages/MyPage.tsx` (PascalCase)
2. Add the route to `client/src/App.tsx`
3. Create a `useMyData.ts` hook in `client/src/hooks/` if you need an API call

### Adding a new service
1. Create `server/services/myDomainService.ts` (camelCase + `Service` suffix)
2. Keep each service focused on a single concern
3. Use the `logger` singleton for all logging
4. Export a plain object or named functions — avoid class instances unless state is required

---

## Dev Workflow

```bash
npm run dev          # Start full-stack dev server (port 5000, Vite HMR)
npm run check        # npx tsc --noEmit — must be zero errors before committing
npm run build        # Vite (frontend) + esbuild (backend) production build
npm start            # Run production build

npm run db:push      # Push schema changes to DB
npm run test:baselines
npm run test:signals
bash scripts/smoke-test.sh
```

Default dev credentials: `admin@example.com` / `admin123`

---

## Files to Never Modify Directly

| File | Reason |
|---|---|
| `client/src/components/ui/*.tsx` | shadcn/ui generated primitives — update via CLI only |
| `drizzle/migrations/*.sql` | Immutable migration history — never edit after applying |
| `shared/schema.ts` | Only add new definitions; never rename or remove columns without a migration |

---

## Known Past Mistakes Log

This section is the single authoritative record of mistakes discovered and fixed.
Before any significant edit, scan this list to avoid regressions.

| # | File(s) | Mistake | Fix Applied |
|---|---|---|---|
| 1 | `server/routes.ts` | Was a 700+ line monolith with all route handlers inline | Refactored to 51-line orchestrator + 9 feature routers |
| 2 | `server/services/*.ts` | Files used kebab-case naming (`signal-engine.ts`) | Renamed to camelCase + Service suffix (`signalEngineService.ts`) |
| 3 | `client/src/hooks/` | Files used kebab-case naming (`use-mobile.tsx`) | Renamed to camelCase (`useMobile.tsx`) |
| 4 | `client/src/pages/` | Files used kebab-case naming (`alert-subscriptions.tsx`) | Renamed to PascalCase (`AlertSubscriptions.tsx`) |
| 5 | Multiple routers | `parseInt(req.query.limit as string)` — no bounds | Replaced with `parseSafeLimit()` from `/utils/pagination.ts` |
| 6 | `server/routes.ts` | Dev endpoints (`seedDemoApiKey`) mounted unconditionally | Moved to `dev.router.ts`, guarded by `NODE_ENV !== 'production'` |
| 7 | `server/graphql.ts` | Dead file with no usages, imported nowhere | Deleted |
| 8 | 10 files | 52 TypeScript errors (missing types, unsafe access, wrong Drizzle column types) | Fixed all; `tsc --noEmit` exits 0 |
| 9 | `drizzle/migrations/0020_*` | Alert subscription timestamp columns used `timestamp` not `timestamptz` | Migration applied to alter to `timestamptz` |
| 10 | Multiple services | `console.log` / `console.error` used directly | Replaced with `logger.info` / `logger.error` from `observability.ts` |
| 11 | Security audit (20 issues) | Missing input sanitisation, unsafe error propagation, missing rate limits | All 20 issues resolved per OWASP Top 10 audit |
| 12 | `server/index.ts` | `dotenv` was never installed or imported — `process.env` vars always undefined when running via `tsx` locally | Added `npm install dotenv` and `import "dotenv/config"` as the first line of `server/index.ts` |
| 13 | `server/index.ts` | `reusePort: true` guarded by `platform !== 'win32'` — causes `ENOTSUP` crash on macOS (Node v25) | Changed guard to `platform === 'linux'`; `reusePort` only needed for Linux multi-process clustering |
| 14 | Root | No `.env.example` file — `cp .env.example .env` failed for new developers | Created `.env.example` with all variables documented; includes macOS `PORT=3000` note for AirPlay conflict |
| 15 | `server/routes.ts` | `mockDataService.initializeBaseData()` was never called on startup — admin user, ports, vessels never seeded automatically | Added idempotent `initializeBaseData()` call in `registerRoutes()` guarded by `NODE_ENV !== 'production'` |
| 16 | `client/src/lib/queryClient.ts` | `getAuthToken()` was refactored to always return `null` (tokens moved to httpOnly cookies) but `AisTracking.tsx` still used it to gate auth — always showed "Authentication Required" even when logged in | Added `GET /api/auth/me` endpoint; created `useCurrentUser` hook; updated `AisTracking.tsx` to use the hook |
| 17 | `client/src/pages/auth/Login.tsx` | Calls `/v1/auth/login` which returns `access_token` in body and sets httpOnly cookies — the localStorage writes are redundant but harmless; the httpOnly cookie is what actually authenticates subsequent requests | No code change needed; document that `/api/auth/me` is the canonical way to check auth state on the frontend |
| 18 | `client/src/components/` and `client/src/pages/` | `OnboardingModal.tsx`, `VesselMarker.tsx`, `Flightscope.tsx`, `Shipscope.tsx` were dead files (never imported or routed) | Deleted all four files |
| 19 | All `server/routers/*.ts` and `server/services/*.ts` | 194+ `console.log/error/warn` calls used directly instead of the project `logger` from `server/middleware/observability.ts` | Replaced all with `logger.info/error/warn` calls with structured metadata objects |
| 20 | `server/routers/auth.router.ts` (lines 98 and 256) | `authRouter.post("/api/auth/refresh"), async (req, res) => {` — closing parenthesis misplaced, registering route with no handler and leaving orphaned async arrow function | Fixed to `authRouter.post("/api/auth/refresh", async (req, res) => {` in both the `/api/auth/refresh` and `/v1/auth/refresh` routes |
