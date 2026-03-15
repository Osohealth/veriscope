# Dashboard Vision -> Execution Backlog (Phase 5+)

This backlog converts the Vision Pack into execution-grade work items.
Format: EPIC -> Stories -> Acceptance Criteria.

Scope note: This assumes Phase 1-4 already delivered alerts/incidents/escalations/metrics.

---

## PHASE 5 — Dashboard Core (Terminal Mode Foundation)

### EPIC 5.1 — App Shell & Global State

**Story 5.1.1 — Persistent App Shell**

Deliver:
- GlobalHeader (commodity/mode/time/region controls)
- FilterDrawer (saved/pinned filters)
- SignalRail (top signals + "why")
- URL state sync (filters + selection + tab)

Acceptance Criteria:
- Filters encoded in URL
- Reload restores state exactly
- Mode/Commodity/Time persist across routes
- Route changes do not full-page reload

**Story 5.1.2 — Global Filter Engine**

Deliver:
- Unified DashboardFilters model
- Cross-widget reactivity (map/chart/table)
- Debounced API queries
- Query deduplication

Acceptance Criteria:
- Changing any filter updates map + table + charts
- No redundant API calls
- Filter changes are reflected in URL within 200ms

---

### EPIC 5.2 — Overview Page

**Story 5.2.1 — KPI Strip**

Deliver:
- Global exports / imports / net balance
- Top congestion entity
- Top anomaly
- Freshness + confidence

Acceptance Criteria:
- Live mode updates in near-real time
- Freshness timestamp visible
- Confidence displayed when provided

**Story 5.2.2 — Map with Entity Interaction**

Deliver:
- Port layer
- Click to select entity
- Hover tooltips
- Lasso select

Acceptance Criteria:
- Selection updates shared state
- Selection filters all widgets
- Multi-select works (shift-click)

**Story 5.2.3 — Top Signals Table**

Deliver:
- Ranked by anomaly score
- Sortable
- Virtualized

Acceptance Criteria:
- Table click highlights entity on map
- Export includes visible rows only

---

## PHASE 6 — Flows Intelligence (Kpler Baseline)

### EPIC 6.1 — Flow Aggregation Engine (UI Integration)

**Story 6.1.1 — Flow Timeseries Chart**

Deliver:
- Exports/imports over time
- Compare vs previous period
- Anomaly band overlay

Acceptance Criteria:
- Range selection updates chart
- Hover shows precise values
- Live mode appends new datapoints

**Story 6.1.2 — Origin -> Destination Sankey**

Deliver:
- Commodity filtered
- Cross-modal ready schema

Acceptance Criteria:
- Clicking node filters table
- Clicking link filters origin/destination

**Story 6.1.3 — Top Lanes Table**

Deliver:
- Origin
- Destination
- Volume
- Delta vs prior period
- Z-score

Acceptance Criteria:
- Column pinning supported
- Export matches current filter state

---

## PHASE 7 — Congestion Intelligence

### EPIC 7.1 — Congestion Leaderboard

**Story 7.1.1 — Port Congestion Table**

Deliver:
- Vessel count
- Avg wait
- Dwell time
- Throughput estimate
- Risk score

Acceptance Criteria:
- Sorted by risk score by default
- Drilldown opens right-rail detail

**Story 7.1.2 — Port Drilldown View**

Deliver:
- Queue timeseries
- Arrivals vs departures
- Explain panel (delta drivers)

Acceptance Criteria:
- Time range recalculates baseline
- Explain shows drivers (arrivals spike, berth utilization)

---

## PHASE 8 — Alerts Integration (UX on top of existing engine)

### EPIC 8.1 — Alert Command UI

**Story 8.1.1 — Alert Queue**

Deliver:
- Severity filter
- Status filter
- Region/commodity filter
- Realtime updates

Acceptance Criteria:
- New alerts appear without reload
- Ack updates state instantly
- Resolve removes from OPEN filter

**Story 8.1.2 — Alert Detail Drawer**

Deliver:
- Trigger metric
- Threshold vs baseline
- Explain section
- Linked chart preview

Acceptance Criteria:
- Drill opens relevant route with filters pre-applied
- Evidence links to timeseries

**Story 8.1.3 — Rule Builder UI**

Deliver:
- Create rule with threshold
- Enable/disable
- Link to escalation engine

Acceptance Criteria:
- Rule persists and appears in list
- Enable toggles affect live alerts

---

## PHASE 9 — Saved Views & Shareability

### EPIC 9.1 — Views Engine

**Story 9.1.1 — Save Current View**

Stores:
- Route
- Filters
- Selection
- Layout config

Acceptance Criteria:
- Opening view restores state exactly
- Multi-tenant safe
- User-scoped

**Story 9.1.2 — Shareable URL**

Acceptance Criteria:
- URL reconstructs state without server calls
- Works across sessions

---

## PHASE 10 — Investigation Mode (Palantir Layer)

### EPIC 10.1 — Entity Graph

**Story 10.1.1 — Entity Graph Canvas**

Nodes:
- Ports
- Vessels
- Corridors
- Companies

Edges:
- Flow
- Ownership
- Routing

Acceptance Criteria:
- Clicking node filters workspace
- Graph and charts cross-filter

**Story 10.1.2 — Case Workspace**

Deliver:
- Hypothesis field
- Evidence list
- Confidence score
- Export evidence pack (PDF later)

---

## PHASE 11 — Command Mode (NOC Wall)

### EPIC 11.1 — Live Wall View

**Story 11.1.1 — Global Hotspot Map**

Deliver:
- Heat overlay
- Critical alerts pinned

**Story 11.1.2 — System Health Panel**

Deliver:
- Freshness p95
- False positive rate
- Coverage % by mode

---

## Technical Epics (Cross-cutting)

### EPIC T1 — Realtime Engine Stabilization
- WebSocket reconnection
- Event buffering
- Idempotent updates

### EPIC T2 — Performance Optimization
- Query caching
- Table virtualization
- Debounced filter updates

### EPIC T3 — Export Engine
- CSV immediate
- PDF scaffold endpoint
- Include metadata in exports

---

## Suggested Sprint Order

Sprint A:
- App shell
- Overview
- Cross-filter state

Sprint B:
- Flows
- Congestion leaderboard

Sprint C:
- Alert UI integration
- Saved views

Sprint D:
- Investigation scaffold
- Performance + polish

---

## Institutional-Grade Definition of Done

- Cross-filtering is seamless
- Filters persist in URL
- Alerts integrate into dashboard context
- Export works (CSV + PDF snapshot)
- Realtime updates do not break UI
- Cold load < 2.5s
- Large table scroll smooth at 10k rows

