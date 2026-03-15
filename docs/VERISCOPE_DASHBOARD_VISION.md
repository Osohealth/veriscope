# Veriscope Dashboard Vision Pack (Kpler x Palantir fusion)

This is the north-star spec for Veriscope's dashboard architecture and UX.
It is intended for design + engineering alignment and implementation sequencing.

## 1) Product Principle: 3 Modes, 1 Brain

Mode A - Terminal (Trader)
  "Answer in 10 seconds."

Mode B - Investigation (Analyst / IC Memo)
  "Prove it, show evidence, track confidence."

Mode C - Command (Ops / Alerts NOC)
  "Monitor, escalate, reduce false positives."

One brain: same underlying entities, signals, freshness, and confidence.

## 2) Information Architecture (left navigation)

Top-level:
- Overview
- Flows
- Congestion
- Inventories & Throughput
- Events & Disruptions
- Predictions
- Alerts
- Investigations
- Data Lab
- Admin

Global controls in header:
- Commodity selector (Crude / Products / LNG / LPG / Dry Bulk / Metals / Agri)
- Mode selector (Sea / Air / Rail / Cross-modal)
- Time (Live <-> Historical replay)
- Region lens (Global / Basin / Country / Corridor)
- Data freshness + confidence

## 3) North-star Wireframes (text)

A) Terminal Mode (Kpler-like, cross-modal)

Header: Veriscope | Commodity | Mode | Time | Search | Freshness | Confidence | Alerts | Export | Share
Left: Filters (region, origin/dest, commodity, company, vessel class, risk tags, sanctions)
Center: Map (vessels, flights, trains, hubs, heat layers)
Right: Signal rail (top clustered signals with "why")
Tabs: Flows | Congestion | Inventory | Rates | Orders/Customs | News/Events
KPI strip: Exports | Imports | Net | Dwell | On-time | Risk
Charts: Flow timeseries, Sankey, Top movers, Inventory delta, Congestion curve, Forecast band

Behavior:
- Cross-filtering everywhere
- Timeline scrubber with replay + bookmarks
- One-click export (CSV/XLSX/PDF) + share links

B) Investigation Mode (Palantir-like workspace)

Left: Entity graph (Port, Vessel, Company, Corridor, Links)
Center: Evidence workspace (Map, timeseries, anomalies, documents, object table)
Right: Case file (Hypothesis, Evidence list, Assumptions, Conclusion, Next actions)
Bottom: Event log (triggers, analyst actions, model runs, exports)

C) Command Mode (Common Operating Picture)

Top: Region / Severity / SLA / On-call
Center: Map wall with hotspots + incidents
Left: Alert queue (live)
Right: Health / Quality (freshness, false positives, coverage, delivery)

## 4) Component Library (design system inventory)

Global shell:
- AppHeader (search + mode + commodity + time + export/share)
- GlobalFilterDrawer (saved + pinned filters)
- FreshnessBadge
- ConfidenceBadge
- SeverityChip
- Explain button (reasoning summary + features)

Map stack:
- LayerPanel (toggle, opacity, legend)
- Layers: points, tracks, heatmap, choropleth, corridor ribbon
- Map selection -> right rail details

Core analytic widgets (must cross-filter):
- KPIStatCard
- TimeseriesChart (anomaly overlays + forecast band)
- SankeyFlow
- Origin-Destination matrix
- ObjectTable (virtualized, pin columns)
- EventTimeline
- AlertRuleBuilder
- ExportPanel (CSV/XLSX/PDF with filters + timestamps)

## 5) Design Tokens (initial spec)

Typography:
- Display numbers: 24-32px (KPI strip)
- Section headers: 14-16px
- Table body: 12-13px (dense mode)
- Tabular numerals everywhere

Layout:
- 12-col grid
- Left filters: 280-320px
- Right signal rail: 360-420px
- Bottom charts: 280-360px height
- Density modes: Trader Dense / Client Clean

Color semantics:
- Base: dark neutral
- Accent: 1 primary + 1 secondary
- Success / Warning / Critical
- Confidence gradient (low -> high)
- Freshness gradient (stale -> fresh)
- Risk tags (sanctions, weather, conflict, strikes)

Interactions:
- Hover = preview
- Click = drilldown (locks selection + cross-filter)
- Shift-click = multi-select
- Pin entity to context rail

## 6) MVP Scope

MVP (4-6 weeks) - Terminal + Alerts

Screens:
- Overview (map + KPI strip + top signals)
- Flows (Sea-only, cross-modal-ready schema)
- Congestion (ports)
- Alerts (rule builder + deliveries)

Capabilities:
- Saved views (filters + layout)
- Cross-filtering (table <-> map <-> chart)
- Exports: CSV + PDF snapshot
- Alert thresholds + subscriptions + escalation logs

V1 (6-10 weeks) - Investigation workspace
- Entity graph
- Case files + evidence packs
- Confidence + provenance panel

V2 - Prediction & Scenario
- ETA prediction + congestion forecast
- Price impact likelihood (probabilistic)
- What-if (reroutes, closures, sanctions)

## 7) Veriscope differentiators

- Cross-modal layers (Sea + Air + Rail)
- Confidence + Freshness as first-class UI
- Explainability rail ("why did we alert?")
- Investigation -> Evidence Pack -> Export pipeline
- Alert lifecycle (ack, escalate, resolve, false-positive feedback)

## 8) Design Deliverables

Produce these frames in Figma:
1. Terminal Overview (Global)
2. Terminal Flows (Region drilldown)
3. Terminal Congestion (Port drilldown)
4. Terminal Inventory (Hub drilldown)
5. Alerts queue
6. Alert rule builder
7. Alert detail (explain + evidence)
8. Investigation home (saved cases)
9. Case workspace (entity graph + evidence)
10. Evidence pack export preview
11. Command COP wall screen
12. System health + coverage

