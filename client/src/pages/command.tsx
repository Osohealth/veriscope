import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "wouter";
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  Clock,
  Crosshair,
  Download,
  Flame,
  Globe,
  ShieldAlert,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { LoadingState } from "@/components/loading-state";
import { cn } from "@/lib/utils";
import { getAuthHeaders } from "@/lib/queryClient";
import { TerminalStoreProvider, useTerminalStore } from "@/hooks/use-terminal-store";
import CongestionMap from "@/components/congestion-map";
import { listInvestigations, type Investigation } from "@/lib/investigations";

type CongestionPort = {
  id: string;
  portId: string;
  portName: string;
  vesselCount: number;
  queueCount: number;
  avgWaitHours: number;
  dwellHours: number;
  throughputEstimate?: number;
  riskScore: number;
  severity?: "low" | "medium" | "high";
  whyItMatters?: string;
  latitude?: number | null;
  longitude?: number | null;
};

type Incident = {
  id: string;
  type: string;
  destination_key?: string | null;
  status: "OPEN" | "ACKED" | "RESOLVED";
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  title: string;
  summary: string;
  opened_at: string;
};

type OpsSnapshot = {
  counters?: Record<string, number>;
  delivery_latency_ms?: {
    EMAIL?: { p95?: number | null };
    WEBHOOK?: { p95?: number | null };
  };
  escalation_run_duration_ms?: { p95?: number | null };
};

const COMMODITIES = ["Crude", "Products", "LNG", "LPG", "Dry Bulk", "Metals", "Agri"];
const MODES = ["Sea", "Air", "Rail", "Cross-modal"];
const TIME_MODES = ["Live", "24h", "7d", "30d"];

const buildQuery = (filters: any) => {
  const params = new URLSearchParams();
  if (filters.commodity?.[0]) params.set("commodity", filters.commodity[0]);
  if (filters.origin?.[0]) params.set("origin", filters.origin[0]);
  if (filters.destination?.[0]) params.set("destination", filters.destination[0]);
  if (filters.hub?.[0]) params.set("hub", filters.hub[0]);
  if (filters.region?.[0]) params.set("region", filters.region[0]);
  if (filters.timeMode === "range" && filters.timeWindow) params.set("time", filters.timeWindow);
  return params.toString();
};

const severityRank: Record<string, number> = {
  CRITICAL: 4,
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

const extractHubToken = (text?: string | null) => {
  if (!text) return null;
  const match = text.match(/[A-Z]{5}/);
  return match ? match[0] : null;
};

const buildContextLink = (incident: Incident) => {
  const type = incident.type?.toLowerCase() ?? "";
  const destinationKey = incident.destination_key?.toLowerCase() ?? "";
  const hubToken =
    extractHubToken(incident.destination_key ?? "") ??
    extractHubToken(incident.title) ??
    extractHubToken(incident.summary);
  const params = hubToken ? `?hub=${encodeURIComponent(hubToken)}` : "";

  if (destinationKey.includes("congestion") || destinationKey.includes("port") || type.includes("congestion") || type.includes("port")) {
    return `/congestion${params}`;
  }
  if (destinationKey.includes("flow") || destinationKey.includes("lane") || destinationKey.includes("trade") || type.includes("flow") || type.includes("lane") || type.includes("trade")) {
    return `/flows${params}`;
  }
  return `/terminal${params}`;
};

function CommandContent() {
  const { filters, updateFilters, resetFilters, selectedEntity } = useTerminalStore();
  const [search, setSearch] = useState("");
  const [ports, setPorts] = useState<CongestionPort[]>([]);
  const [portsState, setPortsState] = useState<"idle" | "loading" | "error">("idle");
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [incidentsState, setIncidentsState] = useState<"idle" | "loading" | "error">("idle");
  const [ops, setOps] = useState<OpsSnapshot | null>(null);
  const [investigations, setInvestigations] = useState<Investigation[]>([]);
  const portsRequestIdRef = useRef(0);
  const incidentsRequestIdRef = useRef(0);
  const opsRequestIdRef = useRef(0);

  const query = useMemo(() => buildQuery(filters), [filters]);

  const loadPorts = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++portsRequestIdRef.current;
      setPortsState("loading");
      try {
        const res = await fetch(`/v1/congestion/ports?${query}`, { signal, headers: getAuthHeaders() });
        if (!res.ok) throw new Error("Failed to load ports");
        const data = await res.json();
        if (requestId !== portsRequestIdRef.current) return;
        setPorts(Array.isArray(data.items) ? data.items : []);
        setPortsState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== portsRequestIdRef.current) return;
        setPortsState("error");
      }
    },
    [query]
  );

  const refreshPorts = useCallback(() => {
    const controller = new AbortController();
    loadPorts(controller.signal);
    return controller;
  }, [loadPorts]);

  useEffect(() => {
    const controller = refreshPorts();
    return () => controller.abort();
  }, [refreshPorts]);

  const loadIncidents = useCallback(async (signal?: AbortSignal) => {
    const requestId = ++incidentsRequestIdRef.current;
    setIncidentsState("loading");
    try {
      const res = await fetch(`/v1/incidents?status=OPEN&limit=200`, { signal, headers: getAuthHeaders() });
      if (!res.ok) throw new Error("Failed to load incidents");
      const data = await res.json();
      if (requestId !== incidentsRequestIdRef.current) return;
      setIncidents(Array.isArray(data.items) ? data.items : []);
      setIncidentsState("idle");
    } catch (error) {
      if ((error as Error)?.name === "AbortError") return;
      if (requestId !== incidentsRequestIdRef.current) return;
      setIncidentsState("error");
    }
  }, []);

  const refreshIncidents = useCallback(() => {
    const controller = new AbortController();
    loadIncidents(controller.signal);
    return controller;
  }, [loadIncidents]);

  useEffect(() => {
    const controller = refreshIncidents();
    return () => controller.abort();
  }, [refreshIncidents]);

  const loadOps = useCallback(async (signal?: AbortSignal) => {
    const requestId = ++opsRequestIdRef.current;
    try {
      const res = await fetch("/metrics/ops", { signal });
      if (!res.ok) throw new Error("Failed to load ops");
      const data = await res.json();
      if (requestId !== opsRequestIdRef.current) return;
      setOps(data);
    } catch (error) {
      if ((error as Error)?.name === "AbortError") return;
      if (requestId !== opsRequestIdRef.current) return;
      setOps(null);
    }
  }, []);

  const refreshOps = useCallback(() => {
    const controller = new AbortController();
    loadOps(controller.signal);
    return controller;
  }, [loadOps]);

  useEffect(() => {
    const controller = refreshOps();
    return () => controller.abort();
  }, [refreshOps]);

  useEffect(() => {
    setInvestigations(listInvestigations().slice(0, 5));
  }, []);

  const topPorts = [...ports].sort((a, b) => b.riskScore - a.riskScore);
  const hotspots = topPorts.slice(0, 5);
  const openIncidents = incidents.filter((i) => i.status === "OPEN");
  const escalated = openIncidents.filter((i) => i.severity === "CRITICAL");
  const acked = incidents.filter((i) => i.status === "ACKED");
  const resolved = incidents.filter((i) => i.status === "RESOLVED");
  const topIncident = [...openIncidents].sort((a, b) => (severityRank[b.severity] ?? 0) - (severityRank[a.severity] ?? 0))[0];

  const healthCounters = ops?.counters ?? {};
  const freshnessP95 = ops?.delivery_latency_ms?.WEBHOOK?.p95 ?? ops?.delivery_latency_ms?.EMAIL?.p95 ?? null;

  const modeLabel =
    filters.mode === "cross"
      ? "Cross-modal"
      : filters.mode === "sea"
        ? "Sea"
        : filters.mode === "air"
          ? "Air"
          : "Rail";

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-40 border-b border-border bg-card/95 backdrop-blur">
        <div className="mx-auto flex w-full max-w-7xl items-center gap-4 px-6 py-4">
          <button
            type="button"
            onClick={() => window.history.length > 1 ? window.history.back() : (window.location.href = "/")}
            className="flex items-center gap-2 text-sm text-muted-foreground hover:text-primary"
          >
            <ArrowLeft className="h-4 w-4" />
            Back
          </button>
          <div className="flex items-center gap-2 text-lg font-semibold">
            <ShieldAlert className="h-5 w-5 text-primary" />
            Command
          </div>
          <div className="flex flex-1 flex-wrap items-center gap-3">
            <Select value={filters.commodity?.[0] ?? "Crude"} onValueChange={(value) => updateFilters({ commodity: [value] })}>
              <SelectTrigger className="h-9 w-[160px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {COMMODITIES.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select
              value={modeLabel}
              onValueChange={(value) => {
                const normalized = value.toLowerCase();
                updateFilters({
                  mode: (normalized === "cross-modal" ? "cross" : normalized) as "sea" | "air" | "rail" | "cross",
                });
              }}
            >
              <SelectTrigger className="h-9 w-[150px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {MODES.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <Select
              value={filters.timeMode === "live" ? "Live" : filters.timeWindow ?? "7d"}
              onValueChange={(value) =>
                updateFilters({
                  timeMode: value === "Live" ? "live" : "range",
                  timeWindow: value === "Live" ? undefined : (value as "24h" | "7d" | "30d"),
                })
              }
            >
              <SelectTrigger className="h-9 w-[110px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                {TIME_MODES.map((item) => (
                  <SelectItem key={item} value={item}>
                    {item}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <div className="flex flex-1 items-center gap-2 rounded-md border border-border bg-background px-2">
              <SearchInput value={search} onChange={setSearch} />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-emerald-400" />
              Freshness {freshnessP95 ? `${Math.round(freshnessP95)}ms` : "-"}
            </Badge>
            <Button variant="outline" size="sm">
              <Download className="mr-2 h-4 w-4" />
              Export
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto grid w-full max-w-7xl grid-cols-[260px_1fr_320px] gap-6 px-6 py-6">
        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Filters</h2>
            <Button variant="ghost" size="sm" onClick={resetFilters}>
              Reset
            </Button>
          </div>
          <Card>
            <CardContent className="space-y-4 pt-4">
              <div>
                <label className="text-xs text-muted-foreground">Region</label>
                <Select
                  value={filters.region?.[0] ?? "Global"}
                  onValueChange={(value) => updateFilters({ region: value === "Global" ? undefined : [value] })}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Global">Global</SelectItem>
                    <SelectItem value="Europe">Europe</SelectItem>
                    <SelectItem value="Asia">Asia</SelectItem>
                    <SelectItem value="Americas">Americas</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Hub / Port</label>
                <Input
                  value={filters.hub?.[0] ?? ""}
                  onChange={(event) => updateFilters({ hub: event.target.value ? [event.target.value] : undefined })}
                  placeholder="Search port"
                  className="mt-2"
                />
              </div>
              <Button className="w-full" onClick={refreshPorts}>
                <Crosshair className="mr-2 h-4 w-4" />
                Apply filters
              </Button>
            </CardContent>
          </Card>
        </aside>

        <section className="space-y-6">
          <div className="grid grid-cols-5 gap-4">
            {[
              { label: "Ports monitored", value: ports.length },
              { label: "Congested ports", value: ports.filter((p) => p.riskScore > 60).length },
              { label: "Active incidents", value: openIncidents.length },
              { label: "Alert volume 24h", value: healthCounters.deliveries_created_total ?? 0 },
              { label: "Top risk", value: hotspots[0]?.portName ?? "-" },
            ].map((kpi) => (
              <Card key={kpi.label} className="border-border/60">
                <CardContent className="space-y-2 pt-4">
                  <div className="text-xs text-muted-foreground">{kpi.label}</div>
                  <div className="text-lg font-semibold">{kpi.value}</div>
                </CardContent>
              </Card>
            ))}
          </div>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Global hotspot map</CardTitle>
              <Badge variant="secondary">{ports.length} ports</Badge>
            </CardHeader>
            <CardContent>
              {portsState === "loading" ? (
                <LoadingState label="Loading hotspots..." />
              ) : portsState === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load hotspots.</span>
                  <Button size="sm" variant="outline" onClick={refreshPorts}>
                    Retry
                  </Button>
                </div>
              ) : (
                <CongestionMap ports={ports} />
              )}
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Live alert board</CardTitle>
              <Button variant="ghost" size="sm" asChild>
                <Link href="/alerts">Open Alerts</Link>
              </Button>
            </CardHeader>
            <CardContent>
              {incidentsState === "loading" ? (
                <LoadingState label="Loading alerts..." />
              ) : incidentsState === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load alerts.</span>
                  <Button size="sm" variant="outline" onClick={refreshIncidents}>
                    Retry
                  </Button>
                </div>
              ) : (
                <div className="grid grid-cols-4 gap-3 text-xs text-muted-foreground">
                  <BoardColumn title="New" items={openIncidents} />
                  <BoardColumn title="Escalated" items={escalated} />
                  <BoardColumn title="Acked" items={acked} />
                  <BoardColumn title="Resolved" items={resolved} />
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        <aside className="space-y-4">
          <Card className="border-border/60">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Executive summary</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 text-xs text-muted-foreground">
              <div className="flex items-center gap-2">
                <Flame className="h-4 w-4 text-rose-400" />
                <span>Biggest hotspot: {hotspots[0]?.portName ?? "-"}</span>
              </div>
              <div className="flex items-center gap-2">
                <AlertTriangle className="h-4 w-4 text-amber-400" />
                <span>Critical alert: {topIncident?.title ?? "-"}</span>
              </div>
              <div className="flex items-center gap-2">
                <Activity className="h-4 w-4 text-sky-400" />
                <span>Latest investigation: {investigations[0]?.title ?? "-"}</span>
              </div>
              <Button size="sm" variant="outline" asChild>
                <Link href={topIncident ? buildContextLink(topIncident) : "/terminal"}>Open context</Link>
              </Button>
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">System health</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-xs text-muted-foreground">
              {!ops && (
                <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
                  <span>Health snapshot unavailable.</span>
                  <Button size="sm" variant="outline" onClick={refreshOps}>
                    Retry
                  </Button>
                </div>
              )}
              <div className="flex items-center justify-between">
                <span>Freshness p95</span>
                <span>{freshnessP95 ? `${Math.round(freshnessP95)}ms` : "-"}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Active incidents</span>
                <span>{openIncidents.length}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Alert volume 24h</span>
                <span>{healthCounters.deliveries_created_total ?? 0}</span>
              </div>
              <div className="flex items-center justify-between">
                <span>Source coverage</span>
                <span>-</span>
              </div>
              <div className="flex items-center justify-between">
                <span>False positives</span>
                <span>-</span>
              </div>
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Top investigations</CardTitle>
            </CardHeader>
            <CardContent className="space-y-2 text-xs text-muted-foreground">
              {investigations.length === 0 ? (
                <div>No investigations yet.</div>
              ) : (
                investigations.map((item) => (
                  <Link key={item.id} href={`/investigations/${item.id}`} className="block rounded-md border border-border/40 px-3 py-2">
                    <div className="text-sm font-medium text-foreground">{item.title}</div>
                    <div className="text-[11px] text-muted-foreground">Updated {new Date(item.updatedAt).toLocaleString()}</div>
                    {item.sourceRoute && <div className="text-[11px] text-muted-foreground">Source: {item.sourceRoute}</div>}
                  </Link>
                ))
              )}
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}

function SearchInput({ value, onChange }: { value: string; onChange: (next: string) => void }) {
  return (
    <>
      <Globe className="h-4 w-4 text-muted-foreground" />
      <Input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder="Search entity, alert, port"
        className="h-8 border-none bg-transparent px-0 text-sm shadow-none focus-visible:ring-0"
      />
    </>
  );
}

function BoardColumn({ title, items }: { title: string; items: Incident[] }) {
  return (
    <div className="rounded-md border border-border/50 bg-background/40 p-3">
      <div className="mb-2 text-xs font-semibold text-muted-foreground">{title}</div>
      <div className="space-y-2">
        {items.length === 0 ? (
          <div className="text-[11px] text-muted-foreground">No items</div>
        ) : (
          items.slice(0, 4).map((incident) => (
            <Link key={incident.id} href={buildContextLink(incident)} className="block rounded-md border border-border/40 px-2 py-1">
              <div className="text-xs font-semibold text-foreground">{incident.title}</div>
              <div className="text-[11px] text-muted-foreground">{incident.severity} | {new Date(incident.opened_at).toLocaleDateString()}</div>
            </Link>
          ))
        )}
      </div>
    </div>
  );
}

export default function CommandPage() {
  return (
    <TerminalStoreProvider basePath="/command">
      <CommandContent />
    </TerminalStoreProvider>
  );
}
