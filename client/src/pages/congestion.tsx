import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { LoadingState } from "@/components/loading-state";
import { cn } from "@/lib/utils";
import { getAuthHeaders } from "@/lib/queryClient";
import { TerminalStoreProvider, useTerminalStore, buildQueryFromFilters } from "@/hooks/use-terminal-store";
import { createInvestigation } from "@/lib/investigations";
import CongestionMap from "@/components/congestion-map";
import { saveView } from "@/lib/saved-views";
import { useToast } from "@/hooks/use-toast";
import { useLocation } from "wouter";
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  Legend,
} from "recharts";
import {
  AlertTriangle,
  ArrowRight,
  ArrowLeft,
  Clock,
  Download,
  Filter,
  Flame,
  Globe,
  MapPin,
  RefreshCw,
  Search,
  Target,
} from "lucide-react";

type CongestionSummary = {
  portsMonitored: number;
  congestedPorts: number;
  avgWaitHours: number;
  maxDwellHours: number;
  topRiskPort: string | null;
};

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

type CongestionPoint = {
  day: string;
  queueCount: number;
  arrivals: number;
  departures: number;
  waitHours: number;
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

function CongestionContent() {
  const { filters, updateFilters, resetFilters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const [search, setSearch] = useState("");
  const [summary, setSummary] = useState<CongestionSummary | null>(null);
  const [ports, setPorts] = useState<CongestionPort[]>([]);
  const [series, setSeries] = useState<CongestionPoint[]>([]);
  const [state, setState] = useState<"idle" | "loading" | "error">("idle");
  const [seriesState, setSeriesState] = useState<"idle" | "loading" | "error">("idle");
  const requestIdRef = useRef(0);
  const seriesRequestIdRef = useRef(0);
  const [, setLocation] = useLocation();
  const { toast } = useToast();

  const query = useMemo(() => buildQuery(filters), [filters]);

  const loadOverview = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++requestIdRef.current;
      setState("loading");
      try {
        const [summaryRes, portsRes] = await Promise.all([
          fetch(`/v1/congestion/summary?${query}`, { signal, headers: getAuthHeaders() }),
          fetch(`/v1/congestion/ports?${query}`, { signal, headers: getAuthHeaders() }),
        ]);
        if (!summaryRes.ok || !portsRes.ok) throw new Error("Failed to load congestion");
        const summaryData = await summaryRes.json();
        const portsData = await portsRes.json();
        if (requestId !== requestIdRef.current) return;
        setSummary(summaryData);
        setPorts(Array.isArray(portsData.items) ? portsData.items : []);
        setState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== requestIdRef.current) return;
        setState("error");
      }
    },
    [query]
  );

  const refreshOverview = useCallback(() => {
    const controller = new AbortController();
    loadOverview(controller.signal);
    return controller;
  }, [loadOverview]);

  useEffect(() => {
    const controller = refreshOverview();
    return () => controller.abort();
  }, [refreshOverview]);

  useEffect(() => {
    if (!selectedEntity && ports.length > 0) {
      const top = ports[0];
      setSelectedEntity({
        id: top.portId ?? top.id,
        name: top.portName,
        type: "port",
      });
    }
  }, [ports, selectedEntity, setSelectedEntity]);

  const loadSeries = useCallback(
    async (signal?: AbortSignal) => {
      if (!selectedEntity?.id) return;
      const requestId = ++seriesRequestIdRef.current;
      setSeriesState("loading");
      try {
        const res = await fetch(`/v1/congestion/timeseries?portId=${selectedEntity.id}&${query}`, {
          signal,
          headers: getAuthHeaders(),
        });
        if (!res.ok) throw new Error("Failed to load congestion timeseries");
        const data = await res.json();
        if (requestId !== seriesRequestIdRef.current) return;
        setSeries(Array.isArray(data.series) ? data.series : []);
        setSeriesState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== seriesRequestIdRef.current) return;
        setSeriesState("error");
      }
    },
    [query, selectedEntity?.id]
  );

  const refreshSeries = useCallback(() => {
    const controller = new AbortController();
    loadSeries(controller.signal);
    return controller;
  }, [loadSeries]);

  useEffect(() => {
    if (!selectedEntity?.id) return;
    const controller = refreshSeries();
    return () => controller.abort();
  }, [refreshSeries, selectedEntity?.id]);

  const modeLabel =
    filters.mode === "cross"
      ? "Cross-modal"
      : filters.mode === "sea"
        ? "Sea"
        : filters.mode === "air"
          ? "Air"
          : "Rail";

  const selectedPort = ports.find((port) => (port.portId ?? port.id) === selectedEntity?.id);

  const kpis = summary
    ? [
        { label: "Ports Monitored", value: summary.portsMonitored.toLocaleString() },
        { label: "Congested Ports", value: summary.congestedPorts.toLocaleString() },
        { label: "Avg Wait", value: `${summary.avgWaitHours.toFixed(1)}h` },
        { label: "Max Dwell", value: `${summary.maxDwellHours.toFixed(1)}h` },
        { label: "Top Risk", value: summary.topRiskPort ?? "N/A" },
      ]
    : [];

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
            <MapPin className="h-5 w-5 text-primary" />
            Congestion
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
              <Search className="h-4 w-4 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search port"
                className="h-8 border-none bg-transparent px-0 text-sm shadow-none focus-visible:ring-0"
              />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-emerald-400" />
              Freshness 22s
            </Badge>
            <Badge variant="outline" className="flex items-center gap-2">
              <Target className="h-3.5 w-3.5 text-sky-400" />
              Confidence 0.79
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                const name = window.prompt("Name this view", "Congestion view");
                if (!name) return;
                saveView({
                  name,
                  route: "/congestion",
                  filters,
                  selection: selectedEntity,
                });
                toast({ title: "View saved", description: name });
              }}
            >
              Save view
            </Button>
            <Button variant="outline" size="sm" asChild>
              <a href="/views">Views</a>
            </Button>
            <Button variant="outline" size="sm">
              <Download className="mr-2 h-4 w-4" />
              Export
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto grid w-full max-w-7xl grid-cols-[280px_1fr_320px] gap-6 px-6 py-6">
        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Filters</h2>
            <Button variant="ghost" size="sm" onClick={resetFilters}>
              <RefreshCw className="mr-2 h-4 w-4" />
              Reset
            </Button>
          </div>
          <Card>
            <CardContent className="space-y-4 pt-4">
              <div>
                <label className="text-xs text-muted-foreground">Region</label>
                <Select value={filters.region?.[0] ?? "Global"} onValueChange={(value) => updateFilters({ region: [value] })}>
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Global">Global</SelectItem>
                    <SelectItem value="Atlantic">Atlantic Basin</SelectItem>
                    <SelectItem value="Asia">Asia</SelectItem>
                    <SelectItem value="Europe">Europe</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Origin</label>
                <Input
                  placeholder="Origin"
                  className="mt-2"
                  value={filters.origin?.[0] ?? ""}
                  onChange={(event) => updateFilters({ origin: event.target.value ? [event.target.value] : undefined })}
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Destination</label>
                <Input
                  placeholder="Destination"
                  className="mt-2"
                  value={filters.destination?.[0] ?? ""}
                  onChange={(event) =>
                    updateFilters({ destination: event.target.value ? [event.target.value] : undefined })
                  }
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Hub / Port</label>
                <Input
                  placeholder="NLRTM, Fujairah"
                  className="mt-2"
                  value={filters.hub?.[0] ?? ""}
                  onChange={(event) => updateFilters({ hub: event.target.value ? [event.target.value] : undefined })}
                />
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Risk tags</label>
                <div className="mt-2 flex flex-wrap gap-2">
                  {(filters.riskTags ?? []).map((tag) => (
                    <Badge key={tag} variant="secondary">
                      {tag}
                    </Badge>
                  ))}
                </div>
              </div>
              <Button className="w-full" onClick={refreshOverview}>
                <Filter className="mr-2 h-4 w-4" />
                Apply filters
              </Button>
            </CardContent>
          </Card>
        </aside>

        <section className="space-y-6">
          {state === "error" && (
            <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              <span>Failed to load congestion summary.</span>
              <Button size="sm" variant="outline" onClick={refreshOverview}>
                Retry
              </Button>
            </div>
          )}
          {state === "loading" && !summary && (
            <div className="grid grid-cols-2 gap-4 xl:grid-cols-5">
              {Array.from({ length: 5 }).map((_, idx) => (
                <Card key={`kpi-skel-${idx}`} className="border-border/60">
                  <CardContent className="space-y-2 pt-4">
                    <div className="h-3 w-1/2 animate-pulse rounded bg-muted/30" />
                    <div className="h-5 w-3/4 animate-pulse rounded bg-muted/40" />
                  </CardContent>
                </Card>
              ))}
            </div>
          )}
          {summary && (
            <div className="grid grid-cols-2 gap-4 xl:grid-cols-5">
              {kpis.map((kpi) => (
                <Card key={kpi.label} className="border-border/60">
                  <CardContent className="space-y-2 pt-4">
                    <div className="text-xs text-muted-foreground">{kpi.label}</div>
                    <div className="text-lg font-semibold">{kpi.value}</div>
                  </CardContent>
                </Card>
              ))}
            </div>
          )}

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Congestion Map</CardTitle>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <Globe className="h-4 w-4" />
                {modeLabel} view
              </div>
            </CardHeader>
            <CardContent>
              <CongestionMap ports={ports} />
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Congestion Leaderboard</CardTitle>
              <Button variant="ghost" size="sm">
                Export
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {state === "loading" ? (
                <LoadingState label="Loading ports..." />
              ) : state === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load congestion ports.</span>
                  <Button size="sm" variant="outline" onClick={refreshOverview}>
                    Retry
                  </Button>
                </div>
              ) : ports.length === 0 ? (
                <div className="text-sm text-muted-foreground">No congestion data in this window.</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-muted-foreground">
                        <th className="py-2">Port</th>
                        <th>Vessels</th>
                        <th>Queue</th>
                        <th>Avg Wait</th>
                        <th>Dwell</th>
                        <th>Throughput</th>
                        <th>Risk</th>
                      </tr>
                    </thead>
                    <tbody>
                      {ports.map((port) => (
                        <tr
                          key={port.portId}
                          className={cn(
                            "border-t border-border/40",
                            selectedEntity?.id === port.portId && "bg-primary/10"
                          )}
                          onClick={() =>
                            setSelectedEntity({
                              id: port.portId,
                              name: port.portName,
                              type: "port",
                            })
                          }
                        >
                          <td className="py-2 font-medium">{port.portName}</td>
                          <td>{port.vesselCount}</td>
                          <td>{port.queueCount}</td>
                          <td>{port.avgWaitHours.toFixed(1)}h</td>
                          <td>{port.dwellHours.toFixed(1)}h</td>
                          <td>{port.throughputEstimate ?? 0}</td>
                          <td>{port.riskScore}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Port Drilldown</CardTitle>
              <div className="text-xs text-muted-foreground">
                {selectedPort ? selectedPort.portName : "Select a port"}
              </div>
            </CardHeader>
            <CardContent>
              {seriesState === "loading" ? (
                <LoadingState label="Loading port trends..." />
              ) : seriesState === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load port trends.</span>
                  <Button size="sm" variant="outline" onClick={refreshSeries}>
                    Retry
                  </Button>
                </div>
              ) : series.length === 0 ? (
                <div className="text-sm text-muted-foreground">No timeseries data.</div>
              ) : (
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
                  <div className="h-[200px]">
                    <div className="text-xs text-muted-foreground">Queue over time</div>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={series}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                        <XAxis dataKey="day" tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <YAxis tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <Tooltip />
                        <Line type="monotone" dataKey="queueCount" stroke="#38bdf8" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="h-[200px]">
                    <div className="text-xs text-muted-foreground">Arrivals vs departures</div>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={series}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                        <XAxis dataKey="day" tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <YAxis tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <Tooltip />
                        <Legend />
                        <Line type="monotone" dataKey="arrivals" stroke="#22c55e" strokeWidth={2} dot={false} />
                        <Line type="monotone" dataKey="departures" stroke="#f97316" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                  <div className="h-[200px]">
                    <div className="text-xs text-muted-foreground">Wait time trend</div>
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={series}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                        <XAxis dataKey="day" tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <YAxis tick={{ fill: "#94a3b8", fontSize: 10 }} />
                        <Tooltip />
                        <Line type="monotone" dataKey="waitHours" stroke="#ef4444" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Signal Rail</h2>
            <Badge variant="secondary">{ports.length} tracked</Badge>
          </div>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <Flame className="h-4 w-4 text-rose-400" />
                Why it matters
              </div>
              <p className="text-xs text-muted-foreground">
                Elevated queueing is compressing throughput on critical ports, signaling downstream inventory risk.
              </p>
              <Button
                size="sm"
                variant="outline"
                className="w-full"
                onClick={() => {
                  const investigation = createInvestigation({
                    title: selectedPort ? `Investigation: ${selectedPort.portName}` : "New investigation",
                    sourceRoute: `/congestion?${buildQueryFromFilters(filters, selectedEntity)}`,
                    linkedEntityId: selectedPort?.portId ?? selectedEntity?.id,
                    linkedEntityName: selectedPort?.portName ?? selectedEntity?.name,
                    linkedEntityType: "port",
                  });
                  setLocation(`/investigations/${investigation.id}`);
                }}
              >
                Open investigation
              </Button>
            </CardContent>
          </Card>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <AlertTriangle className="h-4 w-4 text-amber-400" />
                Selected port
              </div>
              <div className="text-xs text-muted-foreground">
                {selectedPort
                  ? `${selectedPort.portName} | Risk ${selectedPort.riskScore}`
                  : "Select a port to view drivers."}
              </div>
              {selectedPort && (
                <div className="text-xs text-muted-foreground">
                  {selectedPort.whyItMatters ?? "Congestion risk elevated versus baseline."}
                </div>
              )}
            </CardContent>
          </Card>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <MapPin className="h-4 w-4 text-sky-400" />
                Explain
              </div>
              <div className="text-xs text-muted-foreground">
                {selectedPort
                  ? `Queue ${selectedPort.queueCount} | Avg wait ${selectedPort.avgWaitHours.toFixed(1)}h`
                  : "Select a port to see congestion drivers."}
              </div>
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}

export default function CongestionPage() {
  return (
    <TerminalStoreProvider basePath="/congestion">
      <CongestionContent />
    </TerminalStoreProvider>
  );
}
