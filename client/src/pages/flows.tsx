import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "wouter";
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
import FlowMap from "@/components/flow-map";
import { saveView } from "@/lib/saved-views";
import { buildSelectionContextLink } from "@/lib/selection-context";
import { useToast } from "@/hooks/use-toast";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, ResponsiveContainer, Tooltip, Legend } from "recharts";
import {
  ArrowLeft,
  ArrowRight,
  Download,
  Filter,
  Flame,
  Globe,
  RefreshCw,
  Search,
  Target,
  Clock,
  AlertTriangle,
  MapPin,
  Activity,
} from "lucide-react";

type FlowLane = {
  id: string;
  originId: string;
  originName: string;
  destinationId: string;
  destinationName: string;
  commodity: string;
  volume: number;
  unit: string;
  deltaPct?: number;
  zScore?: number;
};

type FlowSummary = {
  totalVolume: number;
  importVolume: number;
  exportVolume: number;
  netFlow: number;
  topLaneDelta: number;
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

function FlowsContent() {
  const { filters, updateFilters, resetFilters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const [search, setSearch] = useState("");
  const [summary, setSummary] = useState<FlowSummary | null>(null);
  const [lanes, setLanes] = useState<FlowLane[]>([]);
  const [timeseries, setTimeseries] = useState<{ current: any[]; previous: any[] }>({ current: [], previous: [] });
  const [state, setState] = useState<"idle" | "loading" | "error">("idle");
  const requestIdRef = useRef(0);
  const [, setLocation] = useLocation();
  const { toast } = useToast();

  const query = useMemo(() => buildQuery(filters), [filters]);

  const loadFlows = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++requestIdRef.current;
      setState("loading");
      try {
        const [summaryRes, lanesRes, seriesRes] = await Promise.all([
          fetch(`/v1/flows/summary?${query}`, { signal, headers: getAuthHeaders() }),
          fetch(`/v1/flows/lanes?${query}`, { signal, headers: getAuthHeaders() }),
          fetch(`/v1/flows/timeseries?${query}`, { signal, headers: getAuthHeaders() }),
        ]);
        if (!summaryRes.ok || !lanesRes.ok || !seriesRes.ok) throw new Error("Failed to load flows");
        const summaryData = await summaryRes.json();
        const lanesData = await lanesRes.json();
        const seriesData = await seriesRes.json();
        if (requestId !== requestIdRef.current) return;
        setSummary(summaryData);
        setLanes(Array.isArray(lanesData.items) ? lanesData.items : []);
        setTimeseries(seriesData);
        setState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== requestIdRef.current) return;
        setState("error");
      }
    },
    [query]
  );

  const refreshFlows = useCallback(() => {
    const controller = new AbortController();
    loadFlows(controller.signal);
    return controller;
  }, [loadFlows]);

  useEffect(() => {
    const controller = refreshFlows();
    return () => controller.abort();
  }, [refreshFlows]);

  const modeLabel =
    filters.mode === "cross"
      ? "Cross-modal"
      : filters.mode === "sea"
        ? "Sea"
        : filters.mode === "air"
          ? "Air"
          : "Rail";

  const kpis = summary
    ? [
        { label: "Total Volume", value: `${summary.totalVolume.toLocaleString()} bbl` },
        { label: "Import Volume", value: `${summary.importVolume.toLocaleString()} bbl` },
        { label: "Export Volume", value: `${summary.exportVolume.toLocaleString()} bbl` },
        { label: "Net Flow", value: `${summary.netFlow.toLocaleString()} bbl` },
        { label: "Top Lane Delta", value: `${summary.topLaneDelta.toFixed(1)}%` },
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
            <Globe className="h-5 w-5 text-primary" />
            Flows
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
                placeholder="Search lane, port, company"
                className="h-8 border-none bg-transparent px-0 text-sm shadow-none focus-visible:ring-0"
              />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-emerald-400" />
              Freshness 30s
            </Badge>
            <Badge variant="outline" className="flex items-center gap-2">
              <Target className="h-3.5 w-3.5 text-sky-400" />
              Confidence 0.78
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                const name = window.prompt("Name this view", "Flows view");
                if (!name) return;
                saveView({
                  name,
                  route: "/flows",
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
              <Button className="w-full" onClick={refreshFlows}>
                <Filter className="mr-2 h-4 w-4" />
                Apply filters
              </Button>
            </CardContent>
          </Card>
        </aside>

        <section className="space-y-6">
          {state === "error" && (
            <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
              <span>Failed to load flow summary.</span>
              <Button size="sm" variant="outline" onClick={refreshFlows}>
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
              <CardTitle className="text-sm font-semibold">Flow Map</CardTitle>
              <div className="flex items-center gap-2 text-xs text-muted-foreground">
                <Globe className="h-4 w-4" />
                {modeLabel} view
              </div>
            </CardHeader>
            <CardContent>
              <FlowMap />
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Flow Timeseries</CardTitle>
              <Button variant="ghost" size="sm">
                View details
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent className="h-[280px]">
              {state === "loading" ? (
                <LoadingState label="Loading timeseries..." />
              ) : state === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load timeseries.</span>
                  <Button size="sm" variant="outline" onClick={refreshFlows}>
                    Retry
                  </Button>
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={timeseries.current.map((point, idx) => ({
                    day: point.day,
                    current: point.volume,
                    previous: timeseries.previous[idx]?.volume ?? 0,
                  }))}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                    <XAxis dataKey="day" tick={{ fill: "#94a3b8", fontSize: 10 }} />
                    <YAxis tick={{ fill: "#94a3b8", fontSize: 10 }} />
                    <Tooltip />
                    <Legend />
                    <Line type="monotone" dataKey="current" stroke="#38bdf8" strokeWidth={2} dot={false} />
                    <Line type="monotone" dataKey="previous" stroke="#94a3b8" strokeDasharray="4 4" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </CardContent>
          </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Top Lanes</CardTitle>
              <Button variant="ghost" size="sm">
                Export
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {state === "loading" ? (
                <LoadingState label="Loading lanes..." />
              ) : state === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load lanes.</span>
                  <Button size="sm" variant="outline" onClick={refreshFlows}>
                    Retry
                  </Button>
                </div>
              ) : lanes.length === 0 ? (
                <div className="text-sm text-muted-foreground">No lanes in this window. Adjust filters to widen coverage.</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-muted-foreground">
                        <th className="py-2">Origin</th>
                        <th>Destination</th>
                        <th>Commodity</th>
                        <th>Volume</th>
                        <th>Delta %</th>
                        <th>Z-score</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lanes.map((lane) => (
                        <tr
                          key={lane.id}
                          className={cn(
                            "border-t border-border/40",
                            selectedEntity?.id === lane.id && "bg-primary/10"
                          )}
                          onClick={() =>
                            setSelectedEntity({
                              id: lane.id,
                              name: `${lane.originName} -> ${lane.destinationName}`,
                              type: "lane",
                            })
                          }
                        >
                          <td className="py-2">{lane.originName}</td>
                          <td>{lane.destinationName}</td>
                          <td>{lane.commodity}</td>
                          <td>{lane.volume.toLocaleString()} {lane.unit}</td>
                          <td>{lane.deltaPct?.toFixed(1) ?? "0.0"}%</td>
                          <td>{lane.zScore?.toFixed(2) ?? "0.00"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Signal Rail</h2>
            <Badge variant="secondary">{lanes.length} active</Badge>
          </div>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <Flame className="h-4 w-4 text-rose-400" />
                Why it matters
              </div>
              <p className="text-xs text-muted-foreground">
                Cross-basin volumes are elevated with stronger divergence versus prior period.
                Focus on corridors with high delta and elevated z-score.
              </p>
              <Button
                size="sm"
                variant="outline"
                className="w-full"
                onClick={() => {
                  const investigation = createInvestigation({
                    title: selectedEntity ? `Investigation: ${selectedEntity.name}` : "New investigation",
                    sourceRoute: `/flows?${buildQueryFromFilters(filters, selectedEntity)}`,
                    linkedEntityId: selectedEntity?.id,
                    linkedEntityName: selectedEntity?.name,
                    linkedEntityType: selectedEntity?.type,
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
                <Activity className="h-4 w-4 text-sky-400" />
                Selected entity
              </div>
              <div className="text-xs text-muted-foreground">
                {selectedEntity
                  ? `${selectedEntity.name} (${selectedEntity.type})`
                  : "Select a lane or map node to focus analysis."}
              </div>
              <Button
                size="sm"
                className="w-full"
                disabled={!selectedEntity}
                onClick={() => setLocation(buildSelectionContextLink(selectedEntity, filters, "/flows"))}
              >
                View detail
              </Button>
            </CardContent>
          </Card>
          <Card className="border-border/60">
            <CardContent className="space-y-2 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <AlertTriangle className="h-4 w-4 text-amber-400" />
                Latest anomalies
              </div>
              {lanes.slice(0, 3).map((lane) => (
                <div key={lane.id} className="text-xs text-muted-foreground">
                  {lane.originName} &rarr; {lane.destinationName} ({lane.deltaPct?.toFixed(1) ?? "0.0"}%)
                </div>
              ))}
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}

export default function FlowsPage() {
  return (
    <TerminalStoreProvider basePath="/flows">
      <FlowsContent />
    </TerminalStoreProvider>
  );
}
