import { useCallback, useEffect, useRef, useState } from "react";
import { Link, useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { cn } from "@/lib/utils";
import TerminalMap from "@/components/terminal-map";
import { TerminalStoreProvider, useTerminalStore, buildQueryFromFilters } from "@/hooks/use-terminal-store";
import { createInvestigation } from "@/lib/investigations";
import { saveView } from "@/lib/saved-views";
import { buildSelectionContextLink } from "@/lib/selection-context";
import { useToast } from "@/hooks/use-toast";
import {
  Activity,
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  ChevronDown,
  Clock,
  Download,
  Filter,
  Flame,
  Globe,
  MapPin,
  Radar,
  RefreshCw,
  Search,
  Target,
} from "lucide-react";

const KPI_STRIP = [
  { label: "Exports", value: "18.4 MMbbl", trend: "+5.1%" },
  { label: "Imports", value: "16.2 MMbbl", trend: "+2.4%" },
  { label: "Net Balance", value: "+2.2 MMbbl", trend: "+1.3%" },
  { label: "Dwell Time", value: "24.3 days", trend: "-0.8%" },
  { label: "On-time", value: "92.1%", trend: "+0.7%" },
  { label: "Risk", value: "Moderate", trend: "Stable" },
];

type TopSignal = {
  id: string;
  entityId: string;
  entityName: string;
  entityType: "port" | "corridor" | "zone";
  metric: string;
  value: number;
  delta: number;
  zScore?: number;
  severity?: "low" | "medium" | "high";
  whyItMatters?: string;
};

const COMMODITIES = ["Crude", "Products", "LNG", "LPG", "Dry Bulk", "Metals", "Agri"];
const MODES = ["Sea", "Air", "Rail", "Cross-modal"];
const TIME_MODES = ["Live", "24h", "7d", "30d"];

const toDayString = (date: Date) => date.toISOString().slice(0, 10);
const daysAgo = (days: number) => {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);
  return toDayString(date);
};

function TerminalContent() {
  const { filters, updateFilters, resetFilters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const [search, setSearch] = useState("");
  const [topSignals, setTopSignals] = useState<TopSignal[]>([]);
  const [signalsState, setSignalsState] = useState<"idle" | "loading" | "error">("idle");
  const requestIdRef = useRef(0);
  const [, setLocation] = useLocation();
  const { toast } = useToast();

  const loadSignals = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++requestIdRef.current;
      setSignalsState("loading");
      try {
        const params = new URLSearchParams();
        params.set("include_entity", "true");
        params.set("clustered", "true");
        params.set("severity_min", "HIGH");
        params.set("limit", "10");

        if (filters.timeMode === "range") {
          const window = filters.timeWindow ?? "7d";
          const rangeDays = window === "24h" ? 1 : window === "30d" ? 30 : 7;
          params.set("day_from", daysAgo(rangeDays));
          params.set("day_to", daysAgo(0));
        }

        const hub = filters.hub?.[0];
        if (hub) {
          params.set("port", hub);
        }

        const res = await fetch(`/v1/signals?${params.toString()}`, { signal });
        if (!res.ok) throw new Error("Failed to load signals");
        const data = await res.json();
        const items = Array.isArray(data.items) ? data.items : [];
        const mapped: TopSignal[] = items.map((item: any) => ({
          id: item.id,
          entityId: item.entity_id ?? item.entity?.id ?? "unknown",
          entityName: item.entity?.name ?? item.entity_id ?? "Unknown",
          entityType: item.entity_type ?? "port",
          metric: item.signal_type ?? "signal",
          value: item.value ?? 0,
          delta: item.delta_pct ?? 0,
          zScore: item.zscore ?? undefined,
          severity: (item.cluster_severity ?? item.severity ?? "high").toLowerCase(),
          whyItMatters: item.cluster_summary ?? item.explanation?.split("\n")[0],
        }));
        if (requestId !== requestIdRef.current) return;
        setTopSignals(mapped);
        setSignalsState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== requestIdRef.current) return;
        setSignalsState("error");
      }
    },
    [filters.hub, filters.timeMode, filters.timeWindow]
  );

  const refreshSignals = useCallback(() => {
    const controller = new AbortController();
    loadSignals(controller.signal);
    return controller;
  }, [loadSignals]);

  useEffect(() => {
    const controller = refreshSignals();
    return () => controller.abort();
  }, [refreshSignals]);

  const handleSignalSelect = (signal: TopSignal) => {
    setSelectedEntity({
      id: signal.entityId,
      name: signal.entityName,
      type: signal.entityType ?? "unknown",
    });
  };

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
            <Radar className="h-5 w-5 text-primary" />
            Terminal
          </div>
          <div className="flex flex-1 flex-wrap items-center gap-3">
            <Select
              value={filters.commodity?.[0] ?? "Crude"}
              onValueChange={(value) => updateFilters({ commodity: [value] })}
            >
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
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="outline" size="sm" className="h-9 gap-2">
                  Terminal modules
                  <ChevronDown className="h-4 w-4" />
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start">
                <DropdownMenuItem asChild>
                  <Link href="/congestion">Congestion</Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/alerts">Alerts</Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/command">Command</Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/investigations">Investigations</Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/views">Views</Link>
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
            <div className="flex flex-1 items-center gap-2 rounded-md border border-border bg-background px-2">
              <Search className="h-4 w-4 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Search entity, port, company"
                className="h-8 border-none bg-transparent px-0 text-sm shadow-none focus-visible:ring-0"
              />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-emerald-400" />
              Freshness 12s
            </Badge>
            <Badge variant="outline" className="flex items-center gap-2">
              <Target className="h-3.5 w-3.5 text-sky-400" />
              Confidence 0.82
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                const name = window.prompt("Name this view", "Terminal view");
                if (!name) return;
                saveView({
                  name,
                  route: "/terminal",
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
        {/* Filters */}
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
                <Select
                  value={filters.region?.[0] ?? "Global"}
                  onValueChange={(value) => updateFilters({ region: value ? [value] : undefined })}
                >
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
                  onChange={(event) =>
                    updateFilters({ origin: event.target.value ? [event.target.value] : undefined })
                  }
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
                  onChange={(event) =>
                    updateFilters({ hub: event.target.value ? [event.target.value] : undefined })
                  }
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
                  {(filters.riskTags ?? []).length === 0 && (
                    <span className="text-xs text-muted-foreground">None selected</span>
                  )}
                </div>
              </div>
              <Button className="w-full" onClick={refreshSignals}>
                <Filter className="mr-2 h-4 w-4" />
                Apply filters
              </Button>
            </CardContent>
          </Card>
        </aside>

        {/* Main body */}
        <section className="space-y-6">
          <div className="grid grid-cols-3 gap-4 xl:grid-cols-6">
            {KPI_STRIP.map((kpi) => (
              <Card key={kpi.label} className="border-border/60">
                <CardContent className="space-y-2 pt-4">
                  <div className="text-xs text-muted-foreground">{kpi.label}</div>
                  <div className="text-lg font-semibold">{kpi.value}</div>
                  <div className="text-xs text-muted-foreground">{kpi.trend}</div>
                </CardContent>
              </Card>
            ))}
          </div>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Global Operating Map</CardTitle>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <Globe className="h-4 w-4" />
              {modeLabel} view
            </div>
          </CardHeader>
          <CardContent>
            <TerminalMap />
          </CardContent>
        </Card>

          <Card className="border-border/60">
            <CardHeader className="flex-row items-center justify-between space-y-0">
              <CardTitle className="text-sm font-semibold">Top Signals</CardTitle>
              <Button variant="ghost" size="sm">
                View all
                <ArrowRight className="ml-2 h-4 w-4" />
              </Button>
            </CardHeader>
            <CardContent>
              {signalsState === "loading" ? (
                <div className="space-y-2 rounded-md border border-border/50 bg-background/40 p-3">
                  <div className="h-4 w-1/3 animate-pulse rounded bg-muted/40" />
                  <div className="h-3 w-2/3 animate-pulse rounded bg-muted/30" />
                </div>
              ) : signalsState === "error" ? (
                <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                  <span>Failed to load signals.</span>
                  <Button size="sm" variant="outline" onClick={refreshSignals}>
                    Retry
                  </Button>
                </div>
              ) : topSignals.length === 0 ? (
                <div className="rounded-md border border-border/50 bg-background/40 p-3 text-sm text-muted-foreground">
                  No signals in this window. Try adjusting filters.
                </div>
              ) : (
                <div className="space-y-3">
                  {topSignals.map((signal) => (
                    <button
                      key={signal.id}
                      className="flex w-full items-center justify-between rounded-md border border-border/50 bg-background/40 p-3 text-left transition hover:border-primary/50"
                      onClick={() => handleSignalSelect(signal)}
                    >
                      <div>
                        <div className="text-sm font-medium">{signal.entityName}</div>
                        <div className="text-xs text-muted-foreground">{signal.whyItMatters ?? signal.metric}</div>
                      </div>
                      <Badge
                        variant="outline"
                        className={cn(
                          "text-xs",
                          signal.severity === "high"
                            ? "border-red-500/40 text-red-400"
                            : "border-amber-500/40 text-amber-300"
                        )}
                      >
                        {signal.zScore ? `${signal.zScore.toFixed(2)} sigma` : `${signal.delta.toFixed(1)}%`}
                      </Badge>
                    </button>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        {/* Signal rail */}
        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Signal Rail</h2>
            <Badge variant="secondary">{topSignals.length} new</Badge>
          </div>
          <div className="space-y-3">
            {topSignals.slice(0, 3).map((signal) => (
              <Card key={signal.id} className="border-border/60">
                <CardContent className="space-y-2 pt-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2 text-sm font-semibold">
                      <AlertTriangle className="h-4 w-4 text-amber-400" />
                      {signal.entityName}
                    </div>
                    <Badge variant="outline" className="text-xs">
                      {(signal.severity ?? "high").toUpperCase()}
                    </Badge>
                  </div>
                  <div className="text-xs text-muted-foreground">{signal.whyItMatters ?? signal.metric}</div>
                  <div className="flex items-center gap-2 text-xs text-muted-foreground">
                    <MapPin className="h-3.5 w-3.5" />
                    {signal.entityType} | {signal.zScore ? `${signal.zScore.toFixed(2)} sigma` : `${signal.delta.toFixed(1)}%`}
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <Flame className="h-4 w-4 text-rose-400" />
                Why it matters
              </div>
              <p className="text-xs text-muted-foreground">
                Congestion and diversion risk are elevating delivery uncertainty across Europe-bound routes.
                Consider tightening inventory buffers for the next 10-14 days.
              </p>
              <Button
                size="sm"
                variant="outline"
                className="w-full"
                onClick={() => {
                  const investigation = createInvestigation({
                    title: selectedEntity ? `Investigation: ${selectedEntity.name}` : "New investigation",
                    sourceRoute: `/terminal?${buildQueryFromFilters(filters, selectedEntity)}`,
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
                  : "Select a signal or map marker to focus analysis."}
              </div>
              <Button
                size="sm"
                className="w-full"
                disabled={!selectedEntity}
                onClick={() => setLocation(buildSelectionContextLink(selectedEntity, filters, "/terminal"))}
              >
                View detail
              </Button>
            </CardContent>
          </Card>
        </aside>
      </div>
    </div>
  );
}

export default function TerminalPage() {
  return (
    <TerminalStoreProvider basePath="/terminal">
      <TerminalContent />
    </TerminalStoreProvider>
  );
}
