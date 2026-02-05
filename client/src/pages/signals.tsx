import { useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { METRIC_LABELS, formatMetricValue } from "@shared/metrics";
import { cn } from "@/lib/utils";
import { AlertTriangle, ArrowLeft, Clock, MapPin, Signal, SlidersHorizontal } from "lucide-react";
import { Link } from "wouter";

type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";

type ExplainDriver = {
  metric: string;
  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;
};

type Explainability = {
  drivers: ExplainDriver[];
  impact: string[];
  followups: string[];
};

type DataQuality = {
  history_days_used: number;
  completeness_pct: number;
  missing_points: number;
};

type SignalEntity = {
  id: string;
  type: "port";
  name: string;
  code: string;
  unlocode: string;
};

type SignalDTO = {
  id: string;
  signal_type: string;
  entity_type: string;
  entity_id: string;
  day: string;
  entity?: SignalEntity;
  severity: Severity;
  value: number;
  baseline?: number | null;
  stddev?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;
  confidence_score?: number | null;
  confidence_band?: "LOW" | "MEDIUM" | "HIGH" | null;
  method?: string | null;
  cluster_id?: string | null;
  cluster_key?: string | null;
  cluster_type?: string | null;
  cluster_severity?: Severity | null;
  cluster_summary?: string | null;
  explanation: string;
  explainability?: Explainability | null;
  data_quality?: DataQuality | null;
  created_at?: string | null;
};

type SignalsState = {
  loading: boolean;
  error?: string;
  items: SignalDTO[];
  filters: {
    day?: string;
    port?: string;
    severity_min: Severity;
    clustered: boolean;
  };
};

const SEVERITY_OPTIONS: Severity[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];

const SEVERITY_STYLES: Record<Severity, string> = {
  LOW: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  MEDIUM: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  HIGH: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  CRITICAL: "bg-red-500/10 text-red-400 border-red-500/30",
};

const METHOD_LABELS: Record<string, string> = {
  zscore_30d: "Z-score (30d)",
  multiplier_30d: "Multiplier (30d)",
};

const DOT = "\u00B7";

const CLUSTER_LABELS: Record<string, string> = {
  PORT_DISRUPTION: "Port disruption",
};

const formatPercent = (value?: number | null) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  const prefix = value >= 0 ? "+" : "";
  return `${prefix}${value.toFixed(1)}%`;
};

const formatNumber = (value?: number | null, digits = 2) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  return value.toFixed(digits);
};

const formatBaseline = (value?: number | null) => {
  if (value === null || value === undefined || Number.isNaN(value)) return "—";
  if (Math.abs(value) < 1) return "<1";
  return value.toFixed(2);
};

const formatRelativeTime = (value?: string | null) => {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "—";
  const delta = Date.now() - date.getTime();
  const minutes = Math.floor(delta / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
};

const getSummaryLine = (signal: SignalDTO) => {
  if (signal.cluster_summary) return signal.cluster_summary;
  return signal.explanation?.split("\n")[0] ?? "Signal update";
};

const getEntityLabel = (signal: SignalDTO) => {
  if (signal.entity) {
    return `${signal.entity.name} (${signal.entity.unlocode || signal.entity.code})`;
  }
  return signal.entity_id;
};

const buildQueryString = (filters: SignalsState["filters"]) => {
  const params = new URLSearchParams();
  params.set("clustered", filters.clustered ? "true" : "false");
  params.set("include_entity", "true");
  if (filters.day) params.set("day", filters.day);
  if (filters.port) params.set("port", filters.port);
  if (filters.severity_min) params.set("severity_min", filters.severity_min);
  return params.toString();
};

const parseFiltersFromUrl = (location: string): SignalsState["filters"] => {
  const search = location.split("?")[1] ?? "";
  const params = new URLSearchParams(search);
  const day = params.get("day") ?? undefined;
  const port = params.get("port") ?? undefined;
  const severityParam = (params.get("severity_min") ?? "HIGH") as Severity;
  const severity = SEVERITY_OPTIONS.includes(severityParam) ? severityParam : "HIGH";
  const clustered = params.get("clustered") !== "false";
  return { day, port, severity_min: severity, clustered };
};

export default function SignalsPage() {
  const [location, setLocation] = useLocation();
  const initialFilters = useMemo(() => parseFiltersFromUrl(location), [location]);
  const [state, setState] = useState<SignalsState>({
    loading: true,
    error: undefined,
    items: [],
    filters: initialFilters,
  });
  const [selectedSignal, setSelectedSignal] = useState<SignalDTO | undefined>(undefined);
  const syncRef = useRef(false);

  useEffect(() => {
    if (syncRef.current) {
      syncRef.current = false;
      return;
    }
    setState((prev) => ({ ...prev, filters: parseFiltersFromUrl(location) }));
  }, [location]);

  useEffect(() => {
    const query = buildQueryString(state.filters);
    syncRef.current = true;
    const target = `/signals?${query}`;
    if (location !== target) {
      setLocation(target);
    }
  }, [state.filters, setLocation, location]);

  useEffect(() => {
    const controller = new AbortController();
    const fetchSignals = async () => {
      setState((prev) => ({ ...prev, loading: true, error: undefined }));
      try {
        const query = buildQueryString(state.filters);
        const response = await fetch(`/v1/signals?${query}`, { signal: controller.signal });
        if (!response.ok) {
          throw new Error("Failed to load signals");
        }
        const payload = await response.json();
        const items = Array.isArray(payload.items) ? payload.items : [];
        setState((prev) => ({ ...prev, loading: false, items }));
      } catch (error: any) {
        if (error?.name === "AbortError") return;
        setState((prev) => ({ ...prev, loading: false, error: error?.message ?? "Unable to load signals" }));
      }
    };
    fetchSignals();
    return () => controller.abort();
  }, [state.filters]);

  useEffect(() => {
    if (!selectedSignal) return;
    const stillExists = state.items.find((item) => item.id === selectedSignal.id);
    if (!stillExists) {
      setSelectedSignal(undefined);
    }
  }, [state.items, selectedSignal]);

  const filters = state.filters;

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <div className="flex items-center gap-3">
                <Link href="/platform">
                  <Button variant="ghost" size="sm">
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    Back to Menu
                  </Button>
                </Link>
              </div>
              <div className="mt-2 flex items-center gap-3">
                <Signal className="h-6 w-6 text-primary" />
                <h1 className="text-2xl font-semibold text-foreground">Signals Feed</h1>
              </div>
              <p className="text-sm text-muted-foreground">
                Clustered market events with explainability and data quality.
              </p>
            </div>
            <div className="flex items-center gap-2 text-xs text-muted-foreground">
              <SlidersHorizontal className="h-4 w-4" />
              Filters update the feed instantly for demo-ready views.
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8">
        <Card className="mb-6 border-border/60 bg-card/70">
          <CardContent className="grid gap-4 py-5 lg:grid-cols-[1.2fr_1.4fr_1fr_auto] lg:items-end">
            <div>
              <label className="text-xs font-medium text-muted-foreground">Day</label>
              <Input
                type="date"
                value={filters.day ?? ""}
                onChange={(event) =>
                  setState((prev) => ({
                    ...prev,
                    filters: { ...prev.filters, day: event.target.value || undefined },
                  }))
                }
                className="mt-2"
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Port</label>
              <Input
                placeholder="NLRTM or Rotterdam"
                value={filters.port ?? ""}
                onChange={(event) =>
                  setState((prev) => ({
                    ...prev,
                    filters: { ...prev.filters, port: event.target.value || undefined },
                  }))
                }
                className="mt-2"
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Severity</label>
              <Select
                value={filters.severity_min}
                onValueChange={(value) =>
                  setState((prev) => ({
                    ...prev,
                    filters: { ...prev.filters, severity_min: value as Severity },
                  }))
                }
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select severity" />
                </SelectTrigger>
                <SelectContent>
                  {SEVERITY_OPTIONS.map((severity) => (
                    <SelectItem key={severity} value={severity}>
                      {severity}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center justify-between rounded-lg border border-border/70 px-4 py-3">
              <div>
                <p className="text-xs font-medium text-muted-foreground">Clustered</p>
                <p className="text-sm text-foreground">{filters.clustered ? "On" : "Off"}</p>
              </div>
              <Switch
                checked={filters.clustered}
                onCheckedChange={(checked) =>
                  setState((prev) => ({
                    ...prev,
                    filters: { ...prev.filters, clustered: checked },
                  }))
                }
              />
            </div>
          </CardContent>
        </Card>

        {state.loading ? (
          <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center text-muted-foreground">
            Loading signals…
          </div>
        ) : state.error ? (
          <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-10 text-center text-sm text-destructive">
            Unable to load signals. Please retry.
          </div>
        ) : state.items.length === 0 ? (
          <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center">
            <AlertTriangle className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground">No high-severity signals for this day.</p>
          </div>
        ) : (
          <div className="grid gap-4">
            {state.items.map((signal) => {
              const severity = signal.cluster_severity ?? signal.severity;
              const confidenceScore = signal.confidence_score ?? 0;
              const confidenceBand = signal.confidence_band ?? "LOW";
              const quality = signal.data_quality;
              const createdAt = signal.created_at ?? "";

              return (
                <Card
                  key={signal.id}
                  className="cursor-pointer border-border/60 bg-card/70 transition hover:border-primary/50"
                  onClick={() => setSelectedSignal(signal)}
                  data-testid={`cluster-card-${signal.id}`}
                >
                  <CardContent className="grid gap-4 p-6 lg:grid-cols-[1.5fr_1fr] lg:items-center">
                    <div className="space-y-3">
                      <div className="flex flex-wrap items-center gap-3">
                        <Badge className={cn("border text-xs", SEVERITY_STYLES[severity])}>{severity}</Badge>
                        <span className="text-xs text-muted-foreground">
                          {CLUSTER_LABELS[signal.cluster_type ?? ""] ?? signal.cluster_type ?? "Event"}
                        </span>
                      </div>
                      <div>
                        <h3 className="text-lg font-semibold text-foreground">
                          {CLUSTER_LABELS[signal.cluster_type ?? ""] ?? "Port disruption"} — {getEntityLabel(signal)}
                        </h3>
                        <p className="text-sm text-muted-foreground">{getSummaryLine(signal)}</p>
                      </div>
                      <div className="flex flex-wrap items-center gap-4 text-xs text-muted-foreground">
                        <div className="flex items-center gap-2">
                          <MapPin className="h-3.5 w-3.5" />
                          <span>{signal.entity?.unlocode ?? signal.entity_id}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <Clock className="h-3.5 w-3.5" />
                          <span title={createdAt}>{formatRelativeTime(createdAt)}</span>
                        </div>
                      </div>
                    </div>
                    <div className="grid gap-3 rounded-xl border border-border/50 bg-muted/20 p-4">
                      <div className="flex items-center justify-between text-sm">
                        <span className="text-muted-foreground">Confidence</span>
                        <span className="font-semibold text-foreground">
                          {confidenceBand} {DOT} {(confidenceScore * 100).toFixed(0)}%
                        </span>
                      </div>
                      <div className="text-xs text-muted-foreground">
                        {quality
                          ? `History ${quality.history_days_used}d ${DOT} Completeness ${quality.completeness_pct}% ${DOT} Missing ${quality.missing_points}`
                          : "Data quality unavailable"}
                      </div>
                    </div>
                  </CardContent>
                </Card>
              );
            })}
          </div>
        )}
      </div>

      <Sheet open={Boolean(selectedSignal)} onOpenChange={(open) => !open && setSelectedSignal(undefined)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
          {selectedSignal && (
            <div className="space-y-6">
              <SheetHeader className="space-y-3">
                <SheetTitle className="text-2xl">{getEntityLabel(selectedSignal)}</SheetTitle>
                <div className="flex flex-wrap items-center gap-3 text-sm">
                  <Badge className={cn("border text-xs", SEVERITY_STYLES[selectedSignal.cluster_severity ?? selectedSignal.severity])}>
                    {selectedSignal.cluster_severity ?? selectedSignal.severity}
                  </Badge>
                  <span className="text-muted-foreground">{selectedSignal.day}</span>
                  <span className="text-muted-foreground">
                    {(selectedSignal.confidence_band ?? "LOW")} {DOT} {(selectedSignal.confidence_score ?? 0) * 100}%
                  </span>
                </div>
                {selectedSignal.method && (
                  <div className="text-xs text-muted-foreground">
                    Method: {METHOD_LABELS[selectedSignal.method] ?? selectedSignal.method}
                  </div>
                )}
              </SheetHeader>

              <div className="rounded-xl border border-border/60 bg-card/70 p-5">
                <h4 className="text-sm font-semibold text-foreground mb-2">What happened</h4>
                <p className="text-sm text-muted-foreground">
                  {selectedSignal.explanation?.split("\n")[0]}
                </p>
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-5">
                <h4 className="text-sm font-semibold text-foreground mb-4">Drivers</h4>
                {selectedSignal.explainability?.drivers?.length ? (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="text-xs text-muted-foreground">
                          <th className="text-left pb-2 font-medium">Metric</th>
                          <th className="text-left pb-2 font-medium">Value</th>
                          <th className="text-left pb-2 font-medium">Baseline</th>
                          <th className="text-left pb-2 font-medium">Δ%</th>
                          <th className="text-left pb-2 font-medium">Z</th>
                          <th className="text-left pb-2 font-medium">σ</th>
                        </tr>
                      </thead>
                      <tbody className="text-foreground">
                        {selectedSignal.explainability.drivers.map((driver, index) => (
                          <tr key={`${driver.metric}-${index}`} className="border-t border-border/40">
                            <td className="py-2 pr-3">
                              {METRIC_LABELS[driver.metric] ?? driver.metric}
                            </td>
                            <td className="py-2 pr-3">{formatMetricValue(driver.metric, driver.value)}</td>                            <td className="py-2 pr-3">{formatBaseline(driver.baseline ?? null)}</td>
                            <td className="py-2 pr-3">{formatPercent(driver.delta_pct)}</td>
                            <td className="py-2 pr-3">{formatNumber(driver.zscore, 2)}</td>
                            <td className="py-2">{formatNumber(driver.stddev, 2)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No drivers available.</p>
                )}
              </div>

              <div className="grid gap-4 md:grid-cols-2">
                <div className="rounded-xl border border-border/60 bg-card/70 p-5">
                  <h4 className="text-sm font-semibold text-foreground mb-3">Impact</h4>
                  {selectedSignal.explainability?.impact?.length ? (
                    <ul className="space-y-2 text-sm text-muted-foreground">
                      {selectedSignal.explainability.impact.map((item, index) => (
                        <li key={`impact-${index}`}>• {item}</li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-sm text-muted-foreground">No impact notes.</p>
                  )}
                </div>
                <div className="rounded-xl border border-border/60 bg-card/70 p-5">
                  <h4 className="text-sm font-semibold text-foreground mb-3">Follow-ups</h4>
                  {selectedSignal.explainability?.followups?.length ? (
                    <div className="flex flex-wrap gap-2">
                      {selectedSignal.explainability.followups.map((item, index) => (
                        <Badge key={`followup-${index}`} variant="secondary">
                          {item}
                        </Badge>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">No follow-ups listed.</p>
                  )}
                </div>
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-5">
                <h4 className="text-sm font-semibold text-foreground mb-3">Data quality</h4>
                {selectedSignal.data_quality ? (
                  <div className="grid gap-2 text-sm text-muted-foreground">
                    <div>History days: {selectedSignal.data_quality.history_days_used}</div>
                    <div>Completeness: {selectedSignal.data_quality.completeness_pct}%</div>
                    <div>Missing points: {selectedSignal.data_quality.missing_points}</div>
                  </div>
                ) : (
                  <p className="text-sm text-muted-foreground">No data quality metrics.</p>
                )}
              </div>

              <Button variant="outline" className="w-full" onClick={() => setSelectedSignal(undefined)}>
                Close
              </Button>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
