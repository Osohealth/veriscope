import { useEffect, useMemo, useState } from "react";
import { useLocation } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";
import { apiFetchJson } from "@/lib/apiFetch";
import { AlertTriangle, ArrowLeft, Clock, RefreshCcw } from "lucide-react";
import { Link } from "wouter";

type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
type ConfidenceBand = "LOW" | "MEDIUM" | "HIGH";

type AlertPayload = {
  event_type: string;
  day: string;
  entity_type: string;
  entity_id: string;
  cluster_id: string | null;
  cluster_severity: Severity | null;
  confidence_score: number | null;
  confidence_band: ConfidenceBand | null;
  cluster_summary: string | null;
  top_drivers: Array<{ metric: string; value: number; baseline?: number | null; zscore?: number | null; delta_pct?: number | null }>;
  impact: string[];
  followups: string[];
  data_quality: {
    history_days_used: number;
    completeness_pct: number;
    missing_points: number;
  } | null;
};

type AlertDelivery = {
  id: string;
  run_id: string;
  subscription_id: string;
  cluster_id: string;
  scope?: "PORT" | "GLOBAL";
  cluster_type?: string | null;
  cluster_summary?: string | null;
  cluster_severity?: Severity | null;
  confidence_score?: number | null;
  confidence_band?: ConfidenceBand | null;
  method?: string | null;
  entity_type: string;
  entity_id: string;
  day: string;
  destination_type: string;
  endpoint: string;
  status: string;
  dlq_pending?: boolean;
  dlq_terminal?: boolean;
  attempts: number;
  last_http_status?: number | null;
  latency_ms?: number | null;
  error?: string | null;
  sent_at?: string | null;
  created_at?: string | null;
  entity?: {
    id: string;
    type: "port";
    name: string;
    code: string;
    unlocode: string;
  };
  alert_payload?: AlertPayload | null;
  attempt_history?: Array<{
    attempt_no: number;
    status: string;
    latency_ms: number | null;
    http_status: number | null;
    error: string | null;
    sent_at: string | null;
    created_at: string | null;
  }>;
};

type AlertMetrics = {
  version: string;
  days: number;
  delivery_health: Array<{
    day: string;
    success_rate: number | null;
    failed: number;
    deduped: number;
    rate_limited: number;
  }>;
  latency: { p50: number | null; p90: number | null; p99: number | null };
  endpoint_health: Array<{ endpoint: string; failed: number; sent: number }>;
};


const SEVERITY_OPTIONS: Severity[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
const STATUS_OPTIONS = ["ALL", "SENT", "FAILED", "SKIPPED", "DLQ"] as const;
const DESTINATION_OPTIONS = ["ALL", "WEBHOOK", "EMAIL"] as const;

const SEVERITY_STYLES: Record<Severity, string> = {
  LOW: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  MEDIUM: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  HIGH: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  CRITICAL: "bg-red-500/10 text-red-400 border-red-500/30",
};

const STATUS_STYLES: Record<string, string> = {
  SENT: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  FAILED: "bg-red-500/10 text-red-400 border-red-500/30",
  SKIPPED: "bg-slate-500/10 text-slate-300 border-slate-500/30",
  DLQ: "bg-orange-500/10 text-orange-400 border-orange-500/30",
};

const DOT = "\u00B7";
const DEMO_USER_ID = "00000000-0000-0000-0000-000000000001";

const formatRelativeTime = (value?: string | null) => {
  if (!value) return "--";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "--";
  const delta = Date.now() - date.getTime();
  const minutes = Math.floor(delta / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
};

const buildQueryString = (filters: Filters, cursor?: string | null) => {
  const params = new URLSearchParams();
  params.set("days", String(filters.days));
  if (filters.port) params.set("port", filters.port);
  if (filters.status !== "ALL") params.set("status", filters.status);
  if (filters.destination !== "ALL") params.set("destination", filters.destination);
  if (filters.severity_min) params.set("severity_min", filters.severity_min);
  if (filters.subscription_id) params.set("subscription_id", filters.subscription_id);
  params.set("limit", "50");
  if (cursor) params.set("cursor", cursor);
  params.set("include_entity", "true");
  return params.toString();
};

type Filters = {
  days: number;
  status: typeof STATUS_OPTIONS[number];
  destination: typeof DESTINATION_OPTIONS[number];
  severity_min: Severity;
  port?: string;
  subscription_id?: string;
};

const parseFiltersFromUrl = (location: string): Filters => {
  const search = location.split("?")[1] ?? "";
  const params = new URLSearchParams(search);
  const days = Number(params.get("days") ?? 7);
  const status = (params.get("status") ?? "ALL").toUpperCase() as Filters["status"];
  const destination = (params.get("destination") ?? "ALL").toUpperCase() as Filters["destination"];
  const severity = (params.get("severity_min") ?? "HIGH").toUpperCase() as Severity;
  return {
    days: Number.isFinite(days) ? Math.min(Math.max(days, 1), 365) : 7,
    status: STATUS_OPTIONS.includes(status as any) ? status : "ALL",
    destination: DESTINATION_OPTIONS.includes(destination as any) ? destination : "ALL",
    severity_min: SEVERITY_OPTIONS.includes(severity) ? severity : "HIGH",
    port: params.get("port") ?? undefined,
    subscription_id: params.get("subscription_id") ?? undefined,
  };
};

export default function AlertsPage() {
  const [location, setLocation] = useLocation();
  const [filters, setFilters] = useState<Filters>(() => parseFiltersFromUrl(location));
  const [deliveries, setDeliveries] = useState<AlertDelivery[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<AlertDelivery | null>(null);
  const [selectedLoading, setSelectedLoading] = useState(false);
  const [metrics, setMetrics] = useState<AlertMetrics | null>(null);
  const [summaryRange, setSummaryRange] = useState<"1" | "7">("7");
  const { toast } = useToast();

  const syncFilters = useMemo(() => parseFiltersFromUrl(location), [location]);
  useEffect(() => setFilters(syncFilters), [syncFilters]);

  useEffect(() => {
    const query = buildQueryString(filters);
    const target = `/alerts?${query}`;
    if (location !== target) {
      setLocation(target, { replace: true });
    }
  }, [filters, location, setLocation]);

  useEffect(() => {
    const controller = new AbortController();
    const loadDeliveries = async () => {
      setLoading(true);
      setError(undefined);
      setNextCursor(null);
      try {
        const query = buildQueryString(filters);
        const payload = await apiFetchJson(`/v1/alert-deliveries?${query}`, { signal: controller.signal });
        if (!payload) {
          setDeliveries([]);
          setNextCursor(null);
        } else {
          setDeliveries(Array.isArray(payload.items) ? payload.items : []);
          setNextCursor(payload.next_cursor ?? null);
        }
      } catch (err: any) {
        if (err?.name === "AbortError") return;
        setError(err?.message ?? "Unable to load deliveries");
      } finally {
        setLoading(false);
      }
    };
    loadDeliveries();
    return () => controller.abort();
  }, [filters]);

  const handleLoadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    try {
      const query = buildQueryString(filters, nextCursor);
      const payload = await apiFetchJson(`/v1/alert-deliveries?${query}`);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setDeliveries((prev) => [...prev, ...items]);
      setNextCursor(payload?.next_cursor ?? null);
    } catch (err: any) {
      toast({ title: "Load more failed", description: err?.message ?? "Unable to load more deliveries.", variant: "destructive" });
    } finally {
      setLoadingMore(false);
    }
  };

  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      return;
    }
    const controller = new AbortController();
    const loadDetail = async () => {
      setSelectedLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/alert-deliveries/${selectedId}?include_entity=true`, { signal: controller.signal });
        setSelected(payload?.item ?? null);
      } catch {
        setSelected(null);
      } finally {
        setSelectedLoading(false);
      }
    };
    loadDetail();
    return () => controller.abort();
  }, [selectedId]);

  useEffect(() => {
    const controller = new AbortController();
    const loadMetrics = async () => {
      try {
        const payload = await apiFetchJson(`/api/alerts/metrics?days=${summaryRange}`, { signal: controller.signal });
        setMetrics(payload);
      } catch {
        setMetrics(null);
      }
    };
    loadMetrics();
    return () => controller.abort();
  }, [summaryRange]);

  const summary = useMemo(() => {
    const uniqueRuns = new Set(deliveries.map((row) => row.run_id));
    const sent = deliveries.filter((row) => row.status === "SENT").length;
    const failed = deliveries.filter((row) => row.status === "FAILED").length;
    return {
      runs: uniqueRuns.size,
      sent,
      failed,
    };
  }, [deliveries]);

  const dlqPending = deliveries.filter((row) => row.dlq_pending).length;
  const medianLatency = metrics?.latency?.p50 ?? null;

  const showDevTools = import.meta.env.MODE !== "production" || import.meta.env.VITE_SHOW_DEV_TOOLS === "true";
  const handleSeedDemo = async () => {
    try {
      await apiFetchJson("/api/dev/alert-subscriptions/seed", { method: "POST" });
      await apiFetchJson(`/api/alerts/run?user_id=${DEMO_USER_ID}`, { method: "POST" });
      toast({ title: "Demo data seeded", description: "Alert deliveries refreshed." });
      setFilters((prev) => ({ ...prev }));
      const metricsPayload = await apiFetchJson(`/api/alerts/metrics?days=${summaryRange}`);
      if (metricsPayload) setMetrics(metricsPayload);
    } catch (error: any) {
      toast({ title: "Seed failed", description: error?.message ?? "Unable to seed demo data.", variant: "destructive" });
    }
  };

  return (
    <TooltipProvider>
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
                <h1 className="mt-2 text-2xl font-semibold text-foreground">Alert Activity</h1>
                <p className="text-sm text-muted-foreground">
                  Delivery-level visibility into alerting reliability.
                </p>
              </div>
              <div className="flex items-center gap-3">
                <Button
                  size="sm"
                  variant={summaryRange === "1" ? "default" : "outline"}
                  onClick={() => setSummaryRange("1")}
                >
                  24h
                </Button>
                <Button
                  size="sm"
                  variant={summaryRange === "7" ? "default" : "outline"}
                  onClick={() => setSummaryRange("7")}
                >
                  7d
                </Button>
                {showDevTools && (
                  <Button size="sm" variant="outline" onClick={handleSeedDemo}>
                    Seed demo data
                  </Button>
                )}
              </div>
            </div>
            <div className="mt-6 grid gap-4 md:grid-cols-4">
              {["Total runs", "Sent", "Failed", "DLQ pending"].map((label, index) => (
                <Card key={label} className="border-border/60 bg-card/70">
                  <CardContent className="py-4">
                    <p className="text-xs text-muted-foreground">{label}</p>
                    {loading ? (
                      <Skeleton className="mt-2 h-5 w-16" />
                    ) : (
                      <p className="text-xl font-semibold">
                        {index === 0 ? summary.runs : index === 1 ? summary.sent : index === 2 ? summary.failed : dlqPending}
                      </p>
                    )}
                    {label === "DLQ pending" && dlqPending > 0 && (
                      <p className="text-[11px] text-orange-400 mt-1">Retries waiting</p>
                    )}
                    {label === "Sent" && medianLatency !== null && (
                      <p className="text-[11px] text-muted-foreground mt-1">
                        Median latency {medianLatency}ms
                      </p>
                    )}
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
        </div>

        <div className="container mx-auto px-6 py-8">
          <Card className="mb-6 border-border/60 bg-card/70">
            <CardContent className="grid gap-4 py-5 lg:grid-cols-[1.2fr_1.1fr_1fr_1fr_auto] lg:items-end">
              <div>
                <label className="text-xs font-medium text-muted-foreground">Days</label>
                <Input
                  type="number"
                  min={1}
                  max={365}
                  value={filters.days}
                  onChange={(event) =>
                    setFilters((prev) => ({
                      ...prev,
                      days: Math.min(Math.max(Number(event.target.value || 7), 1), 365),
                    }))
                  }
                  className="mt-2"
                />
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">Status</label>
                <Select
                  value={filters.status}
                  onValueChange={(value) => setFilters((prev) => ({ ...prev, status: value as Filters["status"] }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Select status" />
                  </SelectTrigger>
                  <SelectContent>
                    {STATUS_OPTIONS.map((status) => (
                      <SelectItem key={status} value={status}>
                        {status}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">Destination</label>
                <Select
                  value={filters.destination}
                  onValueChange={(value) => setFilters((prev) => ({ ...prev, destination: value as Filters["destination"] }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Select destination" />
                  </SelectTrigger>
                  <SelectContent>
                    {DESTINATION_OPTIONS.map((dest) => (
                      <SelectItem key={dest} value={dest}>
                        {dest}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">Severity</label>
                <Select
                  value={filters.severity_min}
                  onValueChange={(value) => setFilters((prev) => ({ ...prev, severity_min: value as Severity }))}
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
                  <p className="text-xs font-medium text-muted-foreground">Port</p>
                  <Input
                    placeholder="NLRTM"
                    value={filters.port ?? ""}
                    onChange={(event) =>
                      setFilters((prev) => ({
                        ...prev,
                        port: event.target.value || undefined,
                      }))
                    }
                    className="mt-2"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {loading ? (
            <Card className="border-border/60 bg-card/70">
              <CardContent className="py-10">
                <Skeleton className="h-6 w-1/3 mb-4" />
                <div className="space-y-3">
                  {Array.from({ length: 5 }).map((_, idx) => (
                    <Skeleton key={idx} className="h-12 w-full" />
                  ))}
                </div>
              </CardContent>
            </Card>
          ) : error ? (
            <div className="rounded-xl border border-destructive/30 bg-destructive/10 p-10 text-center text-sm text-destructive">
              Unable to load deliveries. Please retry.
            </div>
          ) : deliveries.length === 0 ? (
            <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center">
              <AlertTriangle className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No deliveries in the last {filters.days} days.</p>
              <p className="mt-2 text-xs text-muted-foreground">Run /api/alerts/run or seed subscriptions to generate activity.</p>
            </div>
          ) : (
            <Card className="border-border/60 bg-card/70">
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Time</TableHead>
                      <TableHead>Destination</TableHead>
                      <TableHead>Subscription</TableHead>
                      <TableHead>Cluster</TableHead>
                      <TableHead>Scope</TableHead>
                      <TableHead>Severity</TableHead>
                      <TableHead>Confidence</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Error</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {deliveries.map((row) => {
                      const statusLabel = row.dlq_pending ? "DLQ" : row.status.startsWith("SKIPPED") ? "SKIPPED" : row.status;
                      const severity = row.cluster_severity ?? "LOW";
                      const confidenceScore = row.confidence_score ?? 0;
                      const confidenceBand = row.confidence_band ?? "LOW";
                      const timeValue = row.sent_at ?? row.created_at ?? "";
                      return (
                        <TableRow
                          key={row.id}
                          className="cursor-pointer"
                          onClick={() => setSelectedId(row.id)}
                        >
                          <TableCell>
                            <div className="text-sm text-foreground" title={timeValue}>
                              {formatRelativeTime(timeValue)}
                            </div>
                          </TableCell>
                          <TableCell>
                            <div className="text-xs text-muted-foreground">{row.destination_type}</div>
                            <div className="text-sm">{row.endpoint}</div>
                          </TableCell>
                          <TableCell>
                            <span className="text-xs text-muted-foreground">{row.subscription_id.slice(0, 8)}</span>
                          </TableCell>
                          <TableCell>
                            <div className="text-xs text-muted-foreground">{row.cluster_type ?? "Event"}</div>
                            <div className="text-sm text-foreground">
                              {row.entity?.unlocode ?? row.entity_id}
                            </div>
                          </TableCell>
                          <TableCell>
                            <Badge
                              className={cn(
                                "border text-xs",
                                row.scope === "GLOBAL"
                                  ? "bg-indigo-500/10 text-indigo-300 border-indigo-500/30"
                                  : "bg-slate-500/10 text-slate-300 border-slate-500/30",
                              )}
                            >
                              {row.scope === "GLOBAL"
                                ? "GLOBAL"
                                : row.entity?.unlocode
                                  ? `PORT: ${row.entity.unlocode}`
                                  : "PORT"}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <Badge className={cn("border text-xs", SEVERITY_STYLES[severity])}>{severity}</Badge>
                          </TableCell>
                          <TableCell>
                            <Badge variant="secondary">
                              {confidenceBand} {DOT} {(confidenceScore * 100).toFixed(0)}%
                            </Badge>
                          </TableCell>
                          <TableCell>
                            <Badge className={cn("border text-xs", STATUS_STYLES[statusLabel] ?? STATUS_STYLES.FAILED)}>
                              {statusLabel}
                            </Badge>
                          </TableCell>
                          <TableCell>
                            {row.error ? (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <span className="text-xs text-muted-foreground line-clamp-1">
                                    {row.error}
                                  </span>
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="max-w-xs text-xs">{row.error}</p>
                                </TooltipContent>
                              </Tooltip>
                            ) : (
                              <span className="text-xs text-muted-foreground">--</span>
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
                {nextCursor && (
                  <div className="flex justify-center border-t border-border/60 px-4 py-4">
                    <Button variant="outline" onClick={handleLoadMore} disabled={loadingMore}>
                      {loadingMore ? "Loading..." : "Load more"}
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      <Sheet open={Boolean(selectedId)} onOpenChange={(open) => !open && setSelectedId(null)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
          {selectedLoading && (
            <div className="py-10">
              <Skeleton className="h-6 w-1/2 mb-4" />
              <Skeleton className="h-24 w-full" />
            </div>
          )}
          {selected && (
            <div className="space-y-6">
              <SheetHeader className="space-y-2">
                <SheetTitle className="text-xl">
                  {selected.cluster_summary ?? "Alert Delivery"} {selected.entity ? `-- ${selected.entity.name}` : ""}
                </SheetTitle>
                <div className="flex flex-wrap items-center gap-3 text-sm text-muted-foreground">
                  <Badge className={cn("border text-xs", STATUS_STYLES[selected.dlq_pending ? "DLQ" : selected.status])}>
                    {selected.dlq_pending ? "DLQ" : selected.status}
                  </Badge>
                  <span>{selected.day}</span>
                  <span>{selected.sent_at ?? selected.created_at ?? ""}</span>
                </div>
              </SheetHeader>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 text-sm text-muted-foreground">
                Destination: {selected.destination_type} {DOT} {selected.endpoint}
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                <div className="text-sm font-semibold text-foreground">Attempts</div>
                {selected.attempt_history && selected.attempt_history.length > 0 ? (
                  <div className="space-y-2">
                    {selected.attempt_history.map((attempt) => (
                      <div key={attempt.attempt_no} className="flex items-center justify-between text-xs text-muted-foreground">
                        <span>Attempt {attempt.attempt_no}</span>
                        <span>{attempt.status}</span>
                        <span>{attempt.latency_ms ?? "--"}ms</span>
                        <span>{attempt.http_status ?? "--"}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="text-xs text-muted-foreground">No attempts recorded.</div>
                )}
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                <div className="text-sm font-semibold text-foreground">Payload preview</div>
                <details>
                  <summary className="cursor-pointer text-xs text-muted-foreground">Show payload</summary>
                  <pre className="mt-3 whitespace-pre-wrap text-xs text-muted-foreground">
                    {JSON.stringify(selected.alert_payload ?? {}, null, 2)}
                  </pre>
                </details>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => navigator.clipboard.writeText(JSON.stringify(selected.alert_payload ?? {}, null, 2))}
                >
                  Copy payload
                </Button>
              </div>

              {selected.error && (
                <div className="rounded-xl border border-destructive/40 bg-destructive/10 p-4">
                  <div className="text-sm font-semibold text-destructive mb-1">Error</div>
                  <p className="text-xs text-destructive">{selected.error}</p>
                </div>
              )}

                <div className="flex items-center gap-3">
                  <Button
                    variant="outline"
                    disabled={!selected.dlq_pending || Boolean(selected.dlq_terminal)}
                    onClick={async () => {
                      try {
                        await apiFetchJson(`/api/alerts/retry-delivery/${selected.id}`, { method: "POST" });
                        toast({ title: "Retry queued", description: "Delivery retry triggered." });
                        setFilters((prev) => ({ ...prev }));
                        setSelectedId(selected.id);
                      } catch {
                        toast({ title: "Retry failed", description: "Unable to retry delivery.", variant: "destructive" });
                      }
                    }}
                  >
                    <RefreshCcw className="h-4 w-4 mr-2" />
                    Retry now
                  </Button>
                  {!selected.dlq_pending && (
                    <span className="text-xs text-muted-foreground">Retry available only for DLQ pending items.</span>
                  )}
                  {selected.dlq_pending && selected.dlq_terminal && (
                    <span className="text-xs text-muted-foreground">Retry disabled: max attempts reached.</span>
                  )}
                </div>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </TooltipProvider>
  );
}

