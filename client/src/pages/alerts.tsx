import { useEffect, useMemo, useState } from "react";
import { useLocation } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";
import { apiFetchJson, getApiKey } from "@/lib/apiFetch";
import { useAuth } from "@/auth/useAuth";
import AlertsSubnav from "@/components/alerts-subnav";
import { getAlertPlaybook } from "@shared/alertPlaybook";
import { AlertTriangle, ArrowLeft, Copy, RefreshCcw } from "lucide-react";
import { Link } from "wouter";

type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
type ConfidenceBand = "LOW" | "MEDIUM" | "HIGH";

type AlertClusterPayload = {
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

type AlertBundleItem = {
  cluster_id?: string | null;
  cluster_type?: string | null;
  cluster_summary?: string | null;
  day?: string | null;
  severity?: Severity | null;
  confidence?: { score?: number | null; band?: ConfidenceBand | null; method?: string | null };
  quality?: { score?: number | null; band?: "LOW" | "MEDIUM" | "HIGH"; reasons?: any[]; version?: string | null };
  entity?: { id?: string; type?: string; name?: string; code?: string; unlocode?: string } | null;
};

type AlertBundlePayload = {
  payload_version?: string;
  type?: string;
  sent_at?: string;
  subscription?: {
    id?: string;
    scope?: string;
    entity?: { id?: string; type?: string; name?: string; code?: string; unlocode?: string } | null;
    severity_min?: string;
    destination_type?: string;
    destination?: string;
  };
  summary?: {
    matched_total?: number;
    sent_items?: number;
    overflow?: number;
    skipped_dedupe?: number;
    skipped_noise_budget?: number;
    suppressed_quality?: number;
  };
  items?: AlertBundleItem[];
  system?: any;
};

type AlertPayload = AlertClusterPayload | AlertBundlePayload;

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
  destination_key?: string | null;
  destination_state?: string | null;
  destination_reason?: string | null;
  destination_ready_to_resume?: boolean;
  status: string;
  skip_reason?: string | null;
  is_bundle?: boolean;
  bundle_size?: number | null;
  bundle_overflow?: number | null;
  bundle_payload?: AlertBundlePayload | null;
  decision_summary?: {
    bundle_size?: number | null;
    bundle_overflow?: number | null;
    gates?: {
      dedupe_blocked?: boolean | null;
      noise_allowed?: boolean | null;
      quality_suppressed?: boolean | null;
      rate_allowed?: boolean | null;
      endpoint_status?: string | null;
      endpoint_allowed?: boolean | null;
      destination_state?: string | null;
      destination_allowed?: boolean | null;
    } | null;
    suppressed_counts?: { dedupe: number; noise_budget: number; quality: number; overflow: number } | null;
  } | null;
  decision?: any | null;
  endpoint_health?: {
    window?: string | null;
    status?: string | null;
    success_rate?: number | null;
    p95_ms?: number | null;
    last_success_at?: string | null;
    last_failure_at?: string | null;
    updated_at?: string | null;
  } | null;
  quality_score?: number | null;
  quality_band?: "LOW" | "MEDIUM" | "HIGH" | null;
  quality_reasons?: Array<{ code: string; weight: number; note?: string }> | null;
  quality_version?: string | null;
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

type SlaItem = {
  destination_type: string;
  destination_key: string;
  window: "24h" | "7d";
  window_start: string;
  attempts_total: number;
  attempts_success: number;
  attempts_failed: number;
  success_rate_pct: number;
  latency_p50_ms: number;
  latency_p95_ms: number;
  status: "OK" | "AT_RISK";
  destination?: {
    destination_key: string;
    state: "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
    reason?: string | null;
    ready_to_resume?: boolean;
    resume_ready_at?: string | null;
  };
  sla: { p95_ms: number; success_rate_pct: number; source?: string };
  updated_at: string;
};


const SEVERITY_OPTIONS: Severity[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
const STATUS_OPTIONS = ["ALL", "SENT", "FAILED", "SKIPPED", "DLQ"] as const;
const DESTINATION_OPTIONS = ["ALL", "WEBHOOK", "EMAIL"] as const;
const SLA_WINDOWS = ["24h", "7d"] as const;

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

const SKIP_REASON_LABELS: Record<string, string> = {
  NOISE_BUDGET_EXCEEDED: "Noise budget exceeded",
  QUALITY_BELOW_THRESHOLD: "Quality below threshold",
  ENDPOINT_DOWN: "Endpoint down",
  DESTINATION_PAUSED: "Destination paused",
  DESTINATION_AUTO_PAUSED: "Destination auto-paused",
  DESTINATION_DISABLED: "Destination disabled",
};

const formatSkipReason = (value?: string | null) => {
  if (!value) return null;
  return SKIP_REASON_LABELS[value] ?? value.replace(/_/g, " ").toLowerCase();
};

const SLA_STATUS_STYLES: Record<string, string> = {
  OK: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  AT_RISK: "bg-orange-500/10 text-orange-400 border-orange-500/30",
};

const DESTINATION_STATE_STYLES: Record<string, string> = {
  ACTIVE: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  PAUSED: "bg-slate-500/10 text-slate-300 border-slate-500/30",
  AUTO_PAUSED: "bg-amber-500/10 text-amber-300 border-amber-500/30",
  DISABLED: "bg-red-500/10 text-red-400 border-red-500/30",
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
  if (filters.destination_key) params.set("destination_key", filters.destination_key);
  if (filters.run_id) params.set("run_id", filters.run_id);
  if (filters.delivery_id) params.set("delivery_id", filters.delivery_id);
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
  destination_key?: string;
  run_id?: string;
  delivery_id?: string;
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
    destination_key: params.get("destination_key") ?? undefined,
    run_id: params.get("run_id") ?? undefined,
    delivery_id: params.get("delivery_id") ?? undefined,
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
  const [summary, setSummary] = useState<{
    total: number;
    sent: number;
    failed: number;
    dlq_pending: number;
    skipped: number;
    skipped_noise_budget?: number;
    p50_latency_ms: number | null;
  } | null>(null);
  const [incidentSummary, setIncidentSummary] = useState<{ open_count: number; acked_count: number; resolved_count: number } | null>(null);
  const [slaWindow, setSlaWindow] = useState<(typeof SLA_WINDOWS)[number]>("24h");
  const [slaItems, setSlaItems] = useState<SlaItem[]>([]);
  const [slaLoading, setSlaLoading] = useState(true);
  const [slaError, setSlaError] = useState<string | undefined>(undefined);
  const { toast } = useToast();
  const { role } = useAuth();
  const canOperate = role === "OWNER" || role === "OPERATOR";
  const canSeed = role === "OWNER";
  const canSlaOperate = role === "OWNER" || role === "OPERATOR";
  const canSlaEnable = role === "OWNER";
  const playbook = useMemo(() => {
    if (!selected) return null;
    const statusLabel = selected.dlq_pending ? "DLQ" : selected.status;
    const primaryClusterType =
      selected.cluster_type ??
      selected.decision?.selection?.bundle?.included?.[0]?.cluster_type ??
      selected.bundle_payload?.items?.[0]?.cluster_type ??
      null;
    return getAlertPlaybook({
      clusterType: primaryClusterType,
      status: statusLabel,
      destinationType: selected.destination_type,
      skipReason: selected.skip_reason,
    });
  }, [selected]);
  const destinationState = selected?.destination_state ?? selected?.decision?.gates?.destination_state?.state ?? null;
  const destinationReady = Boolean(selected?.destination_ready_to_resume);

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
    if (!filters.delivery_id) return;
    setSelectedId(filters.delivery_id);
  }, [filters.delivery_id]);

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
          setSummary(null);
        } else {
          setDeliveries(Array.isArray(payload.items) ? payload.items : []);
          setNextCursor(payload.next_cursor ?? null);
          setSummary(payload.summary ?? null);
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

  useEffect(() => {
    const controller = new AbortController();
    const loadIncidentSummary = async () => {
      try {
        const payload = await apiFetchJson("/v1/incidents?limit=1", { signal: controller.signal });
        setIncidentSummary(payload?.summary ?? null);
      } catch (err: any) {
        if (err?.name === "AbortError") return;
        setIncidentSummary(null);
      }
    };
    loadIncidentSummary();
    return () => controller.abort();
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    const loadSla = async () => {
      setSlaLoading(true);
      setSlaError(undefined);
      try {
        const payload = await apiFetchJson(`/v1/alert-slas?window=${slaWindow}`, { signal: controller.signal });
        setSlaItems(Array.isArray(payload?.items) ? payload.items : []);
      } catch (err: any) {
        if (err?.name === "AbortError") return;
        setSlaError(err?.message ?? "Unable to load SLA data");
        setSlaItems([]);
      } finally {
        setSlaLoading(false);
      }
    };
    loadSla();
    return () => controller.abort();
  }, [slaWindow]);

  const handleLoadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    try {
      const query = buildQueryString(filters, nextCursor);
      const payload = await apiFetchJson(`/v1/alert-deliveries?${query}`);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setDeliveries((prev) => [...prev, ...items]);
      setNextCursor(payload?.next_cursor ?? null);
      if (payload?.summary) {
        setSummary(payload.summary);
      }
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

  const updateDestinationState = async (state: "ACTIVE" | "PAUSED" | "DISABLED") => {
    if (!selected?.destination_key) {
      toast({ title: "Destination key missing", description: "Unable to update destination state.", variant: "destructive" });
      return;
    }
    try {
      await apiFetchJson("/v1/alert-destinations/state", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          destination_type: selected.destination_type,
          destination_key: selected.destination_key,
          state,
          reason: "manual",
        }),
      });
      toast({ title: "Destination updated", description: `State set to ${state}.` });
      if (selected?.id) {
        const payload = await apiFetchJson(`/v1/alert-deliveries/${selected.id}?include_entity=true`);
        if (payload?.item) setSelected(payload.item);
      }
      setFilters((prev) => ({ ...prev }));
    } catch (error: any) {
      toast({ title: "Update failed", description: error?.message ?? "Unable to update destination.", variant: "destructive" });
    }
  };

  const updateSlaDestinationState = async (destinationKey: string, destinationType: string, state: "ACTIVE" | "PAUSED" | "DISABLED") => {
    try {
      await apiFetchJson("/v1/alert-destinations/state", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          destination_type: destinationType,
          destination_key: destinationKey,
          state,
          reason: "SLA_OPS",
        }),
      });
      toast({ title: "Destination updated", description: `State set to ${state}.` });
      setFilters((prev) => ({ ...prev }));
    } catch (error: any) {
      toast({ title: "Update failed", description: error?.message ?? "Unable to update destination.", variant: "destructive" });
    }
  };

  const retryDelivery = async (force?: boolean) => {
    if (!selected) return;
    try {
      const apiKey = getApiKey();
      const qs = force ? "?force=true" : "";
      const res = await fetch(`/api/alerts/retry-delivery/${selected.id}${qs}`, {
        method: "POST",
        headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined,
      });
      const text = await res.text();
      let body: any = null;
      try {
        body = text ? JSON.parse(text) : null;
      } catch {
        body = null;
      }
      if (!res.ok) {
        if (res.status === 409 && body?.error === "DESTINATION_BLOCKED") {
          toast({
            title: "Retry blocked",
            description: `Retry blocked: destination is ${body.state ?? "blocked"}.`,
          });
          return;
        }
        toast({ title: "Retry failed", description: "Unable to retry delivery.", variant: "destructive" });
        return;
      }
      toast({ title: force ? "Force retry queued" : "Retry queued", description: "Delivery retry triggered." });
      setFilters((prev) => ({ ...prev }));
      setSelectedId(selected.id);
    } catch (error: any) {
      toast({ title: "Retry failed", description: error?.message ?? "Unable to retry delivery.", variant: "destructive" });
    }
  };

  const dlqPending = summary?.dlq_pending ?? 0;
  const medianLatency = summary?.p50_latency_ms ?? null;
  const renderSummaryValue = (value: number | null | undefined) => (
    summary ? value ?? 0 : "--"
  );

  const showDevTools = (import.meta.env.MODE !== "production" || import.meta.env.VITE_SHOW_DEV_TOOLS === "true") && canSeed;
  const handleSeedDemo = async () => {
    try {
      const now = new Date();
      const day = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
      const dayParam = day.toISOString().slice(0, 10);
      await apiFetchJson(`/api/dev/seed-anomaly?day=${dayParam}`, { method: "POST" });
      await apiFetchJson(`/api/signals/run?day=${dayParam}`, { method: "POST" });
      await apiFetchJson("/api/dev/alert-subscriptions/seed", { method: "POST" });
      await apiFetchJson(`/api/alerts/run?user_id=${DEMO_USER_ID}`, { method: "POST" });
      toast({ title: "Demo data seeded", description: "Alert deliveries refreshed." });
      setFilters((prev) => ({ ...prev }));
    } catch (error: any) {
      const message = String(error?.message ?? "");
      if (message.includes("HTTP 404")) {
        toast({
          title: "Seed disabled",
          description: "Dev seed routes are disabled. Start with DEV_ROUTES_ENABLED=true or run npm run demo:server.",
          variant: "destructive",
        });
        return;
      }
      toast({ title: "Seed failed", description: message || "Unable to seed demo data.", variant: "destructive" });
    }
  };

  const slaByDestination = useMemo(() => {
    const groups = slaItems.reduce<Record<string, SlaItem[]>>((acc, item) => {
      const key = item.destination_type;
      acc[key] = acc[key] ?? [];
      acc[key].push(item);
      return acc;
    }, {});

    const aggregate = (items: SlaItem[]) => {
      const status = items.some((row) => row.status === "AT_RISK") ? "AT_RISK" : "OK";
      const latencyP95 = Math.max(...items.map((row) => row.latency_p95_ms));
      const successRate = Math.min(...items.map((row) => row.success_rate_pct));
      const atRiskCount = items.filter((row) => row.status === "AT_RISK").length;
      return {
        status,
        latency_p95_ms: latencyP95,
        success_rate_pct: successRate,
        at_risk_count: atRiskCount,
        total: items.length,
      };
    };

    return Object.entries(groups).reduce<Record<string, ReturnType<typeof aggregate>>>((acc, [key, items]) => {
      if (items.length) {
        acc[key] = aggregate(items);
      }
      return acc;
    }, {});
  }, [slaItems]);

  const renderSlaCard = (label: string) => {
    const item = slaByDestination[label];
    if (!item) {
      return (
        <Card className="border-border/60 bg-card/70">
          <CardContent className="py-4">
            <p className="text-xs text-muted-foreground">{label}</p>
            <p className="mt-2 text-sm text-muted-foreground">No data yet.</p>
          </CardContent>
        </Card>
      );
    }
    return (
      <Card className="border-border/60 bg-card/70">
        <CardContent className="py-4 space-y-2">
          <div className="flex items-center justify-between">
            <p className="text-xs text-muted-foreground">{label}</p>
            <Badge className={cn("border text-xs", SLA_STATUS_STYLES[item.status])}>{item.status}</Badge>
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Worst p95</span>
            <span className="text-foreground">{item.latency_p95_ms}ms</span>
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>Worst success</span>
            <span className="text-foreground">{item.success_rate_pct.toFixed(2)}%</span>
          </div>
          <div className="flex items-center justify-between text-xs text-muted-foreground">
            <span>At-risk endpoints</span>
            <span className="text-foreground">{item.at_risk_count}</span>
          </div>
          {item.status === "AT_RISK" && (
            <a className="text-xs text-orange-300 hover:text-orange-200" href="#sla-details">
              View details
            </a>
          )}
        </CardContent>
      </Card>
    );
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
                <AlertsSubnav />
              </div>
              <div className="flex items-center gap-3">
                <Button
                  size="sm"
                  variant={filters.days === 1 ? "default" : "outline"}
                  onClick={() => setFilters((prev) => ({ ...prev, days: 1 }))}
                >
                  24h
                </Button>
                <Button
                  size="sm"
                  variant={filters.days === 7 ? "default" : "outline"}
                  onClick={() => setFilters((prev) => ({ ...prev, days: 7 }))}
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
                        {index === 0
                          ? renderSummaryValue(summary?.total)
                          : index === 1
                            ? renderSummaryValue(summary?.sent)
                            : index === 2
                              ? renderSummaryValue(summary?.failed)
                              : renderSummaryValue(dlqPending)}
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
            {incidentSummary?.open_count > 0 && (
              <div className="mt-6 flex items-center justify-between rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3">
                <div className="flex items-center gap-2 text-sm text-amber-200">
                  <AlertTriangle className="h-4 w-4" />
                  {incidentSummary.open_count} Open Incident{incidentSummary.open_count > 1 ? "s" : ""}
                </div>
                <Link href="/incidents?status=OPEN">
                  <a className="text-xs text-amber-200 hover:text-amber-100">View</a>
                </Link>
              </div>
            )}
            <div className="mt-6 rounded-xl border border-border/60 bg-card/70 p-5">
              <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
                <div>
                  <h2 className="text-sm font-semibold text-foreground">Delivery SLA</h2>
                  <p className="text-xs text-muted-foreground">p95 latency + success rate by destination.</p>
                </div>
                <div className="flex items-center gap-2">
                  {SLA_WINDOWS.map((window) => (
                    <Button
                      key={window}
                      size="sm"
                      variant={slaWindow === window ? "default" : "outline"}
                      onClick={() => setSlaWindow(window)}
                    >
                      {window}
                    </Button>
                  ))}
                </div>
              </div>
              <div className="mt-4">
                {slaLoading ? (
                  <div className="grid gap-4 md:grid-cols-2">
                    <Skeleton className="h-20 w-full" />
                    <Skeleton className="h-20 w-full" />
                  </div>
                ) : slaError ? (
                  <div className="text-xs text-destructive">{slaError}</div>
                ) : slaItems.length === 0 ? (
                  <div className="text-xs text-muted-foreground">No SLA data yet.</div>
                ) : (
                  <div className="grid gap-4 md:grid-cols-2">
                    {renderSlaCard("WEBHOOK")}
                    {renderSlaCard("EMAIL")}
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        <div className="container mx-auto px-6 py-8">
          {slaItems.length > 0 && (
            <Card id="sla-details" className="mb-6 border-border/60 bg-card/70">
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Destination</TableHead>
                      <TableHead>Destination key</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>p95 latency</TableHead>
                      <TableHead>Success rate</TableHead>
                      <TableHead>Destination state</TableHead>
                      <TableHead>Thresholds</TableHead>
                      <TableHead>Updated</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {[...slaItems]
                      .sort((a, b) => {
                        if (a.destination_type !== b.destination_type) {
                          return a.destination_type.localeCompare(b.destination_type);
                        }
                        if (a.status !== b.status) {
                          return a.status === "AT_RISK" ? -1 : 1;
                        }
                        return a.destination_key.localeCompare(b.destination_key);
                      })
                      .map((item) => (
                      <TableRow key={`${item.destination_type}-${item.destination_key}-${item.window}`}>
                        <TableCell>{item.destination_type}</TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Link
                              href={`/alerts/destinations?destination_key=${encodeURIComponent(item.destination_key)}`}
                              className="text-xs text-muted-foreground hover:text-foreground"
                            >
                              {item.destination_key.slice(0, 12)}...
                            </Link>
                            <Button
                              size="icon"
                              variant="ghost"
                              onClick={() => navigator.clipboard.writeText(item.destination_key)}
                              aria-label="Copy destination key"
                            >
                              <Copy className="h-3 w-3" />
                            </Button>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("border text-xs", SLA_STATUS_STYLES[item.status])}>{item.status}</Badge>
                        </TableCell>
                        <TableCell>{item.latency_p95_ms}ms</TableCell>
                        <TableCell>{item.success_rate_pct.toFixed(2)}%</TableCell>
                        <TableCell>
                          <div className="flex flex-col gap-1">
                            <Badge className={cn("border text-xs w-fit", DESTINATION_STATE_STYLES[item.destination?.state ?? "ACTIVE"])}>
                              {item.destination?.state ?? "ACTIVE"}
                            </Badge>
                            {item.destination?.state === "AUTO_PAUSED" && item.destination?.ready_to_resume && (
                              <span className="text-[11px] text-muted-foreground">Ready to resume</span>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="text-xs text-muted-foreground">
                            p95 ≤ {item.sla?.p95_ms ?? "--"}ms · ≥ {(item.sla?.success_rate_pct ?? 0).toFixed(2)}%
                          </div>
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">{formatRelativeTime(item.updated_at)}</TableCell>
                        <TableCell>
                          <div className="flex flex-wrap items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => setLocation(`/alerts/destinations?destination_key=${encodeURIComponent(item.destination_key)}`)}
                            >
                              Open destination
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => setLocation(`/alerts?destination_key=${encodeURIComponent(item.destination_key)}&status=FAILED`)}
                            >
                              View deliveries
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => setLocation(`/incidents?destination_key=${encodeURIComponent(item.destination_key)}&type=SLA`)}
                            >
                              View incidents
                            </Button>
                            {item.destination?.state === "ACTIVE" && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!canSlaOperate}
                                title={canSlaOperate ? "Pause destination" : "Requires OPERATOR"}
                                onClick={() => updateSlaDestinationState(item.destination_key, item.destination_type, "PAUSED")}
                              >
                                Pause
                              </Button>
                            )}
                            {(item.destination?.state === "PAUSED" || item.destination?.state === "AUTO_PAUSED") && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!canSlaOperate}
                                title={canSlaOperate ? "Resume destination" : "Requires OPERATOR"}
                                onClick={() => updateSlaDestinationState(item.destination_key, item.destination_type, "ACTIVE")}
                              >
                                Resume
                              </Button>
                            )}
                            {item.destination?.state === "DISABLED" && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!canSlaEnable}
                                title={canSlaEnable ? "Enable destination" : "Requires OWNER"}
                                onClick={() => updateSlaDestinationState(item.destination_key, item.destination_type, "ACTIVE")}
                              >
                                Enable
                              </Button>
                            )}
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}
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
                      <TableHead>Bundle</TableHead>
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
                      const errorValue = row.error ?? formatSkipReason(row.skip_reason);
                      const bundleSize = row.bundle_size ?? (row.is_bundle ? 1 : 0);
                      const bundleOverflow = row.bundle_overflow ?? 0;
                      const bundleLabel = row.is_bundle
                        ? `${bundleSize} alert${bundleSize === 1 ? "" : "s"}${bundleOverflow > 0 ? ` (+${bundleOverflow})` : ""}`
                        : "--";
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
                            <span className="text-xs text-muted-foreground">{bundleLabel}</span>
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
                            {errorValue ? (
                              <Tooltip>
                                <TooltipTrigger asChild>
                                  <span className="text-xs text-muted-foreground line-clamp-1">
                                    {errorValue}
                                  </span>
                                </TooltipTrigger>
                                <TooltipContent>
                                  <p className="max-w-xs text-xs">{errorValue}</p>
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
                  {(() => {
                    const statusLabel = selected.dlq_pending ? "DLQ" : selected.status.startsWith("SKIPPED") ? "SKIPPED" : selected.status;
                    return (
                      <Badge className={cn("border text-xs", STATUS_STYLES[statusLabel] ?? STATUS_STYLES.FAILED)}>
                        {statusLabel}
                      </Badge>
                    );
                  })()}
                  <span>{selected.day}</span>
                  <span>{selected.sent_at ?? selected.created_at ?? ""}</span>
                </div>
              </SheetHeader>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 text-sm text-muted-foreground">
                Destination: {selected.destination_type} {DOT} {selected.endpoint}
              </div>

              {(() => {
                const payload = (selected.bundle_payload ?? selected.alert_payload) as AlertBundlePayload | AlertPayload | null;
                const isBundle = Boolean(payload && (payload as any).type === "ALERT_BUNDLE");
                if (!isBundle) return null;
                const summary = (payload as AlertBundlePayload).summary ?? {};
                const items = Array.isArray((payload as AlertBundlePayload).items)
                  ? (payload as AlertBundlePayload).items ?? []
                  : [];
                return (
                  <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-3">
                    <div className="text-sm font-semibold text-foreground">Bundle summary</div>
                    <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                      <div>Matched: {summary.matched_total ?? items.length}</div>
                      <div>Sent items: {summary.sent_items ?? items.length}</div>
                      <div>Overflow: {summary.overflow ?? 0}</div>
                      <div>Skipped dedupe: {summary.skipped_dedupe ?? 0}</div>
                      <div>Skipped noise budget: {summary.skipped_noise_budget ?? 0}</div>
                      <div>Suppressed quality: {summary.suppressed_quality ?? 0}</div>
                    </div>
                    {items.length > 0 && (
                      <div className="space-y-2">
                        <div className="text-xs uppercase text-muted-foreground">Top alerts</div>
                        {items.map((item, idx) => {
                          const label = item.cluster_summary ?? item.cluster_type ?? "Alert";
                          const entity = item.entity?.name ?? item.entity?.code ?? item.entity?.unlocode ?? "";
                          const entitySuffix = entity ? ` \u2014 ${entity}` : "";
                          return (
                            <div key={`${item.cluster_id ?? idx}`} className="text-xs text-muted-foreground">
                              {idx + 1}. {item.severity ?? "ALERT"} {label}
                              {entitySuffix}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })()}

              {selected.decision && (
                <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-3">
                  <div className="text-sm font-semibold text-foreground">Decision</div>
                  <div className="text-xs text-muted-foreground">
                    {selected.status.startsWith("SKIPPED")
                      ? "Skipped by decision gates"
                      : selected.status === "SENT"
                        ? "Sent by decision gates"
                        : selected.status === "FAILED"
                          ? "Failed after decision gates"
                          : "Processed by decision gates"}
                  </div>

                  <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                    <div>Scope: {selected.decision?.subscription?.scope ?? selected.scope ?? "PORT"}</div>
                    <div>Severity min: {selected.decision?.subscription?.severity_min ?? "--"}</div>
                    <div>Destination: {selected.decision?.subscription?.destination_type ?? selected.destination_type}</div>
                    <div>Bundle: {selected.decision?.selection?.bundle?.size ?? selected.bundle_size ?? 0} included, {selected.decision?.selection?.bundle?.overflow ?? selected.bundle_overflow ?? 0} overflow</div>
                  </div>

                  <div className="flex flex-wrap gap-2 text-xs">
                    <Badge variant="secondary">
                      Severity min {selected.decision?.gates?.severity_min_pass ? "✅" : "❌"}
                    </Badge>
                    <Badge variant="secondary">
                      Dedupe {selected.decision?.gates?.dedupe?.blocked ? "❌" : "✅"}
                    </Badge>
                    <Badge variant="secondary">
                      Noise budget {selected.decision?.gates?.noise_budget?.allowed === false ? "❌" : "✅"}
                    </Badge>
                    <Badge variant="secondary">
                      Quality {selected.decision?.gates?.quality?.suppressed ? "❌" : "✅"}
                    </Badge>
                    <Badge variant="secondary">
                      Rate limit {selected.decision?.gates?.rate_limit?.allowed === false ? "❌" : "✅"}
                    </Badge>
                    <Badge variant="secondary">
                      Endpoint {selected.decision?.gates?.endpoint_health?.allowed === false ? "❌" : "✅"} {selected.decision?.gates?.endpoint_health?.status ? `(${selected.decision.gates.endpoint_health.status})` : ""}
                    </Badge>
                    <Badge variant="secondary">
                      Destination {selected.decision?.gates?.destination_state?.allowed === false ? "❌" : "✅"} {selected.decision?.gates?.destination_state?.state ? `(${selected.decision.gates.destination_state.state})` : ""}
                    </Badge>
                  </div>

                  {selected.decision?.suppressed_counts && (
                    <div className="text-xs text-muted-foreground">
                      Suppressed: dedupe {selected.decision.suppressed_counts.dedupe ?? 0} {DOT} noise {selected.decision.suppressed_counts.noise_budget ?? 0} {DOT} quality {selected.decision.suppressed_counts.quality ?? 0} {DOT} overflow {selected.decision.suppressed_counts.overflow ?? 0}
                    </div>
                  )}

                  {Array.isArray(selected.decision?.selection?.bundle?.included) && selected.decision.selection.bundle.included.length > 0 && (
                    <div className="space-y-2">
                      <div className="text-xs uppercase text-muted-foreground">Included alerts</div>
                      {selected.decision.selection.bundle.included.slice(0, 5).map((item: any, idx: number) => (
                        <div key={`${item.cluster_id}-${idx}`} className="text-xs text-muted-foreground">
                          {idx + 1}. {item.cluster_severity ?? "ALERT"} {item.summary ?? item.cluster_type ?? "Alert"}
                        </div>
                      ))}
                    </div>
                  )}

                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => navigator.clipboard.writeText(JSON.stringify(selected.decision ?? {}, null, 2))}
                  >
                    Copy decision JSON
                  </Button>
                </div>
              )}

              {playbook && (
                <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-4">
                  <div>
                    <div className="text-sm font-semibold text-foreground">Playbook</div>
                    <div className="text-xs text-muted-foreground">{playbook.title}</div>
                  </div>
                  <div className="grid gap-4 sm:grid-cols-2">
                    <div>
                      <div className="text-xs uppercase text-muted-foreground">Checks</div>
                      <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
                        {playbook.checks.map((item) => (
                          <li key={item}>- {item}</li>
                        ))}
                      </ul>
                    </div>
                    <div>
                      <div className="text-xs uppercase text-muted-foreground">Actions</div>
                      <ul className="mt-2 space-y-1 text-xs text-muted-foreground">
                        {playbook.actions.map((item) => (
                          <li key={item}>- {item}</li>
                        ))}
                      </ul>
                    </div>
                  </div>
                  {playbook.notes && playbook.notes.length > 0 && (
                    <div className="text-xs text-muted-foreground">
                      Notes: {playbook.notes.join(" ")}
                    </div>
                  )}
                </div>
              )}

              {selected.endpoint_health && (
                <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold text-foreground">Endpoint health ({selected.endpoint_health.window ?? "1h"})</div>
                    <Badge variant="secondary">{selected.endpoint_health.status ?? "UNKNOWN"}</Badge>
                  </div>
                  <div className="grid gap-2 text-xs text-muted-foreground sm:grid-cols-2">
                    <div>Success rate: {selected.endpoint_health.success_rate != null ? `${Math.round(selected.endpoint_health.success_rate * 100)}%` : "--"}</div>
                    <div>p95: {selected.endpoint_health.p95_ms ?? "--"}ms</div>
                    <div>Last success: {formatRelativeTime(selected.endpoint_health.last_success_at ?? null)}</div>
                    <div>Last failure: {formatRelativeTime(selected.endpoint_health.last_failure_at ?? null)}</div>
                  </div>
                  {selected.status.startsWith("SKIPPED") && selected.skip_reason === "ENDPOINT_DOWN" && (
                    <div className="text-xs text-muted-foreground">
                      Delivery suppressed to avoid DLQ inflation while the endpoint is down.
                    </div>
                  )}
                </div>
              )}

              {destinationState && (
                <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold text-foreground">Destination state</div>
                    <Badge variant="secondary">{destinationState}</Badge>
                  </div>
                  {(selected.destination_reason || selected.decision?.gates?.destination_state?.reason) && (
                    <div className="text-xs text-muted-foreground">Reason: {selected.destination_reason ?? selected.decision?.gates?.destination_state?.reason}</div>
                  )}
                  {canOperate && selected.destination_key && (
                    <div className="flex flex-wrap gap-2">
                      {destinationState === "ACTIVE" && (
                        <Button size="sm" variant="outline" onClick={() => updateDestinationState("PAUSED")}>
                          Pause destination
                        </Button>
                      )}
                      {(destinationState === "PAUSED" || destinationState === "AUTO_PAUSED") && (
                        <Button size="sm" variant="outline" onClick={() => updateDestinationState("ACTIVE")}>
                          Resume destination
                        </Button>
                      )}
                      {destinationState === "DISABLED" && role === "OWNER" && (
                        <Button size="sm" variant="outline" onClick={() => updateDestinationState("ACTIVE")}>
                          Enable destination
                        </Button>
                      )}
                    </div>
                  )}
                </div>
              )}

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
                    {JSON.stringify(selected.bundle_payload ?? selected.alert_payload ?? {}, null, 2)}
                  </pre>
                </details>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => navigator.clipboard.writeText(JSON.stringify(selected.bundle_payload ?? selected.alert_payload ?? {}, null, 2))}
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

              {!selected.error && selected.skip_reason && (
                <div className="rounded-xl border border-border/60 bg-card/70 p-4">
                  <div className="text-sm font-semibold text-foreground mb-1">Skip reason</div>
                  <p className="text-xs text-muted-foreground">{formatSkipReason(selected.skip_reason)}</p>
                </div>
              )}

                <div className="flex items-center gap-3">
                  <Button
                    variant="outline"
                    disabled={!selected.dlq_pending || Boolean(selected.dlq_terminal) || !canOperate}
                    onClick={() => retryDelivery(false)}
                  >
                    <RefreshCcw className="h-4 w-4 mr-2" />
                    Retry now
                  </Button>
                  {canOperate && destinationState === "AUTO_PAUSED" && !destinationReady && (
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => retryDelivery(true)}
                      title="Override auto-paused and attempt delivery now"
                    >
                      Force retry
                    </Button>
                  )}
                  {!canOperate && (
                    <span className="text-xs text-muted-foreground">Requires OPERATOR role.</span>
                  )}
                  {canOperate && !selected.dlq_pending && (
                    <span className="text-xs text-muted-foreground">Retry available only for DLQ pending items.</span>
                  )}
                  {canOperate && selected.dlq_pending && selected.dlq_terminal && (
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

