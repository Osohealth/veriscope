import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
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
import { apiFetchJson, getApiKey } from "@/lib/apiFetch";
import { IncidentTypeFilter, getIncidentTypeLabel, mapIncidentTypeToApi, parseIncidentTypeFilter } from "@/lib/incidentTypes";
import { fmtMs } from "@/lib/duration";
import { cn } from "@/lib/utils";
import AlertsSubnav from "@/components/alerts-subnav";
import { useAuth } from "@/auth/useAuth";
import { AlertTriangle, ArrowLeft, Copy } from "lucide-react";

type IncidentStatus = "OPEN" | "ACKED" | "RESOLVED";
type IncidentItem = {
  id: string;
  type: string;
  destination_key?: string | null;
  status: IncidentStatus;
  severity: string;
  title: string;
  summary: string;
  opened_at: string;
  acked_at?: string | null;
  resolved_at?: string | null;
};

type IncidentSummary = {
  open_count: number;
  acked_count: number;
  resolved_count: number;
};

type AuditEvent = {
  id: string;
  actor_type: string;
  actor_label?: string | null;
  actor_user_id?: string | null;
  actor_api_key_id?: string | null;
  action: string;
  resource_type: string;
  resource_id?: string | null;
  message: string;
  metadata?: Record<string, unknown>;
  created_at?: string | null;
};

type EscalationPolicy = {
  id: string;
  incident_type: string;
  level: number;
  after_minutes: number;
  severity_min: string;
  target_type: string;
  target_ref: string;
};

type EscalationDelivery = {
  id: string;
  created_at?: string | null;
  level?: number | null;
  destination_type?: string | null;
  status?: string | null;
  destination_key?: string | null;
};

type EscalationNext = {
  level?: number | null;
  after_minutes?: number | null;
  due_at?: string | null;
  eta_seconds?: number | null;
  reason: string;
};

type EscalationSnapshot = {
  has_policy: boolean;
  incident_type: string;
  severity: string;
  current_level: number;
  last_escalated_at?: string | null;
  next: EscalationNext;
  policies: EscalationPolicy[];
  deliveries: EscalationDelivery[];
};

type Filters = {
  status: "ALL" | IncidentStatus;
  type: IncidentTypeFilter;
  destination_key?: string;
  incident_id?: string;
  severity_min?: string;
};

const STATUS_OPTIONS: Filters["status"][] = ["ALL", "OPEN", "ACKED", "RESOLVED"];
const TYPE_OPTIONS: IncidentTypeFilter[] = ["ALL", "SLA", "ENDPOINT"];

const STATUS_STYLES: Record<IncidentStatus, string> = {
  OPEN: "bg-red-500/10 text-red-400 border-red-500/30",
  ACKED: "bg-amber-500/10 text-amber-300 border-amber-500/30",
  RESOLVED: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
};

const DOT = "\u00B7";

const parseFiltersFromUrl = (location: string): Filters => {
  const params = new URLSearchParams(location.split("?")[1] ?? "");
  const status = (params.get("status") ?? "ALL").toUpperCase() as Filters["status"];
  return {
    status: STATUS_OPTIONS.includes(status) ? status : "ALL",
    type: parseIncidentTypeFilter(params.get("type")),
    destination_key: params.get("destination_key") ?? undefined,
    incident_id: params.get("incident_id") ?? undefined,
    severity_min: params.get("severity_min") ?? undefined,
  };
};

const buildQueryString = (filters: Filters, cursor?: string | null) => {
  const params = new URLSearchParams();
  if (filters.status !== "ALL") params.set("status", filters.status);
  if (filters.type !== "ALL") params.set("type", filters.type);
  if (filters.destination_key) params.set("destination_key", filters.destination_key);
  if (filters.incident_id) params.set("incident_id", filters.incident_id);
  if (filters.severity_min) params.set("severity_min", filters.severity_min);
  params.set("limit", "20");
  if (cursor) params.set("cursor", cursor);
  return params.toString();
};

const buildListQueryString = (filters: Filters, cursor?: string | null) => {
  const params = new URLSearchParams();
  if (filters.status !== "ALL") params.set("status", filters.status);
  const type = mapIncidentTypeToApi(filters.type);
  if (type) params.set("type", type);
  if (filters.destination_key) params.set("destination_key", filters.destination_key);
  if (filters.severity_min) params.set("severity_min", filters.severity_min);
  params.set("limit", "20");
  if (cursor) params.set("cursor", cursor);
  return params.toString();
};

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

const summarizeActor = (item: AuditEvent) => {
  if (item.actor_type === "SYSTEM") return "System";
  if (item.actor_label) return item.actor_label;
  if (item.actor_api_key_id) return "API key";
  if (item.actor_user_id) return "User";
  return item.actor_type?.toLowerCase() ?? "Actor";
};

const getUpdatedAt = (incident?: IncidentItem | null) =>
  incident?.resolved_at ?? incident?.acked_at ?? incident?.opened_at ?? null;

const incidentTypeToEscalationFilter = (type?: string | null) => {
  const upper = String(type ?? "").toUpperCase();
  if (upper.includes("SLA")) return "SLA";
  if (upper.includes("ENDPOINT")) return "ENDPOINT";
  return "ALL";
};

const getEscalationLevel = (delivery: any) => {
  const decisionLevel = delivery?.decision?.escalation?.level ?? delivery?.decision?.gates?.escalation?.level;
  if (Number.isFinite(decisionLevel)) return Number(decisionLevel);
  const explicitLevel = delivery?.level;
  if (Number.isFinite(explicitLevel)) return Number(explicitLevel);
  const clusterId = String(delivery?.cluster_id ?? "");
  const match = clusterId.match(/:level:(\d+)/i);
  return match ? Number(match[1]) : null;
};

export default function IncidentsPage() {
  const { toast } = useToast();
  const { role } = useAuth();
  const [location, setLocation] = useLocation();
  const [filters, setFilters] = useState<Filters>(() => parseFiltersFromUrl(location));
  const [items, setItems] = useState<IncidentItem[]>([]);
  const [summary, setSummary] = useState<IncidentSummary | null>(null);
  const [metricsDays, setMetricsDays] = useState(7);
  const [metrics, setMetrics] = useState<any | null>(null);
  const [metricsLoading, setMetricsLoading] = useState(true);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<IncidentItem | null>(null);
  const [selectedLoading, setSelectedLoading] = useState(false);
  const [detailRefresh, setDetailRefresh] = useState(0);
  const [auditItems, setAuditItems] = useState<AuditEvent[]>([]);
  const [auditCursor, setAuditCursor] = useState<string | null>(null);
  const [auditLoadingMore, setAuditLoadingMore] = useState(false);
  const [mutatingIds, setMutatingIds] = useState<Record<string, boolean>>({});
  const [showSystemEvents, setShowSystemEvents] = useState(true);
  const [escalation, setEscalation] = useState<EscalationSnapshot | null>(null);
  const [showEscalationPolicies, setShowEscalationPolicies] = useState(false);
  const [runningEscalations, setRunningEscalations] = useState(false);
  const playbookItems = useMemo(() => {
    if (!selected) return [];
    const destinationKey = selected.destination_key;
    const items: Array<{ title: string; description?: string; href?: string }> = [];
    if (String(selected.type).toUpperCase().includes("ENDPOINT")) {
      items.push({
        title: "Check endpoint health",
        description: "Review recent failures and latency.",
        href: "/alerts/health",
      });
      if (destinationKey) {
        items.push({
          title: "Open destination",
          description: "Pause or resume the endpoint if needed.",
          href: `/alerts/destinations?destination_key=${encodeURIComponent(destinationKey)}`,
        });
        items.push({
          title: "View failed deliveries",
          description: "Inspect recent failures for this endpoint.",
          href: `/alerts?destination_key=${encodeURIComponent(destinationKey)}&status=FAILED`,
        });
      }
      items.push({
        title: "Rotate secrets / verify auth",
        description: "Confirm webhook secret and auth headers.",
      });
    } else if (String(selected.type).toUpperCase().includes("SLA")) {
      items.push({
        title: "Review SLA thresholds",
        description: "Check p95 and success rate thresholds.",
        href: "/alerts",
      });
      if (destinationKey) {
        items.push({
          title: "Open destination",
          description: "Inspect endpoint health and delivery stats.",
          href: `/alerts/destinations?destination_key=${encodeURIComponent(destinationKey)}`,
        });
        items.push({
          title: "View failed deliveries",
          description: "Inspect recent failures for this destination.",
          href: `/alerts?destination_key=${encodeURIComponent(destinationKey)}&status=FAILED`,
        });
      }
      items.push({
        title: "Review alerts health",
        description: "Check delivery SLA summaries.",
        href: "/alerts/health",
      });
    }
    return items;
  }, [selected]);

  const canMutate = role === "OWNER" || role === "OPERATOR";
  const isMutatingSelected = selected ? Boolean(mutatingIds[selected.id]) : false;

  const syncedFilters = useMemo(() => parseFiltersFromUrl(location), [location]);
  useEffect(() => setFilters(syncedFilters), [syncedFilters]);

  useEffect(() => {
    const query = buildQueryString(filters);
    const target = `/incidents?${query}`;
    if (location !== target) {
      setLocation(target, { replace: true });
    }
  }, [filters, location, setLocation]);

  useEffect(() => {
    if (filters.incident_id) {
      setSelectedId(filters.incident_id);
    }
  }, [filters.incident_id]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const payload = await apiFetchJson(`/v1/incidents?${buildListQueryString(filters)}`, { signal: controller.signal });
        setItems(Array.isArray(payload?.items) ? payload.items : []);
        setSummary(payload?.summary ?? null);
        setNextCursor(payload?.next_cursor ?? null);
      } catch (err: any) {
        if (controller.signal.aborted || err?.name === "AbortError") return;
        setError(err?.message ?? "Unable to load incidents.");
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [filters]);

  useEffect(() => {
    const controller = new AbortController();
    const loadMetrics = async () => {
      setMetricsLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/incidents/metrics?days=${metricsDays}`, { signal: controller.signal });
        setMetrics(payload ?? null);
      } catch (err: any) {
        if (err?.name === "AbortError") return;
        setMetrics(null);
      } finally {
        setMetricsLoading(false);
      }
    };
    loadMetrics();
    return () => controller.abort();
  }, [metricsDays]);

  const handleLoadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    try {
      const payload = await apiFetchJson(`/v1/incidents?${buildListQueryString(filters, nextCursor)}`);
      const more = Array.isArray(payload?.items) ? payload.items : [];
      setItems((prev) => [...prev, ...more]);
      setNextCursor(payload?.next_cursor ?? null);
      if (payload?.summary) setSummary(payload.summary);
    } catch (err: any) {
      toast({ title: "Load more failed", description: err?.message ?? "Unable to load more incidents.", variant: "destructive" });
    } finally {
      setLoadingMore(false);
    }
  };

  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      setAuditItems([]);
      setAuditCursor(null);
      setEscalation(null);
      setShowEscalationPolicies(false);
      return;
    }
    const controller = new AbortController();
    const loadDetail = async () => {
      setSelectedLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/incidents/${selectedId}`, { signal: controller.signal });
        setSelected(payload?.item ?? null);
        setEscalation(payload?.escalation ?? null);
        const auditPayload = await apiFetchJson(`/v1/audit-events?resource_type=INCIDENT&resource_id=${selectedId}&limit=20&days=365`, { signal: controller.signal });
        setAuditItems(Array.isArray(auditPayload?.items) ? auditPayload.items : []);
        setAuditCursor(auditPayload?.next_cursor ?? null);
      } catch {
        setSelected(null);
        setAuditItems([]);
        setAuditCursor(null);
        setEscalation(null);
      } finally {
        setSelectedLoading(false);
      }
    };
    loadDetail();
    return () => controller.abort();
  }, [selectedId, detailRefresh]);

  const loadMoreAudit = async () => {
    if (!selectedId || !auditCursor) return;
    setAuditLoadingMore(true);
    try {
      const payload = await apiFetchJson(`/v1/audit-events?resource_type=INCIDENT&resource_id=${selectedId}&limit=20&days=365&cursor=${encodeURIComponent(auditCursor)}`);
      const more = Array.isArray(payload?.items) ? payload.items : [];
      setAuditItems((prev) => [...prev, ...more]);
      setAuditCursor(payload?.next_cursor ?? null);
    } catch (err: any) {
      toast({ title: "Audit load failed", description: err?.message ?? "Unable to load audit timeline.", variant: "destructive" });
    } finally {
      setAuditLoadingMore(false);
    }
  };

  const postIncidentAction = async (path: string) => {
    const apiKey = getApiKey();
    const res = await fetch(path, {
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
      throw new Error(body?.error ?? `HTTP ${res.status}`);
    }
    return body;
  };

  const ackIncident = async (incidentId: string) => {
    try {
      setMutatingIds((prev) => ({ ...prev, [incidentId]: true }));
      await postIncidentAction(`/v1/incidents/${incidentId}/ack`);
      toast({ title: "Incident acknowledged" });
      setDetailRefresh((prev) => prev + 1);
      setFilters((prev) => ({ ...prev }));
    } catch (err: any) {
      toast({ title: "Ack failed", description: err?.message ?? "Unable to ack incident.", variant: "destructive" });
    } finally {
      setMutatingIds((prev) => ({ ...prev, [incidentId]: false }));
    }
  };

  const resolveIncident = async (incidentId: string) => {
    try {
      setMutatingIds((prev) => ({ ...prev, [incidentId]: true }));
      await postIncidentAction(`/v1/incidents/${incidentId}/resolve`);
      toast({ title: "Incident resolved" });
      setDetailRefresh((prev) => prev + 1);
      setFilters((prev) => ({ ...prev }));
    } catch (err: any) {
      toast({ title: "Resolve failed", description: err?.message ?? "Unable to resolve incident.", variant: "destructive" });
    } finally {
      setMutatingIds((prev) => ({ ...prev, [incidentId]: false }));
    }
  };

  const runEscalationsNow = async () => {
    if (!selected) return;
    setRunningEscalations(true);
    try {
      const response = await apiFetchJson("/api/admin/incidents/escalations/run", { method: "POST" });
      const escalatedCount = response?.escalated ?? 0;
      const processedCount = response?.processed ?? 0;
      const skipped = response?.skipped ? " (skipped)" : "";
      toast({ title: "Escalations run", description: `Escalated ${escalatedCount} of ${processedCount}${skipped}` });
      setDetailRefresh((prev) => prev + 1);
      setFilters((prev) => ({ ...prev }));
    } catch (err: any) {
      const message = String(err?.message ?? "");
      if (message.startsWith("HTTP 404")) {
        toast({
          title: "Run disabled",
          description: "Run is disabled (dev-only). Enable DEV_ROUTES_ENABLED=true.",
          variant: "destructive",
        });
      } else {
        toast({ title: "Run failed", description: err?.message ?? "Unable to run escalations.", variant: "destructive" });
      }
    } finally {
      setRunningEscalations(false);
    }
  };

  const openIncident = (id: string) => {
    setFilters((prev) => ({ ...prev, incident_id: id }));
  };

  const clearFilters = () => {
    setFilters({ status: "ALL", type: "ALL" });
  };

  const totalCount = (summary?.open_count ?? 0) + (summary?.acked_count ?? 0) + (summary?.resolved_count ?? 0);

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
                <h1 className="mt-2 text-2xl font-semibold text-foreground">Incidents</h1>
                <p className="text-sm text-muted-foreground">
                  System incidents opened by SLA and endpoint health transitions.
                </p>
                <AlertsSubnav />
              </div>
            </div>

            <div className="mt-6 flex flex-wrap items-center gap-3">
              {[7, 30].map((days) => (
                <Button
                  key={days}
                  size="sm"
                  variant={metricsDays === days ? "default" : "outline"}
                  onClick={() => setMetricsDays(days)}
                >
                  {days}d
                </Button>
              ))}
            </div>
            <div className="mt-4 grid gap-4 md:grid-cols-3">
              <Card className="border-border/60 bg-card/70">
                <CardContent className="py-4">
                  <p className="text-xs text-muted-foreground">Open incidents</p>
                  {metricsLoading ? (
                    <Skeleton className="mt-2 h-5 w-16" />
                  ) : (
                    <p className="text-xl font-semibold">{metrics?.open_count ?? 0}</p>
                  )}
                  {metrics?.by_type && (
                    <p className="text-[11px] text-muted-foreground mt-1">
                      SLA {metrics.by_type.SLA_AT_RISK?.open_count ?? 0} {DOT} Endpoint {metrics.by_type.ENDPOINT_DOWN?.open_count ?? 0}
                    </p>
                  )}
                </CardContent>
              </Card>
              <Card className="border-border/60 bg-card/70">
                <CardContent className="py-4">
                  <p className="text-xs text-muted-foreground">MTTA (p50 / p95)</p>
                  {metricsLoading ? (
                    <Skeleton className="mt-2 h-5 w-32" />
                  ) : (
                    <p className="text-lg font-semibold">
                      {fmtMs(metrics?.mtta_ms?.p50)} {DOT} {fmtMs(metrics?.mtta_ms?.p95)}
                    </p>
                  )}
                  <p className="text-[11px] text-muted-foreground mt-1">
                    Samples {metrics?.mtta_ms?.n ?? 0}
                  </p>
                </CardContent>
              </Card>
              <Card className="border-border/60 bg-card/70">
                <CardContent className="py-4">
                  <p className="text-xs text-muted-foreground">MTTR (p50 / p95)</p>
                  {metricsLoading ? (
                    <Skeleton className="mt-2 h-5 w-32" />
                  ) : (
                    <p className="text-lg font-semibold">
                      {fmtMs(metrics?.mttr_ms?.p50)} {DOT} {fmtMs(metrics?.mttr_ms?.p95)}
                    </p>
                  )}
                  <p className="text-[11px] text-muted-foreground mt-1">
                    Samples {metrics?.mttr_ms?.n ?? 0}
                  </p>
                </CardContent>
              </Card>
            </div>
          </div>
        </div>

        <div className="container mx-auto px-6 py-8">
          <Card className="mb-6 border-border/60 bg-card/70">
            <CardContent className="grid gap-4 py-5 lg:grid-cols-[1fr_1fr_1.6fr_auto] lg:items-end">
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
                <label className="text-xs font-medium text-muted-foreground">Type</label>
                <Select
                  value={filters.type}
                  onValueChange={(value) => setFilters((prev) => ({ ...prev, type: value as IncidentTypeFilter }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Select type" />
                  </SelectTrigger>
                  <SelectContent>
                    {TYPE_OPTIONS.map((type) => (
                      <SelectItem key={type} value={type}>
                        {type}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs font-medium text-muted-foreground">Destination key</label>
                <Input
                  placeholder="0a4b7ff88375fc18"
                  value={filters.destination_key ?? ""}
                  onChange={(event) =>
                    setFilters((prev) => ({ ...prev, destination_key: event.target.value || undefined }))
                  }
                  className="mt-2"
                />
              </div>
              <div className="flex gap-2">
                <Button variant="outline" onClick={clearFilters}>
                  Clear filters
                </Button>
                <Button variant="outline" disabled>
                  {totalCount} total
                </Button>
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
              Unable to load incidents.
            </div>
          ) : items.length === 0 ? (
            <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center">
              <AlertTriangle className="mx-auto mb-3 h-8 w-8 text-muted-foreground" />
              <p className="text-sm text-muted-foreground">No incidents match these filters.</p>
            </div>
          ) : (
            <Card className="border-border/60 bg-card/70">
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Created</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Destination</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Title</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {items.map((item) => (
                      <TableRow key={item.id} className="cursor-pointer" onClick={() => openIncident(item.id)}>
                        <TableCell className="text-xs text-muted-foreground">{formatRelativeTime(item.opened_at)}</TableCell>
                        <TableCell className="text-xs">{getIncidentTypeLabel(item.type)}</TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-muted-foreground">
                              {item.destination_key ? `${item.destination_key.slice(0, 12)}...` : "--"}
                            </span>
                            {item.destination_key && (
                              <Button
                                size="icon"
                                variant="ghost"
                                onClick={(event) => {
                                  event.stopPropagation();
                                  navigator.clipboard.writeText(item.destination_key ?? "");
                                }}
                              >
                                <Copy className="h-3 w-3" />
                              </Button>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("border text-xs", STATUS_STYLES[item.status])}>{item.status}</Badge>
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">{item.title}</TableCell>
                        <TableCell>
                          <div className="flex items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={!canMutate || item.status !== "OPEN" || mutatingIds[item.id]}
                              onClick={(event) => {
                                event.stopPropagation();
                                ackIncident(item.id);
                              }}
                            >
                              Ack
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={!canMutate || item.status === "RESOLVED" || mutatingIds[item.id]}
                              onClick={(event) => {
                                event.stopPropagation();
                                resolveIncident(item.id);
                              }}
                            >
                              Resolve
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </CardContent>
            </Card>
          )}

          {nextCursor && (
            <div className="mt-6 flex justify-center">
              <Button variant="outline" disabled={loadingMore} onClick={handleLoadMore}>
                {loadingMore ? "Loading..." : "Load more"}
              </Button>
            </div>
          )}
        </div>

        <Sheet
          open={Boolean(selectedId)}
          onOpenChange={(open) => {
            if (!open) {
              setSelectedId(null);
              setFilters((prev) => ({ ...prev, incident_id: undefined }));
            }
          }}
        >
          <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
            {selectedLoading || !selected ? (
              <div className="space-y-4">
                <Skeleton className="h-6 w-40" />
                <Skeleton className="h-24 w-full" />
                <Skeleton className="h-40 w-full" />
              </div>
            ) : (
              <div className="space-y-6">
                <SheetHeader className="space-y-2">
                  <SheetTitle className="text-xl">Incident</SheetTitle>
                  <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                    <Badge variant="outline">{getIncidentTypeLabel(selected.type)}</Badge>
                    <Badge className={cn("border text-xs", STATUS_STYLES[selected.status])}>{selected.status}</Badge>
                    <span className="text-xs">Opened {formatRelativeTime(selected.opened_at)}</span>
                    {getUpdatedAt(selected) && (
                      <span className="text-xs">Updated {formatRelativeTime(getUpdatedAt(selected))}</span>
                    )}
                  </div>
                </SheetHeader>

                <Card className="border-border/60 bg-card/70">
                  <CardContent className="space-y-2 py-4">
                    <div className="text-sm font-semibold">{selected.title}</div>
                    <div className="text-xs text-muted-foreground">{selected.summary}</div>
                    <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
                      <span className="font-mono">{selected.destination_key ?? "--"}</span>
                      {selected.destination_key && (
                        <>
                          <Button
                            size="icon"
                            variant="ghost"
                            onClick={() => navigator.clipboard.writeText(selected.destination_key ?? "")}
                          >
                            <Copy className="h-3 w-3" />
                          </Button>
                          <Link href={`/alerts/destinations?destination_key=${encodeURIComponent(selected.destination_key)}`}>
                            <a className="text-xs text-primary">Open destination</a>
                          </Link>
                        </>
                      )}
                    </div>
                    <div className="flex flex-wrap gap-2 pt-2">
                      <Button size="sm" variant="outline" disabled={!canMutate || selected.status !== "OPEN" || isMutatingSelected} onClick={() => ackIncident(selected.id)}>
                        Ack
                      </Button>
                      <Button size="sm" variant="outline" disabled={!canMutate || selected.status === "RESOLVED" || isMutatingSelected} onClick={() => resolveIncident(selected.id)}>
                        Resolve
                      </Button>
                      {!canMutate && (
                        <span className="text-xs text-muted-foreground">Requires OPERATOR role.</span>
                      )}
                    </div>
                  </CardContent>
                </Card>

                {playbookItems.length > 0 && (
                  <Card className="border-border/60 bg-card/70">
                    <CardContent className="py-4 space-y-3">
                      <div className="text-sm font-semibold">Suggested actions</div>
                      <div className="space-y-2">
                        {playbookItems.map((item) => (
                          <div key={item.title} className="rounded-lg border border-border/60 bg-background/40 px-3 py-2">
                            <div className="text-sm text-foreground">{item.title}</div>
                            {item.description && <div className="text-xs text-muted-foreground">{item.description}</div>}
                            {item.href && (
                              <div className="mt-1">
                                <Link href={item.href}>
                                  <a className="text-xs text-primary">Open</a>
                                </Link>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                )}

                <Card className="border-border/60 bg-card/70">
                  <CardContent className="py-4 space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="text-sm font-semibold">Escalation</div>
                      <Badge variant="outline">L{Math.max(0, escalation?.current_level ?? 0)}</Badge>
                    </div>
                    <div className="grid gap-2 text-xs text-muted-foreground">
                      <div>
                        Last escalated: {formatRelativeTime(escalation?.last_escalated_at ?? null)}
                      </div>
                      <div>
                        {(() => {
                          if (!escalation?.has_policy) {
                            return (
                              <>
                                No escalation policy configured.{" "}
                                <Link href="/settings/escalations">
                                  <a className="text-primary">Open policies</a>
                                </Link>
                              </>
                            );
                          }
                          if (escalation.next?.reason === "INCIDENT_NOT_OPEN") {
                            return "Incident not OPEN.";
                          }
                          if (escalation.next?.reason === "ALREADY_SENT") {
                            return "No further escalation levels configured.";
                          }
                          if (escalation.next?.reason === "READY") {
                            return "Due now.";
                          }
                          if (escalation.next?.reason === "NOT_DUE") {
                            const etaSeconds = Number(escalation.next?.eta_seconds ?? 0);
                            const etaMinutes = Math.max(1, Math.ceil(etaSeconds / 60));
                            return `Next L${escalation.next?.level ?? ""} in ${etaMinutes}m (due ${escalation.next?.due_at ? new Date(escalation.next.due_at).toLocaleTimeString() : "--"})`;
                          }
                          return "Escalation timing unavailable.";
                        })()}
                      </div>
                      {selected.status !== "OPEN" && (
                        <div className="text-muted-foreground">Escalation runs only while OPEN.</div>
                      )}
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => setShowEscalationPolicies((prev) => !prev)}
                      >
                        {showEscalationPolicies ? "Hide policies" : `View policies (${escalation?.policies?.length ?? 0})`}
                      </Button>
                      {!escalation?.has_policy && (
                        <Link href={`/settings/escalations?type=${incidentTypeToEscalationFilter(escalation?.incident_type ?? selected.type)}`}>
                          <a className="inline-flex">
                            <Button size="sm" variant="default">
                              Create policy
                            </Button>
                          </a>
                        </Link>
                      )}
                      {role === "OWNER" && escalation?.next?.reason === "READY" && (
                        <Button
                          size="sm"
                          variant="default"
                          disabled={runningEscalations}
                          onClick={runEscalationsNow}
                        >
                          {runningEscalations ? "Running..." : "Run escalations now"}
                        </Button>
                      )}
                    </div>
                    {showEscalationPolicies && (
                      <div className="rounded-lg border border-border/60 bg-background/40 p-3">
                        {escalation?.policies?.length ? (
                          <Table>
                            <TableHeader>
                              <TableRow>
                                <TableHead>Type</TableHead>
                                <TableHead>Level</TableHead>
                                <TableHead>After</TableHead>
                                <TableHead>Severity</TableHead>
                                <TableHead>Target</TableHead>
                              </TableRow>
                            </TableHeader>
                            <TableBody>
                              {escalation.policies.map((policy) => (
                                <TableRow key={policy.id}>
                                  <TableCell className="text-xs">{policy.incident_type}</TableCell>
                                  <TableCell className="text-xs">L{policy.level}</TableCell>
                                  <TableCell className="text-xs">{policy.after_minutes}m</TableCell>
                                  <TableCell className="text-xs">{policy.severity_min}</TableCell>
                                  <TableCell className="text-xs">{policy.target_type}:{policy.target_ref}</TableCell>
                                </TableRow>
                              ))}
                            </TableBody>
                          </Table>
                        ) : (
                          <div className="text-xs text-muted-foreground">No policies configured.</div>
                        )}
                      </div>
                    )}
                  </CardContent>
                </Card>

                <Card className="border-border/60 bg-card/70">
                  <CardContent className="py-4 space-y-3">
                    <div className="flex items-center justify-between">
                      <div className="text-sm font-semibold">Escalation deliveries</div>
                      <span className="text-xs text-muted-foreground">{escalation?.deliveries?.length ?? 0} total</span>
                    </div>
                    {(escalation?.deliveries?.length ?? 0) === 0 ? (
                      <div className="text-xs text-muted-foreground">No escalation deliveries yet.</div>
                    ) : (
                      <Table>
                        <TableHeader>
                          <TableRow>
                            <TableHead>Time</TableHead>
                            <TableHead>Level</TableHead>
                            <TableHead>Destination</TableHead>
                            <TableHead>Status</TableHead>
                            <TableHead></TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {escalation?.deliveries?.map((delivery) => (
                            <TableRow key={delivery.id}>
                              <TableCell className="text-xs text-muted-foreground">{formatRelativeTime(delivery.created_at)}</TableCell>
                              <TableCell className="text-xs">
                                {(() => {
                                  const level = getEscalationLevel(delivery);
                                  return level === null ? "L?" : `L${level}`;
                                })()}
                              </TableCell>
                              <TableCell className="text-xs">
                                {delivery.destination_type ?? "--"} {delivery.destination_key ? `· ${delivery.destination_key.slice(0, 8)}...` : ""}
                              </TableCell>
                              <TableCell className="text-xs">
                                {delivery.status ?? "--"}
                              </TableCell>
                              <TableCell>
                                <Link href={`/alerts?delivery_id=${delivery.id}`}>
                                  <a className="text-xs text-primary">Open delivery</a>
                                </Link>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    )}
                  </CardContent>
                </Card>

                <Card className="border-border/60 bg-card/70">
                  <CardContent className="py-4">
                    <div className="flex items-center justify-between">
                      <div className="text-sm font-semibold">Audit timeline</div>
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <Button
                            size="icon"
                            variant="ghost"
                            onClick={() => navigator.clipboard.writeText(JSON.stringify(auditItems, null, 2))}
                          >
                            <Copy className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent>Copy audit JSON</TooltipContent>
                      </Tooltip>
                    </div>
                    <div className="mt-3 flex items-center gap-2 text-xs text-muted-foreground">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => setShowSystemEvents((prev) => !prev)}
                      >
                        {showSystemEvents ? "Hide system events" : "Show system events"}
                      </Button>
                    </div>
                    <div className="mt-3 space-y-3">
                      {auditItems.filter((item) => showSystemEvents || item.actor_type !== "SYSTEM").length === 0 ? (
                        <div className="text-xs text-muted-foreground">No audit events yet.</div>
                      ) : (
                        auditItems
                          .filter((item) => showSystemEvents || item.actor_type !== "SYSTEM")
                          .map((item) => (
                          <div key={item.id} className="rounded-lg border border-border/60 bg-background/40 px-3 py-2">
                            <div className="flex items-center justify-between text-xs text-muted-foreground">
                              <span>{formatRelativeTime(item.created_at)}</span>
                              <span>{summarizeActor(item)}</span>
                            </div>
                            <div className="mt-1 text-sm text-foreground">{item.action}</div>
                            <div className="text-xs text-muted-foreground">{item.message}</div>
                            <div className="mt-2 flex gap-2">
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => navigator.clipboard.writeText(JSON.stringify(item, null, 2))}
                              >
                                Copy JSON
                              </Button>
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                    {auditCursor && (
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={auditLoadingMore}
                        onClick={loadMoreAudit}
                        className="mt-3"
                      >
                        {auditLoadingMore ? "Loading..." : "Load more"}
                      </Button>
                    )}
                  </CardContent>
                </Card>
              </div>
            )}
          </SheetContent>
        </Sheet>
      </div>
    </TooltipProvider>
  );
}
