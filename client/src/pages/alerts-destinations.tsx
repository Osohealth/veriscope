import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";
import { apiFetchJson } from "@/lib/apiFetch";
import { useAuth } from "@/auth/useAuth";
import AlertsSubnav from "@/components/alerts-subnav";
import { getAlertPlaybook } from "@shared/alertPlaybook";
import { ArrowLeft, Copy, RefreshCcw } from "lucide-react";

type DestinationState = "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
type DestinationType = "WEBHOOK" | "EMAIL";
type HealthStatus = "OK" | "DEGRADED" | "DOWN";

type DestinationStats = {
  sent: number;
  failed: number;
  skipped: number;
  p95_ms: number | null;
  success_rate: number;
};

type EndpointHealth = {
  window?: string | null;
  status?: HealthStatus | null;
  success_rate?: number | null;
  p95_ms?: number | null;
  last_success_at?: string | null;
  last_failure_at?: string | null;
  updated_at?: string | null;
};

type ResolvedNoiseBudget = {
  enabled?: boolean;
  window?: string | null;
  window_minutes?: number | null;
  max_deliveries?: number | null;
  source?: "DESTINATION" | "TENANT_DEFAULT";
};

type ResolvedSlaThreshold = {
  enabled?: boolean;
  p95_ms?: number | null;
  success_rate_min_pct?: number | null;
  source?: "DESTINATION" | "TENANT_DEFAULT";
};

type DestinationOverrideRow = {
  noiseBudgetEnabled?: boolean;
  noiseBudgetWindowMinutes?: number | null;
  noiseBudgetMaxDeliveries?: number | null;
  slaEnabled?: boolean;
};

type DestinationOverridesResponse = {
  version: string;
  destination_key: string;
  destination_type: DestinationType;
  destination?: string | null;
  overrides?: DestinationOverrideRow | null;
  sla_overrides?: Array<{ window: "24h" | "7d"; p95Ms?: number | null; successRateMinPct?: number | null }> | null;
  resolved?: {
    noise_budget?: ResolvedNoiseBudget | null;
    sla?: {
      "24h"?: ResolvedSlaThreshold | null;
      "7d"?: ResolvedSlaThreshold | null;
    } | null;
  } | null;
};

type OverrideFormState = {
  noiseEnabled: boolean;
  noiseWindowMinutes: string;
  noiseMaxDeliveries: string;
  slaEnabled: boolean;
  sla24P95: string;
  sla24Success: string;
  sla7dP95: string;
  sla7dSuccess: string;
};

type DestinationItem = {
  destination_key: string;
  destination_type: DestinationType;
  destination?: string | null;
  state: DestinationState;
  reason?: string | null;
  ready_to_resume?: boolean;
  resume_ready_at?: string | null;
  updated_at?: string | null;
  stats_24h?: DestinationStats | null;
  stats_1h?: DestinationStats | null;
  endpoint_health_24h?: EndpointHealth | null;
  endpoint_health_1h?: EndpointHealth | null;
  health?: EndpointHealth | null;
  last_delivery?: { status: string; created_at: string } | null;
};

type DestinationsResponse = {
  version: string;
  window: "1h" | "24h";
  items: DestinationItem[];
  next_cursor: string | null;
  summary: {
    states: Record<DestinationState, number>;
    types: Record<DestinationType, number>;
  };
};

type DestinationDetail = {
  version: string;
  destination_key: string;
  destination_type: DestinationType;
  endpoint: string;
  state: {
    state: DestinationState;
    reason?: string | null;
    ready_to_resume?: boolean;
    resume_ready_at?: string | null;
    updated_at?: string | null;
  };
  endpoint_health?: EndpointHealth | null;
  sla?: {
    window?: string | null;
    status?: string | null;
    p50_ms?: number | null;
    p95_ms?: number | null;
    success_rate?: number | null;
    thresholds?: { p95_ms?: number | null; success_rate_min?: number | null } | null;
    computed_at?: string | null;
    window_start?: string | null;
  } | null;
  recent_deliveries?: {
    items: Array<{
      id: string;
      status: string;
      skip_reason?: string | null;
      cluster_id?: string | null;
      cluster_severity?: string | null;
      cluster_summary?: string | null;
      error?: string | null;
      created_at?: string | null;
      sent_at?: string | null;
    }>;
    next_cursor?: string | null;
  } | null;
  audit_preview?: {
    items: AuditEventItem[];
    next_cursor?: string | null;
  } | null;
};

type AuditEventItem = {
  id: string;
  actor_type?: string | null;
  actor_user_id?: string | null;
  actor_api_key_id?: string | null;
  actor_label?: string | null;
  action?: string | null;
  message?: string | null;
  metadata?: Record<string, any>;
  created_at?: string | null;
};

const STATE_OPTIONS: Array<DestinationState | "ALL"> = ["ALL", "ACTIVE", "PAUSED", "AUTO_PAUSED", "DISABLED"];
const TYPE_OPTIONS: Array<DestinationType | "ALL"> = ["ALL", "WEBHOOK", "EMAIL"];

const STATE_STYLES: Record<DestinationState, string> = {
  ACTIVE: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  PAUSED: "bg-amber-500/10 text-amber-400 border-amber-500/30",
  AUTO_PAUSED: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  DISABLED: "bg-slate-500/10 text-slate-300 border-slate-500/30",
};

const HEALTH_STYLES: Record<HealthStatus, string> = {
  OK: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  DEGRADED: "bg-amber-500/10 text-amber-400 border-amber-500/30",
  DOWN: "bg-red-500/10 text-red-400 border-red-500/30",
};

const formatPercent = (value?: number | null) => {
  if (value == null) return "--";
  return `${Math.round(value * 100)}%`;
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

const formatDestination = (destination?: string | null, type?: DestinationType) => {
  if (!destination) return "--";
  if (type === "EMAIL") {
    const [user, domain] = destination.split("@");
    if (!domain) return destination;
    return `${user.slice(0, 2)}***@${domain}`;
  }
  try {
    const url = new URL(destination);
    return `${url.protocol}//${url.host}${url.pathname}`;
  } catch {
    return destination.length > 48 ? `${destination.slice(0, 45)}...` : destination;
  }
};

const toInputValue = (value?: number | null) => (value == null ? "" : String(value));
const parseOptionalInt = (value: string) => {
  const trimmed = value.trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildQueryString = (params: {
  window: "1h" | "24h";
  state: DestinationState | "ALL";
  destinationType: DestinationType | "ALL";
  q: string;
  limit?: number;
  cursor?: string | null;
}) => {
  const query = new URLSearchParams();
  query.set("window", params.window);
  if (params.state !== "ALL") query.set("state", params.state);
  if (params.destinationType !== "ALL") query.set("destination_type", params.destinationType);
  if (params.q) query.set("q", params.q);
  query.set("limit", String(params.limit ?? 50));
  if (params.cursor) query.set("cursor", params.cursor);
  return query.toString();
};

export default function AlertsDestinationsPage() {
  const [location] = useLocation();
  const { toast } = useToast();
  const { role } = useAuth();
  const canOperate = role === "OWNER" || role === "OPERATOR";
  const canDisable = role === "OWNER";

  const [window, setWindow] = useState<"1h" | "24h">("1h");
  const [stateFilter, setStateFilter] = useState<DestinationState | "ALL">("ALL");
  const [typeFilter, setTypeFilter] = useState<DestinationType | "ALL">("ALL");
  const [query, setQuery] = useState("");
  const [items, setItems] = useState<DestinationItem[]>([]);
  const [summary, setSummary] = useState<DestinationsResponse["summary"] | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedKeys, setSelectedKeys] = useState<Set<string>>(new Set());
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [detail, setDetail] = useState<DestinationDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [auditItems, setAuditItems] = useState<AuditEventItem[]>([]);
  const [auditCursor, setAuditCursor] = useState<string | null>(null);
  const [auditLoadingMore, setAuditLoadingMore] = useState(false);
  const [showSystemEvents, setShowSystemEvents] = useState(false);
  const [confirmAction, setConfirmAction] = useState<{
    key: string;
    current: DestinationState;
    next: DestinationState;
  } | null>(null);
  const [overrideDetail, setOverrideDetail] = useState<DestinationOverridesResponse | null>(null);
  const [overrideLoading, setOverrideLoading] = useState(false);
  const [overrideEditOpen, setOverrideEditOpen] = useState(false);
  const [overrideForm, setOverrideForm] = useState<OverrideFormState>({
    noiseEnabled: true,
    noiseWindowMinutes: "",
    noiseMaxDeliveries: "",
    slaEnabled: true,
    sla24P95: "",
    sla24Success: "",
    sla7dP95: "",
    sla7dSuccess: "",
  });

  useEffect(() => {
    if (query) return;
    const search = location.split("?")[1] ?? "";
    const params = new URLSearchParams(search);
    const destinationKey = params.get("destination_key");
    if (destinationKey) {
      setQuery(destinationKey);
    }
  }, [location, query]);

  const listParams = useMemo(() => ({
    window,
    state: stateFilter,
    destinationType: typeFilter,
    q: query.trim(),
  }), [window, stateFilter, typeFilter, query]);

  const loadDestinations = async (cursor?: string | null, append = false) => {
    const queryString = buildQueryString({
      ...listParams,
      limit: 50,
      cursor,
    });
    if (append) {
      setLoadingMore(true);
    } else {
      setLoading(true);
    }
    setError(null);
    try {
      const payload = await apiFetchJson(`/v1/alert-destinations?${queryString}`);
      const nextItems = Array.isArray(payload?.items) ? payload.items : [];
      setItems((prev) => append ? [...prev, ...nextItems] : nextItems);
      setNextCursor(payload?.next_cursor ?? null);
      setSummary(payload?.summary ?? null);
    } catch (err: any) {
      setError(err?.message ?? "Unable to load destinations");
      if (!append) {
        setItems([]);
        setNextCursor(null);
        setSummary(null);
      }
    } finally {
      setLoading(false);
      setLoadingMore(false);
    }
  };

  const refreshDetail = async (key?: string | null) => {
    const targetKey = key ?? selectedKey;
    if (!targetKey) return;
    setDetailLoading(true);
    try {
      const payload = await apiFetchJson(`/v1/alert-destinations/${targetKey}`);
      setDetail(payload ?? null);
      const preview = payload?.audit_preview ?? null;
      setAuditItems(Array.isArray(preview?.items) ? preview.items : []);
      setAuditCursor(preview?.next_cursor ?? null);
    } catch {
      setDetail(null);
      setAuditItems([]);
      setAuditCursor(null);
    } finally {
      setDetailLoading(false);
    }
  };

  useEffect(() => {
    loadDestinations(null, false);
    setSelectedKeys(new Set());
  }, [listParams]);

  useEffect(() => {
    if (!selectedKey) {
      setDetail(null);
      setAuditItems([]);
      setAuditCursor(null);
      return;
    }
    const controller = new AbortController();
    const loadDetail = async () => {
      setDetailLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/alert-destinations/${selectedKey}`, { signal: controller.signal });
        setDetail(payload ?? null);
        const preview = payload?.audit_preview ?? null;
        setAuditItems(Array.isArray(preview?.items) ? preview.items : []);
        setAuditCursor(preview?.next_cursor ?? null);
      } catch {
        setDetail(null);
        setAuditItems([]);
        setAuditCursor(null);
      } finally {
        setDetailLoading(false);
      }
    };
    loadDetail();
    return () => controller.abort();
  }, [selectedKey, window]);

  useEffect(() => {
    if (!selectedKey) {
      setOverrideDetail(null);
      return;
    }
    const controller = new AbortController();
    const loadOverrides = async () => {
      setOverrideLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/alert-destination-overrides/${selectedKey}`, { signal: controller.signal });
        setOverrideDetail(payload ?? null);
      } catch {
        setOverrideDetail(null);
      } finally {
        setOverrideLoading(false);
      }
    };
    loadOverrides();
    return () => controller.abort();
  }, [selectedKey]);

  useEffect(() => {
    if (!overrideEditOpen) return;
    const overrideRow = overrideDetail?.overrides ?? null;
    const slaRows = overrideDetail?.sla_overrides ?? [];
    const sla24 = slaRows.find((row) => row.window === "24h");
    const sla7d = slaRows.find((row) => row.window === "7d");
    setOverrideForm({
      noiseEnabled: overrideRow?.noiseBudgetEnabled ?? true,
      noiseWindowMinutes: toInputValue(overrideRow?.noiseBudgetWindowMinutes ?? null),
      noiseMaxDeliveries: toInputValue(overrideRow?.noiseBudgetMaxDeliveries ?? null),
      slaEnabled: overrideRow?.slaEnabled ?? true,
      sla24P95: toInputValue(sla24?.p95Ms ?? null),
      sla24Success: toInputValue(sla24?.successRateMinPct ?? null),
      sla7dP95: toInputValue(sla7d?.p95Ms ?? null),
      sla7dSuccess: toInputValue(sla7d?.successRateMinPct ?? null),
    });
  }, [overrideEditOpen, overrideDetail]);

  useEffect(() => {
    setSelectedKeys((prev) => {
      const next = new Set<string>();
      items.forEach((item) => {
        if (prev.has(item.destination_key)) next.add(item.destination_key);
      });
      return next;
    });
  }, [items]);

  const handleToggleSelectAll = (checked: boolean) => {
    if (!checked) {
      setSelectedKeys(new Set());
      return;
    }
    setSelectedKeys(new Set(items.map((item) => item.destination_key).filter(Boolean)));
  };

  const handleToggleSelect = (key: string, checked: boolean) => {
    setSelectedKeys((prev) => {
      const next = new Set(prev);
      if (checked) next.add(key);
      else next.delete(key);
      return next;
    });
  };

  const applyDestinationState = async (key: string, state: "ACTIVE" | "PAUSED" | "DISABLED") => {
    try {
      await apiFetchJson(`/v1/alert-destinations/${key}/state`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ state, note: "manual" }),
      });
      toast({ title: "Destination updated", description: `State set to ${state}.` });
      await loadDestinations(null, false);
      await refreshDetail(key);
    } catch (err: any) {
      toast({ title: "Update failed", description: err?.message ?? "Unable to update destination.", variant: "destructive" });
    }
  };

  const promptDestinationAction = (key: string, current: DestinationState, next: DestinationState) => {
    setConfirmAction({ key, current, next });
  };

  const runBulkUpdate = async (state: "ACTIVE" | "PAUSED" | "DISABLED") => {
    if (selectedKeys.size === 0) return;
    try {
      const payload = await apiFetchJson("/v1/alert-destinations/bulk/state", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ destination_keys: Array.from(selectedKeys), state, note: "bulk" }),
      });
      const okCount = Array.isArray(payload?.results)
        ? payload.results.filter((item: any) => item.status === "ok").length
        : 0;
      toast({ title: "Bulk update complete", description: `${okCount} destinations updated.` });
      setSelectedKeys(new Set());
      await loadDestinations(null, false);
    } catch (err: any) {
      toast({ title: "Bulk update failed", description: err?.message ?? "Unable to update destinations.", variant: "destructive" });
    }
  };

  const handleSaveOverrides = async () => {
    if (!selectedKey) return;
    try {
      const noiseWindowMinutes = overrideForm.noiseEnabled
        ? parseOptionalInt(overrideForm.noiseWindowMinutes)
        : null;
      const noiseMaxDeliveries = overrideForm.noiseEnabled
        ? parseOptionalInt(overrideForm.noiseMaxDeliveries)
        : null;
      const sla24P95 = overrideForm.slaEnabled ? parseOptionalInt(overrideForm.sla24P95) : null;
      const sla24Success = overrideForm.slaEnabled ? parseOptionalInt(overrideForm.sla24Success) : null;
      const sla7dP95 = overrideForm.slaEnabled ? parseOptionalInt(overrideForm.sla7dP95) : null;
      const sla7dSuccess = overrideForm.slaEnabled ? parseOptionalInt(overrideForm.sla7dSuccess) : null;
      const payload = {
        noise_budget: {
          enabled: overrideForm.noiseEnabled,
          window_minutes: noiseWindowMinutes,
          max_deliveries: noiseMaxDeliveries,
        },
        sla: {
          enabled: overrideForm.slaEnabled,
          "24h": {
            p95_ms: sla24P95,
            success_rate_min_pct: sla24Success,
          },
          "7d": {
            p95_ms: sla7dP95,
            success_rate_min_pct: sla7dSuccess,
          },
        },
      };

      await apiFetchJson(`/v1/alert-destination-overrides/${selectedKey}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      toast({ title: "Overrides saved", description: "Destination overrides updated." });
      setOverrideEditOpen(false);
      await loadDestinations(null, false);
      const refreshed = await apiFetchJson(`/v1/alert-destination-overrides/${selectedKey}`);
      setOverrideDetail(refreshed ?? null);
    } catch (err: any) {
      toast({ title: "Save failed", description: err?.message ?? "Unable to update overrides.", variant: "destructive" });
    }
  };

  const loadMoreAudit = async () => {
    if (!selectedKey || !auditCursor) return;
    setAuditLoadingMore(true);
    try {
      const payload = await apiFetchJson(`/v1/audit-events?resource_type=destination&resource_id=${selectedKey}&limit=20&cursor=${encodeURIComponent(auditCursor)}&days=365`);
      const nextItems = Array.isArray(payload?.items) ? payload.items : [];
      setAuditItems((prev) => [...prev, ...nextItems]);
      setAuditCursor(payload?.next_cursor ?? null);
    } catch (err: any) {
      toast({ title: "Audit load failed", description: err?.message ?? "Unable to load audit events.", variant: "destructive" });
    } finally {
      setAuditLoadingMore(false);
    }
  };

  const summaryStates = summary?.states ?? { ACTIVE: 0, PAUSED: 0, AUTO_PAUSED: 0, DISABLED: 0 };
  const downCount = items.filter((item) => item.health?.status === "DOWN").length;

  const filteredAuditItems = useMemo(() => {
    if (showSystemEvents) return auditItems;
    return auditItems.filter((item) => item.actor_type !== "SYSTEM");
  }, [auditItems, showSystemEvents]);

  const formatAuditActor = (item: AuditEventItem) => {
    if (item.actor_label) return item.actor_label;
    if (item.actor_type === "SYSTEM") return "system";
    if (item.actor_type) return item.actor_type.toLowerCase();
    return "unknown";
  };

  const selectedItem = useMemo(() => {
    if (detail) {
      return {
        destination_key: detail.destination_key,
        destination_type: detail.destination_type,
        destination: detail.endpoint,
        state: detail.state?.state ?? "ACTIVE",
        reason: detail.state?.reason ?? null,
        ready_to_resume: detail.state?.ready_to_resume ?? false,
        resume_ready_at: detail.state?.resume_ready_at ?? null,
      } as DestinationItem;
    }
    return items.find((item) => item.destination_key === selectedKey) ?? null;
  }, [detail, items, selectedKey]);
  const playbook = useMemo(() => {
    if (!selectedItem) return null;
    const healthStatus = detail?.endpoint_health?.status ?? selectedItem.health?.status ?? null;
    const clusterType = healthStatus ? `ENDPOINT_${healthStatus}` : null;
    const skipReason = selectedItem.state === "PAUSED"
      ? "DESTINATION_PAUSED"
      : selectedItem.state === "AUTO_PAUSED"
        ? "DESTINATION_AUTO_PAUSED"
        : selectedItem.state === "DISABLED"
          ? "DESTINATION_DISABLED"
          : null;
    const statusLabel = selectedItem.state === "ACTIVE" ? "SENT" : "SKIPPED";
    return getAlertPlaybook({
      clusterType,
      status: statusLabel,
      destinationType: selectedItem.destination_type,
      skipReason,
    });
  }, [selectedItem]);

  const resolvedNoise = overrideDetail?.resolved?.noise_budget ?? null;
  const resolvedSla24 = overrideDetail?.resolved?.sla?.["24h"] ?? null;
  const resolvedSla7d = overrideDetail?.resolved?.sla?.["7d"] ?? null;

  const formatNoiseWindow = (value?: ResolvedNoiseBudget | null) => {
    if (!value) return "--";
    if (value.window_minutes) return `${value.window_minutes}m`;
    if (value.window) return value.window;
    return "--";
  };

  const renderSourceBadge = (source?: "DESTINATION" | "TENANT_DEFAULT") => {
    if (!source) return null;
    const label = source === "DESTINATION" ? "Destination override" : "Tenant default";
    return (
      <Badge variant="outline" className="border-border/60 text-xs">
        {label}
      </Badge>
    );
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      <div className="border-b border-border/60 bg-card/40">
        <div className="mx-auto flex max-w-6xl flex-col gap-3 px-6 py-6">
          <Link href="/alerts">
            <a className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-primary">
              <ArrowLeft className="h-4 w-4" />
              Back to Menu
            </a>
          </Link>
          <div className="flex flex-wrap items-center justify-between gap-4">
            <div>
              <h1 className="text-2xl font-semibold">Destinations</h1>
              <p className="text-sm text-muted-foreground">Pause, resume, and inspect endpoint health across alert destinations.</p>
            </div>
            <div className="flex items-center gap-2">
              <Button variant={window === "1h" ? "default" : "outline"} size="sm" onClick={() => setWindow("1h")}>
                1h
              </Button>
              <Button variant={window === "24h" ? "default" : "outline"} size="sm" onClick={() => setWindow("24h")}>
                24h
              </Button>
              <Button variant="outline" size="icon" onClick={() => loadDestinations(null, false)}>
                <RefreshCcw className="h-4 w-4" />
              </Button>
            </div>
          </div>
          <AlertsSubnav />
        </div>
      </div>

      <div className="mx-auto flex max-w-6xl flex-col gap-6 px-6 py-6">
        <div className="grid gap-3 md:grid-cols-5">
          {(["ACTIVE", "PAUSED", "AUTO_PAUSED", "DISABLED"] as DestinationState[]).map((state) => (
            <Card key={state} className="border-border/60 bg-card/70">
              <CardContent className="py-4">
                <p className="text-xs text-muted-foreground">{state.replace("_", " ")}</p>
                <p className="mt-2 text-xl font-semibold">{summaryStates[state] ?? 0}</p>
              </CardContent>
            </Card>
          ))}
          <Card className="border-border/60 bg-card/70">
            <CardContent className="py-4">
              <p className="text-xs text-muted-foreground">Down endpoints</p>
              <p className="mt-2 text-xl font-semibold">{downCount}</p>
            </CardContent>
          </Card>
        </div>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="py-4">
            <div className="flex flex-wrap items-center gap-3">
              <div className="min-w-[140px]">
                <Select value={stateFilter} onValueChange={(value) => setStateFilter(value as DestinationState | "ALL")}>
                  <SelectTrigger>
                    <SelectValue placeholder="State" />
                  </SelectTrigger>
                  <SelectContent>
                    {STATE_OPTIONS.map((option) => (
                      <SelectItem key={option} value={option}>{option}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="min-w-[140px]">
                <Select value={typeFilter} onValueChange={(value) => setTypeFilter(value as DestinationType | "ALL")}>
                  <SelectTrigger>
                    <SelectValue placeholder="Type" />
                  </SelectTrigger>
                  <SelectContent>
                    {TYPE_OPTIONS.map((option) => (
                      <SelectItem key={option} value={option}>{option}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="flex-1">
                <Input
                  placeholder="Search destination or key"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
              </div>
              {selectedKeys.size > 0 && (
                <div className="flex flex-wrap items-center gap-2">
                  {canOperate && (
                    <>
                      <Button size="sm" variant="outline" onClick={() => runBulkUpdate("PAUSED")}>Pause</Button>
                      <Button size="sm" variant="outline" onClick={() => runBulkUpdate("ACTIVE")}>Resume</Button>
                    </>
                  )}
                  {canDisable && (
                    <Button size="sm" variant="destructive" onClick={() => runBulkUpdate("DISABLED")}>Disable</Button>
                  )}
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-0">
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead className="w-10">
                    <Checkbox
                      checked={items.length > 0 && selectedKeys.size === items.length}
                      onCheckedChange={(value) => handleToggleSelectAll(Boolean(value))}
                      aria-label="Select all destinations"
                    />
                  </TableHead>
                  <TableHead>Destination</TableHead>
                  <TableHead>Type</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Health</TableHead>
                  <TableHead>p95</TableHead>
                  <TableHead>Success</TableHead>
                  <TableHead>Last delivery</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {loading && (
                  Array.from({ length: 5 }).map((_, idx) => (
                    <TableRow key={`skeleton-${idx}`}>
                      <TableCell><Skeleton className="h-4 w-4" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-40" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-16" /></TableCell>
                      <TableCell><Skeleton className="h-4 w-24" /></TableCell>
                      <TableCell><Skeleton className="h-8 w-24" /></TableCell>
                    </TableRow>
                  ))
                )}
                {!loading && items.length === 0 && (
                  <TableRow>
                    <TableCell colSpan={9} className="py-10 text-center text-sm text-muted-foreground">
                      {error ?? "No destinations found."}
                    </TableCell>
                  </TableRow>
                )}
                {items.map((item) => {
                  const stats = window === "1h" ? item.stats_1h : item.stats_24h;
                  const health = window === "1h" ? item.endpoint_health_1h : item.endpoint_health_24h;
                  const stateBadge = (
                    <Badge variant="outline" className={cn("border", STATE_STYLES[item.state])}>
                      {item.state.replace("_", " ")}
                    </Badge>
                  );
                  const healthBadge = health?.status ? (
                    <Badge variant="outline" className={cn("border", HEALTH_STYLES[health.status])}>
                      {health.status}
                    </Badge>
                  ) : (
                    <span className="text-xs text-muted-foreground">--</span>
                  );

                  return (
                  <TableRow
                    key={item.destination_key ?? `${item.destination_type}-${item.destination ?? "unknown"}`}
                    className={cn("cursor-pointer", !item.destination_key && "opacity-60")}
                    onClick={() => {
                      if (!item.destination_key) {
                        toast({ title: "Destination unavailable", description: "Missing destination key for this row." });
                        return;
                      }
                      setSelectedKey(item.destination_key);
                    }}
                  >
                      <TableCell onClick={(event) => event.stopPropagation()}>
                        <Checkbox
                          checked={item.destination_key ? selectedKeys.has(item.destination_key) : false}
                          disabled={!item.destination_key}
                          onCheckedChange={(value) => {
                            if (!item.destination_key) return;
                            handleToggleSelect(item.destination_key, Boolean(value));
                          }}
                          aria-label={`Select ${item.destination_key ?? "destination"}`}
                        />
                      </TableCell>
                      <TableCell>
                        <div className="text-sm font-medium">{formatDestination(item.destination, item.destination_type)}</div>
                        <div className="text-xs text-muted-foreground">
                          {item.destination_key ? `${item.destination_key.slice(0, 12)}...` : "missing key"}
                        </div>
                      </TableCell>
                      <TableCell className="text-sm">{item.destination_type}</TableCell>
                      <TableCell>
                        <div className="space-y-1">
                          {stateBadge}
                          {item.state === "AUTO_PAUSED" && (
                            <div className="text-[10px] text-muted-foreground">
                              {item.ready_to_resume ? "ready to resume" : "not ready"}
                            </div>
                          )}
                        </div>
                      </TableCell>
                      <TableCell>{healthBadge}</TableCell>
                      <TableCell className="text-sm">{stats?.p95_ms ?? "--"}</TableCell>
                      <TableCell className="text-sm">{formatPercent(stats?.success_rate ?? null)}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {item.last_delivery ? `${item.last_delivery.status} - ${formatRelativeTime(item.last_delivery.created_at)}` : "--"}
                      </TableCell>
                      <TableCell onClick={(event) => event.stopPropagation()}>
                        {item.destination_key ? (
                          <div className="flex items-center gap-2">
                            {canOperate && item.state === "ACTIVE" && (
                              <Button size="sm" variant="outline" onClick={() => promptDestinationAction(item.destination_key, item.state, "PAUSED")}>Pause</Button>
                            )}
                            {canOperate && (item.state === "PAUSED" || item.state === "AUTO_PAUSED") && (
                              <Button size="sm" variant="outline" onClick={() => promptDestinationAction(item.destination_key, item.state, "ACTIVE")}>Resume</Button>
                            )}
                            {canDisable && item.state !== "DISABLED" && (
                              <Button size="sm" variant="destructive" onClick={() => promptDestinationAction(item.destination_key, item.state, "DISABLED")}>Disable</Button>
                            )}
                            {canDisable && item.state === "DISABLED" && (
                              <Button size="sm" variant="outline" onClick={() => promptDestinationAction(item.destination_key, item.state, "ACTIVE")}>Enable</Button>
                            )}
                          </div>
                        ) : (
                          <span className="text-xs text-muted-foreground">Actions unavailable</span>
                        )}
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {nextCursor && (
          <div className="flex justify-center">
            <Button variant="outline" disabled={loadingMore} onClick={() => loadDestinations(nextCursor, true)}>
              {loadingMore ? "Loading..." : "Load more"}
            </Button>
          </div>
        )}
      </div>

      <Sheet open={Boolean(selectedKey)} onOpenChange={(open) => !open && setSelectedKey(null)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
          {detailLoading || !selectedItem ? (
            <div className="space-y-4">
              <Skeleton className="h-6 w-40" />
              <Skeleton className="h-24 w-full" />
              <Skeleton className="h-40 w-full" />
            </div>
          ) : (
            <div className="space-y-6">
              <SheetHeader className="space-y-2">
                <SheetTitle className="text-xl">Destination</SheetTitle>
                <div className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
                  <Badge variant="outline">{selectedItem.destination_type}</Badge>
                  <span className="font-mono">{formatDestination(selectedItem.destination, selectedItem.destination_type)}</span>
                  <span className="text-xs">{selectedItem.destination_key}</span>
                  <Button
                    size="icon"
                    variant="ghost"
                    onClick={() => navigator.clipboard.writeText(selectedItem.destination_key)}
                    aria-label="Copy destination key"
                  >
                    <Copy className="h-4 w-4" />
                  </Button>
                  <Link href={`/settings/audit?resource_type=destination&resource_id=${selectedItem.destination_key}`}>
                    <a className="text-xs text-primary">View Audit</a>
                  </Link>
                  <Link href={`/incidents?destination_key=${selectedItem.destination_key}`}>
                    <a className="text-xs text-primary">View Incidents</a>
                  </Link>
                </div>
              </SheetHeader>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="space-y-3 py-4">
                  <div className="flex flex-wrap items-center gap-3">
                    <Badge variant="outline" className={cn("border", STATE_STYLES[selectedItem.state])}>
                      {selectedItem.state.replace("_", " ")}
                    </Badge>
                    {selectedItem.ready_to_resume && (
                      <Badge variant="outline" className="border-emerald-500/30 bg-emerald-500/10 text-emerald-400">
                        Ready to resume
                      </Badge>
                    )}
                  </div>
                  <div className="text-xs text-muted-foreground">
                    {selectedItem.reason ?? "No reason provided."}
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {canOperate && selectedItem.state === "ACTIVE" && (
                      <Button size="sm" variant="outline" onClick={() => promptDestinationAction(selectedItem.destination_key, selectedItem.state, "PAUSED")}>Pause</Button>
                    )}
                    {canOperate && (selectedItem.state === "PAUSED" || selectedItem.state === "AUTO_PAUSED") && (
                      <Button size="sm" variant="outline" onClick={() => promptDestinationAction(selectedItem.destination_key, selectedItem.state, "ACTIVE")}>Resume</Button>
                    )}
                    {canDisable && selectedItem.state !== "DISABLED" && (
                      <Button size="sm" variant="destructive" onClick={() => promptDestinationAction(selectedItem.destination_key, selectedItem.state, "DISABLED")}>Disable</Button>
                    )}
                    {canDisable && selectedItem.state === "DISABLED" && (
                      <Button size="sm" variant="outline" onClick={() => promptDestinationAction(selectedItem.destination_key, selectedItem.state, "ACTIVE")}>Enable</Button>
                    )}
                  </div>
                </CardContent>
              </Card>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="py-4">
                  <div className="text-sm font-semibold">Endpoint health (1h)</div>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <div>
                      <div className="text-xs text-muted-foreground">Status</div>
                      <div className="mt-1">
                        {detail?.endpoint_health?.status ? (
                          <Badge variant="outline" className={cn("border", HEALTH_STYLES[detail.endpoint_health.status])}>
                            {detail.endpoint_health.status}
                          </Badge>
                        ) : (
                          <span className="text-xs text-muted-foreground">No data</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Success rate</div>
                      <div className="mt-1 text-sm">{formatPercent(detail?.endpoint_health?.success_rate ?? null)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">p95 latency</div>
                      <div className="mt-1 text-sm">{detail?.endpoint_health?.p95_ms ?? "--"} ms</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Last success</div>
                      <div className="mt-1 text-sm">{formatRelativeTime(detail?.endpoint_health?.last_success_at ?? null)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Last failure</div>
                      <div className="mt-1 text-sm">{formatRelativeTime(detail?.endpoint_health?.last_failure_at ?? null)}</div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="py-4">
                  <div className="text-sm font-semibold">SLA (24h)</div>
                  <div className="mt-3 grid gap-3 md:grid-cols-2">
                    <div>
                      <div className="text-xs text-muted-foreground">Status</div>
                      <div className="mt-1">
                        {detail?.sla?.status ? (
                          <Badge variant="outline" className={cn("border", detail.sla.status === "AT_RISK" ? HEALTH_STYLES.DOWN : HEALTH_STYLES.OK)}>
                            {detail.sla.status}
                          </Badge>
                        ) : (
                          <span className="text-xs text-muted-foreground">No data</span>
                        )}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Success rate</div>
                      <div className="mt-1 text-sm">{formatPercent(detail?.sla?.success_rate ?? null)}</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">p95 latency</div>
                      <div className="mt-1 text-sm">{detail?.sla?.p95_ms ?? "--"} ms</div>
                    </div>
                    <div>
                      <div className="text-xs text-muted-foreground">Thresholds</div>
                      <div className="mt-1 text-sm">
                        {detail?.sla?.thresholds
                          ? `${detail.sla.thresholds.p95_ms ?? "--"} ms / ${Math.round((detail.sla.thresholds.success_rate_min ?? 0) * 100)}%`
                          : "--"}
                      </div>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="space-y-3 py-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold">Overrides</div>
                    {role === "OWNER" && (
                      <Button size="sm" variant="outline" onClick={() => setOverrideEditOpen(true)}>
                        Edit overrides
                      </Button>
                    )}
                  </div>
                  {overrideLoading ? (
                    <div className="space-y-2">
                      <Skeleton className="h-4 w-32" />
                      <Skeleton className="h-4 w-52" />
                      <Skeleton className="h-4 w-40" />
                    </div>
                  ) : overrideDetail ? (
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <div className="text-xs font-medium text-muted-foreground">Noise budget</div>
                          {renderSourceBadge(resolvedNoise?.source)}
                        </div>
                        <div className="text-sm">
                          {resolvedNoise?.enabled === false
                            ? "Disabled"
                            : `${resolvedNoise?.max_deliveries ?? "--"} per ${formatNoiseWindow(resolvedNoise)}`}
                        </div>
                        <div className="text-xs text-muted-foreground">
                          Used {resolvedNoise?.used_in_window ?? 0} in window
                        </div>
                      </div>
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <div className="text-xs font-medium text-muted-foreground">SLA thresholds</div>
                          {renderSourceBadge(resolvedSla24?.source)}
                        </div>
                        {resolvedSla24?.enabled === false || resolvedSla7d?.enabled === false ? (
                          <div className="text-sm">Disabled</div>
                        ) : (
                          <div className="space-y-1 text-sm">
                            <div>
                              24h: p95 {resolvedSla24?.p95_ms ?? "--"} ms - {resolvedSla24?.success_rate_min_pct ?? "--"}%
                            </div>
                            <div>
                              7d: p95 {resolvedSla7d?.p95_ms ?? "--"} ms - {resolvedSla7d?.success_rate_min_pct ?? "--"}%
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="text-xs text-muted-foreground">Overrides unavailable.</div>
                  )}
                </CardContent>
              </Card>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="space-y-4 py-4">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-semibold">Recent deliveries</div>
                    <Link href={`/alerts?destination_key=${selectedItem.destination_key}`}>
                      <a className="text-xs text-primary">View in Alerts</a>
                    </Link>
                  </div>
                  {detail?.recent_deliveries?.items?.length ? (
                    detail.recent_deliveries.items.map((delivery) => (
                      <div key={delivery.id} className="rounded-lg border border-border/60 bg-muted/30 p-3 text-xs">
                        <div className="flex items-center justify-between">
                          <span className="font-medium">{delivery.status}</span>
                          <span className="text-muted-foreground">{formatRelativeTime(delivery.created_at ?? delivery.sent_at ?? null)}</span>
                        </div>
                        {delivery.skip_reason && <div className="mt-1 text-muted-foreground">Skip: {delivery.skip_reason}</div>}
                        {delivery.cluster_summary && (
                          <div className="mt-1 text-muted-foreground">{delivery.cluster_summary}</div>
                        )}
                        {delivery.error && <div className="mt-1 text-muted-foreground">Error: {delivery.error}</div>}
                      </div>
                    ))
                  ) : (
                    <div className="text-xs text-muted-foreground">No recent deliveries.</div>
                  )}
                </CardContent>
              </Card>

              <Card className="border-border/60 bg-card/70">
                <CardContent className="space-y-4 py-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-sm font-semibold">Audit</div>
                    <div className="flex items-center gap-2 text-xs text-muted-foreground">
                      <span>Show system events</span>
                      <Switch checked={showSystemEvents} onCheckedChange={setShowSystemEvents} />
                    </div>
                  </div>
                  {filteredAuditItems.length ? (
                    <div className="space-y-2">
                      {filteredAuditItems.map((event) => {
                        const metadata = event.metadata ?? {};
                        const deliveryId = (metadata as any).delivery_id ?? (metadata as any).deliveryId ?? null;
                        const runId = (metadata as any).run_id ?? (metadata as any).runId ?? null;
                        return (
                          <div key={event.id} className="rounded-lg border border-border/60 bg-muted/30 p-3 text-xs">
                            <div className="flex items-center justify-between gap-2">
                              <div className="font-medium">{event.action ?? "Audit event"}</div>
                              <div className="text-muted-foreground">{formatRelativeTime(event.created_at ?? null)}</div>
                            </div>
                            <div className="mt-1 flex flex-wrap items-center gap-2 text-muted-foreground">
                              <span>{formatAuditActor(event)}</span>
                              {event.message ? <span>{event.message}</span> : null}
                            </div>
                            <div className="mt-2 flex flex-wrap items-center gap-2">
                              {deliveryId ? (
                                <Link href={`/alerts?delivery_id=${deliveryId}`}>
                                  <a className="text-xs text-primary">Open delivery</a>
                                </Link>
                              ) : null}
                              {runId ? (
                                <Link href={`/alerts?run_id=${runId}`}>
                                  <a className="text-xs text-primary">Open run</a>
                                </Link>
                              ) : null}
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => navigator.clipboard.writeText(JSON.stringify(event, null, 2))}
                              >
                                Copy event JSON
                              </Button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  ) : (
                    <div className="text-xs text-muted-foreground">No audit events yet.</div>
                  )}
                  {auditCursor && (
                    <Button size="sm" variant="outline" disabled={auditLoadingMore} onClick={loadMoreAudit}>
                      {auditLoadingMore ? "Loading..." : "Load more"}
                    </Button>
                  )}
                </CardContent>
              </Card>

              {playbook && (
                <Card className="border-border/60 bg-card/70">
                  <CardContent className="space-y-3 py-4">
                    <div className="text-sm font-semibold">Playbook</div>
                    <div className="text-xs text-muted-foreground">{playbook.title}</div>
                    <div className="grid gap-4 md:grid-cols-2">
                      <div>
                        <div className="text-xs font-medium text-muted-foreground">Checks</div>
                        <ul className="mt-2 list-disc space-y-1 pl-4 text-xs">
                          {playbook.checks.map((item) => (
                            <li key={item}>{item}</li>
                          ))}
                        </ul>
                      </div>
                      <div>
                        <div className="text-xs font-medium text-muted-foreground">Actions</div>
                        <ul className="mt-2 list-disc space-y-1 pl-4 text-xs">
                          {playbook.actions.map((item) => (
                            <li key={item}>{item}</li>
                          ))}
                        </ul>
                      </div>
                    </div>
                    {playbook.notes?.length ? (
                      <div className="text-xs text-muted-foreground">Notes: {playbook.notes.join(" ")}</div>
                    ) : null}
                  </CardContent>
                </Card>
              )}
            </div>
          )}
        </SheetContent>
      </Sheet>

      <Dialog open={overrideEditOpen} onOpenChange={setOverrideEditOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Edit Overrides</DialogTitle>
          </DialogHeader>
          <div className="space-y-6">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <div>
                  <Label>Noise budget</Label>
                  <div className="text-xs text-muted-foreground">Leave fields blank to inherit tenant defaults.</div>
                </div>
                <Switch
                  checked={overrideForm.noiseEnabled}
                  onCheckedChange={(checked) => setOverrideForm((prev) => ({ ...prev, noiseEnabled: checked }))}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1">
                  <Label>Window (minutes)</Label>
                  <Input
                    value={overrideForm.noiseWindowMinutes}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, noiseWindowMinutes: event.target.value }))}
                    disabled={!overrideForm.noiseEnabled}
                    placeholder="60"
                  />
                </div>
                <div className="space-y-1">
                  <Label>Max deliveries</Label>
                  <Input
                    value={overrideForm.noiseMaxDeliveries}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, noiseMaxDeliveries: event.target.value }))}
                    disabled={!overrideForm.noiseEnabled}
                    placeholder="20"
                  />
                </div>
              </div>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setOverrideForm((prev) => ({
                  ...prev,
                  noiseWindowMinutes: "",
                  noiseMaxDeliveries: "",
                }))}
              >
                Reset noise budget
              </Button>
            </div>

            <div className="space-y-3 border-t border-border/60 pt-4">
              <div className="flex items-center justify-between">
                <div>
                  <Label>SLA thresholds</Label>
                  <div className="text-xs text-muted-foreground">Overrides apply per destination.</div>
                </div>
                <Switch
                  checked={overrideForm.slaEnabled}
                  onCheckedChange={(checked) => setOverrideForm((prev) => ({ ...prev, slaEnabled: checked }))}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div className="space-y-1">
                  <Label>24h p95 (ms)</Label>
                  <Input
                    value={overrideForm.sla24P95}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, sla24P95: event.target.value }))}
                    disabled={!overrideForm.slaEnabled}
                    placeholder="5000"
                  />
                </div>
                <div className="space-y-1">
                  <Label>24h success %</Label>
                  <Input
                    value={overrideForm.sla24Success}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, sla24Success: event.target.value }))}
                    disabled={!overrideForm.slaEnabled}
                    placeholder="99"
                  />
                </div>
                <div className="space-y-1">
                  <Label>7d p95 (ms)</Label>
                  <Input
                    value={overrideForm.sla7dP95}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, sla7dP95: event.target.value }))}
                    disabled={!overrideForm.slaEnabled}
                    placeholder="8000"
                  />
                </div>
                <div className="space-y-1">
                  <Label>7d success %</Label>
                  <Input
                    value={overrideForm.sla7dSuccess}
                    onChange={(event) => setOverrideForm((prev) => ({ ...prev, sla7dSuccess: event.target.value }))}
                    disabled={!overrideForm.slaEnabled}
                    placeholder="99"
                  />
                </div>
              </div>
              <Button
                size="sm"
                variant="ghost"
                onClick={() => setOverrideForm((prev) => ({
                  ...prev,
                  sla24P95: "",
                  sla24Success: "",
                  sla7dP95: "",
                  sla7dSuccess: "",
                }))}
              >
                Reset SLA thresholds
              </Button>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOverrideEditOpen(false)}>Cancel</Button>
            <Button onClick={handleSaveOverrides}>Save overrides</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={Boolean(confirmAction)} onOpenChange={(open) => !open && setConfirmAction(null)}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Confirm destination change</DialogTitle>
          </DialogHeader>
          {confirmAction && (
            <div className="space-y-3 text-sm">
              <div className="text-muted-foreground">
                Destination <span className="font-mono text-foreground">{confirmAction.key}</span>
              </div>
              <div className="text-muted-foreground">
                {confirmAction.current} → <span className="font-medium text-foreground">{confirmAction.next}</span>
              </div>
              {(confirmAction.next === "PAUSED" || confirmAction.next === "DISABLED") && (
                <div className="text-xs text-muted-foreground">
                  This will block retries while the destination is {confirmAction.next.toLowerCase()}.
                </div>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setConfirmAction(null)}>Cancel</Button>
            <Button
              variant={confirmAction?.next === "DISABLED" ? "destructive" : "default"}
              onClick={async () => {
                if (!confirmAction) return;
                const { key, next } = confirmAction;
                setConfirmAction(null);
                await applyDestinationState(key, next);
              }}
            >
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
