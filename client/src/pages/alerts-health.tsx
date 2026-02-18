import { useEffect, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { apiFetchJson, getApiKey } from "@/lib/apiFetch";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/auth/useAuth";
import AlertsSubnav from "@/components/alerts-subnav";
import { ArrowLeft } from "lucide-react";
import { Link } from "wouter";

type HealthResponse = { status: string; [key: string]: any };
type MetricsResponse = { version: string; [key: string]: any };
type ThresholdItem = {
  destination_type: "WEBHOOK" | "EMAIL";
  p95_ms_threshold: number;
  success_rate_threshold: number;
  source: "DEFAULT" | "CUSTOM";
};
type NoiseBudgetItem = {
  destination_type: "WEBHOOK" | "EMAIL";
  max_deliveries: number;
  source: "DEFAULT" | "CUSTOM";
};
type EndpointHealthItem = {
  destination_type: "WEBHOOK" | "EMAIL";
  destination: string;
  status: "OK" | "DEGRADED" | "DOWN";
  success_rate: number;
  p95_ms: number | null;
  consecutive_failures: number;
  last_success_at: string | null;
};
type DestinationStateItem = {
  destination_type: "WEBHOOK" | "EMAIL";
  destination_key: string;
  destination?: string | null;
  state: "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
  reason?: string | null;
  paused_at?: string | null;
  auto_paused_at?: string | null;
  resume_ready_at?: string | null;
  health?: {
    window?: string | null;
    status?: "OK" | "DEGRADED" | "DOWN" | null;
    success_rate?: number | null;
    p95_ms?: number | null;
    last_success_at?: string | null;
    last_failure_at?: string | null;
  } | null;
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

export default function AlertsHealthPage() {
  const [alertsHealth, setAlertsHealth] = useState<HealthResponse | null>(null);
  const [webhooksHealth, setWebhooksHealth] = useState<HealthResponse | null>(null);
  const [dlqHealth, setDlqHealth] = useState<MetricsResponse | null>(null);
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [slaSummary, setSlaSummary] = useState<MetricsResponse | null>(null);
  const [thresholdWindow, setThresholdWindow] = useState<"24h" | "7d">("24h");
  const [thresholdItems, setThresholdItems] = useState<ThresholdItem[]>([]);
  const [thresholdDrafts, setThresholdDrafts] = useState<Record<string, { p95: string; success: string }>>({});
  const [thresholdLoading, setThresholdLoading] = useState(false);
  const [thresholdError, setThresholdError] = useState<string | null>(null);
  const [noiseWindow, setNoiseWindow] = useState<"24h" | "7d">("24h");
  const [noiseItems, setNoiseItems] = useState<NoiseBudgetItem[]>([]);
  const [noiseDrafts, setNoiseDrafts] = useState<Record<string, string>>({});
  const [noiseLoading, setNoiseLoading] = useState(false);
  const [noiseError, setNoiseError] = useState<string | null>(null);
  const [endpointWindow, setEndpointWindow] = useState<"1h" | "24h">("24h");
  const [endpointItems, setEndpointItems] = useState<EndpointHealthItem[]>([]);
  const [endpointSummary, setEndpointSummary] = useState<{ OK: number; DEGRADED: number; DOWN: number; total: number } | null>(null);
  const [endpointLoading, setEndpointLoading] = useState(false);
  const [endpointError, setEndpointError] = useState<string | null>(null);
  const [destinationWindow, setDestinationWindow] = useState<"1h" | "24h">("1h");
  const [destinationItems, setDestinationItems] = useState<DestinationStateItem[]>([]);
  const [destinationLoading, setDestinationLoading] = useState(false);
  const [destinationError, setDestinationError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [apiKey, setApiKey] = useState(() => (getApiKey() ?? ""));
  const { toast } = useToast();
  const { role } = useAuth();
  const canEditThresholds = role === "OWNER";
  const canEditNoise = role === "OWNER";
  const canComputeEndpoints = role === "OWNER";
  const canEditDestinations = role === "OWNER" || role === "OPERATOR";
  const canDisableDestinations = role === "OWNER";

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const [alertsRes, webhooksRes, dlqRes, metricsRes, slaRes] = await Promise.all([
          apiFetchJson("/health/alerts", { signal: controller.signal, headers }),
          apiFetchJson("/health/webhooks", { signal: controller.signal, headers }),
          apiFetchJson("/api/alerts/dlq-health", { signal: controller.signal, headers }),
          apiFetchJson("/api/alerts/metrics?days=30", { signal: controller.signal, headers }),
          apiFetchJson("/v1/alert-slas/summary", { signal: controller.signal, headers }),
        ]);
        setAlertsHealth(alertsRes);
        setWebhooksHealth(webhooksRes);
        setDlqHealth(dlqRes);
        setMetrics(metricsRes);
        setSlaSummary(slaRes);
      } catch {
        setAlertsHealth(null);
        setWebhooksHealth(null);
        setDlqHealth(null);
        setMetrics(null);
        setSlaSummary(null);
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [apiKey]);

  useEffect(() => {
    const controller = new AbortController();
    const loadThresholds = async () => {
      setThresholdLoading(true);
      setThresholdError(null);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const payload = await apiFetchJson(`/v1/alert-sla-thresholds?window=${thresholdWindow}`, {
          signal: controller.signal,
          headers,
        });
        const items = Array.isArray(payload?.items) ? payload.items : [];
        setThresholdItems(items);
        const drafts = items.reduce<Record<string, { p95: string; success: string }>>((acc, item) => {
          acc[item.destination_type] = {
            p95: String(item.p95_ms_threshold ?? ""),
            success: String(Number((item.success_rate_threshold * 100).toFixed(2))),
          };
          return acc;
        }, {});
        setThresholdDrafts(drafts);
      } catch (error: any) {
        setThresholdError(error?.message ?? "Unable to load SLA thresholds");
        setThresholdItems([]);
      } finally {
        setThresholdLoading(false);
      }
    };
    loadThresholds();
    return () => controller.abort();
  }, [apiKey, thresholdWindow]);

  useEffect(() => {
    const controller = new AbortController();
    const loadNoiseBudgets = async () => {
      setNoiseLoading(true);
      setNoiseError(null);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const payload = await apiFetchJson(`/v1/alert-noise-budgets?window=${noiseWindow}`, {
          signal: controller.signal,
          headers,
        });
        const items = Array.isArray(payload?.items) ? payload.items : [];
        setNoiseItems(items);
        const drafts = items.reduce<Record<string, string>>((acc, item) => {
          acc[item.destination_type] = String(item.max_deliveries ?? "");
          return acc;
        }, {});
        setNoiseDrafts(drafts);
      } catch (error: any) {
        setNoiseError(error?.message ?? "Unable to load noise budgets");
        setNoiseItems([]);
      } finally {
        setNoiseLoading(false);
      }
    };
    loadNoiseBudgets();
    return () => controller.abort();
  }, [apiKey, noiseWindow]);

  useEffect(() => {
    const controller = new AbortController();
    const loadEndpointHealth = async () => {
      setEndpointLoading(true);
      setEndpointError(null);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const payload = await apiFetchJson(`/v1/alert-endpoints?window=${endpointWindow}`, {
          signal: controller.signal,
          headers,
        });
        const items = Array.isArray(payload?.items) ? payload.items : [];
        setEndpointItems(items);
        setEndpointSummary(payload?.summary ?? null);
      } catch (error: any) {
        setEndpointError(error?.message ?? "Unable to load endpoint health");
        setEndpointItems([]);
        setEndpointSummary(null);
      } finally {
        setEndpointLoading(false);
      }
    };
    loadEndpointHealth();
    return () => controller.abort();
  }, [apiKey, endpointWindow]);

  useEffect(() => {
    const controller = new AbortController();
    const loadDestinations = async () => {
      setDestinationLoading(true);
      setDestinationError(null);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const payload = await apiFetchJson(`/v1/alert-destinations?window=${destinationWindow}`, {
          signal: controller.signal,
          headers,
        });
        const items = Array.isArray(payload?.items) ? payload.items : [];
        setDestinationItems(items);
      } catch (error: any) {
        setDestinationError(error?.message ?? "Unable to load destination controls");
        setDestinationItems([]);
      } finally {
        setDestinationLoading(false);
      }
    };
    loadDestinations();
    return () => controller.abort();
  }, [apiKey, destinationWindow]);

  const refreshDestinations = async () => {
    try {
      const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
      const payload = await apiFetchJson(`/v1/alert-destinations?window=${destinationWindow}`, { headers });
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setDestinationItems(items);
    } catch (error: any) {
      setDestinationError(error?.message ?? "Unable to load destination controls");
      setDestinationItems([]);
    }
  };

  const updateDestinationState = async (item: DestinationStateItem, state: DestinationStateItem["state"]) => {
    if (!apiKey) {
      toast({ title: "API key required", description: "Set an API key to update destination state.", variant: "destructive" });
      return;
    }
    try {
      const headers = { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json" };
      await apiFetchJson("/v1/alert-destinations/state", {
        method: "PATCH",
        headers,
        body: JSON.stringify({
          destination_type: item.destination_type,
          destination_key: item.destination_key,
          state,
          reason: item.reason ?? null,
        }),
      });
      toast({ title: "Destination updated", description: `State set to ${state}.` });
      await refreshDestinations();
    } catch (error: any) {
      toast({ title: "Update failed", description: error?.message ?? "Unable to update destination.", variant: "destructive" });
    }
  };

  const computeAutoPause = async () => {
    if (!apiKey) return;
    try {
      const headers = { Authorization: `Bearer ${apiKey}` };
      await apiFetchJson(`/api/admin/alerts/endpoints/auto-pause/compute?window=${destinationWindow}`, {
        method: "POST",
        headers,
      });
      await refreshDestinations();
      toast({ title: "Auto-pause computed", description: "Destination states refreshed." });
    } catch (error: any) {
      toast({ title: "Auto-pause failed", description: error?.message ?? "Unable to compute auto-pause.", variant: "destructive" });
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-6">
          <div className="flex items-center gap-3">
            <Link href="/platform">
              <Button variant="ghost" size="sm">
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back to Menu
              </Button>
            </Link>
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Alert Ops Health</h1>
          <p className="text-sm text-muted-foreground">
            Live health and metrics for alerting reliability.
          </p>
          <AlertsSubnav />
        </div>
      </div>

      <div className="container mx-auto px-6 py-8 space-y-6">
        {import.meta.env.MODE !== "production" && (
          <Card className="border-border/60 bg-card/70">
            <CardContent className="p-5 space-y-3">
              <p className="text-sm font-semibold text-foreground">Dev API Key</p>
              <div className="flex flex-col gap-3 md:flex-row md:items-center">
                <Input
                  placeholder="Paste API key (vs_demo_...)"
                  value={apiKey}
                  onChange={(event) => setApiKey(event.target.value)}
                />
                <Button
                  onClick={() => {
                    if (typeof window !== "undefined") {
                      window.localStorage.setItem("api_key", apiKey);
                    }
                  }}
                >
                  Save key
                </Button>
                <Button
                  variant="outline"
                  onClick={() => {
                    if (typeof window !== "undefined") {
                      window.localStorage.removeItem("api_key");
                      setApiKey("");
                    }
                  }}
                >
                  Clear
                </Button>
              </div>
            </CardContent>
          </Card>
        )}
        {[{
          title: "/health/alerts",
          payload: alertsHealth,
        }, {
          title: "/health/webhooks",
          payload: webhooksHealth,
        }, {
          title: "/api/alerts/dlq-health",
          payload: dlqHealth,
        }, {
          title: "/api/alerts/metrics?days=30",
          payload: metrics,
        }, {
          title: "/v1/alert-slas/summary",
          payload: slaSummary,
        }].map((section) => (
          <Card key={section.title} className="border-border/60 bg-card/70">
            <CardContent className="p-5">
              <p className="text-sm font-semibold text-foreground mb-3">{section.title}</p>
              {loading ? (
                <Skeleton className="h-24 w-full" />
              ) : section.payload ? (
                <pre className="whitespace-pre-wrap text-xs text-muted-foreground">
                  {JSON.stringify(section.payload, null, 2)}
                </pre>
              ) : (
                <p className="text-sm text-muted-foreground">No data available.</p>
              )}
            </CardContent>
          </Card>
        ))}

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-foreground">SLA thresholds</p>
                <p className="text-xs text-muted-foreground">Customize p95 latency + success rate per destination.</p>
              </div>
              <div className="flex items-center gap-2">
                {(["24h", "7d"] as const).map((window) => (
                  <Button
                    key={window}
                    size="sm"
                    variant={thresholdWindow === window ? "default" : "outline"}
                    onClick={() => setThresholdWindow(window)}
                  >
                    {window}
                  </Button>
                ))}
              </div>
            </div>
            {thresholdLoading ? (
              <Skeleton className="h-16 w-full" />
            ) : thresholdError ? (
              <p className="text-xs text-destructive">{thresholdError}</p>
            ) : (
              <div className="grid gap-4 md:grid-cols-2">
                {thresholdItems.map((item) => (
                  <div key={item.destination_type} className="rounded-lg border border-border/60 bg-background/40 p-4 space-y-3">
                    <div className="flex items-center justify-between">
                      <p className="text-sm font-semibold">{item.destination_type}</p>
                      <span className="text-[11px] text-muted-foreground">{item.source}</span>
                    </div>
                    <div className="grid gap-3">
                      <div>
                        <label className="text-xs text-muted-foreground">p95 threshold (ms)</label>
                        <Input
                          type="number"
                          min={1}
                          value={thresholdDrafts[item.destination_type]?.p95 ?? ""}
                          onChange={(event) =>
                            setThresholdDrafts((prev) => ({
                              ...prev,
                              [item.destination_type]: {
                                p95: event.target.value,
                                success: prev[item.destination_type]?.success ?? "",
                              },
                            }))
                          }
                          disabled={!canEditThresholds}
                          className="mt-2"
                        />
                      </div>
                      <div>
                        <label className="text-xs text-muted-foreground">Success rate (%)</label>
                        <Input
                          type="number"
                          min={0}
                          max={100}
                          step="0.1"
                          value={thresholdDrafts[item.destination_type]?.success ?? ""}
                          onChange={(event) =>
                            setThresholdDrafts((prev) => ({
                              ...prev,
                              [item.destination_type]: {
                                p95: prev[item.destination_type]?.p95 ?? "",
                                success: event.target.value,
                              },
                            }))
                          }
                          disabled={!canEditThresholds}
                          className="mt-2"
                        />
                      </div>
                    </div>
                    {canEditThresholds ? (
                      <Button
                        size="sm"
                        onClick={async () => {
                          const draft = thresholdDrafts[item.destination_type];
                          const p95 = Number(draft?.p95);
                          const successPct = Number(draft?.success);
                          if (!Number.isFinite(p95) || p95 <= 0 || !Number.isFinite(successPct) || successPct < 0 || successPct > 100) {
                            toast({
                              title: "Invalid thresholds",
                              description: "Enter a positive p95 and success rate between 0 and 100.",
                              variant: "destructive",
                            });
                            return;
                          }
                          try {
                            await apiFetchJson("/v1/alert-sla-thresholds", {
                              method: "PATCH",
                              headers: { "Content-Type": "application/json" },
                              body: JSON.stringify({
                                window: thresholdWindow,
                                destination_type: item.destination_type,
                                p95_ms_threshold: p95,
                                success_rate_threshold: successPct / 100,
                              }),
                            });
                            toast({ title: "Threshold updated", description: `${item.destination_type} thresholds saved.` });
                            setThresholdItems((prev) =>
                              prev.map((row) =>
                                row.destination_type === item.destination_type
                                  ? {
                                      ...row,
                                      p95_ms_threshold: p95,
                                      success_rate_threshold: successPct / 100,
                                      source: "CUSTOM",
                                    }
                                  : row,
                              ),
                            );
                          } catch (error: any) {
                            toast({
                              title: "Update failed",
                              description: error?.message ?? "Unable to update thresholds.",
                              variant: "destructive",
                            });
                          }
                        }}
                      >
                        Save
                      </Button>
                    ) : (
                      <p className="text-xs text-muted-foreground">Owner role required to edit thresholds.</p>
                    )}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-foreground">Endpoint health</p>
                <p className="text-xs text-muted-foreground">Passive success rate and latency by destination.</p>
              </div>
              <div className="flex items-center gap-2">
                {(["1h", "24h"] as const).map((window) => (
                  <Button
                    key={window}
                    size="sm"
                    variant={endpointWindow === window ? "default" : "outline"}
                    onClick={() => setEndpointWindow(window)}
                  >
                    {window}
                  </Button>
                ))}
                {import.meta.env.MODE !== "production" && canComputeEndpoints && (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={async () => {
                      try {
                        await apiFetchJson(`/api/admin/alerts/endpoints/compute?window=${endpointWindow}`, { method: "POST" });
                        toast({ title: "Endpoint health updated", description: "Latest metrics computed." });
                        setEndpointWindow((prev) => prev);
                      } catch (error: any) {
                        toast({
                          title: "Compute failed",
                          description: error?.message ?? "Unable to compute endpoint health.",
                          variant: "destructive",
                        });
                      }
                    }}
                  >
                    Compute
                  </Button>
                )}
              </div>
            </div>
            {endpointLoading ? (
              <Skeleton className="h-16 w-full" />
            ) : endpointError ? (
              <p className="text-xs text-destructive">{endpointError}</p>
            ) : endpointItems.length === 0 ? (
              <p className="text-xs text-muted-foreground">No endpoint activity in this window.</p>
            ) : (
              <div className="space-y-3">
                <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                  <span>OK: {endpointSummary?.OK ?? 0}</span>
                  <span>DEGRADED: {endpointSummary?.DEGRADED ?? 0}</span>
                  <span>DOWN: {endpointSummary?.DOWN ?? 0}</span>
                  <span>Total: {endpointSummary?.total ?? endpointItems.length}</span>
                </div>
                <div className="space-y-2">
                  {endpointItems.map((item) => (
                    <div key={`${item.destination_type}-${item.destination}`} className="rounded-lg border border-border/60 bg-background/40 p-3 text-xs text-muted-foreground">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="text-foreground">{item.destination_type}</span>
                        <span className={item.status === "OK" ? "text-emerald-400" : item.status === "DEGRADED" ? "text-orange-300" : "text-red-400"}>
                          {item.status}
                        </span>
                      </div>
                      <div className="mt-2 truncate text-xs">{item.destination}</div>
                      <div className="mt-2 grid gap-2 md:grid-cols-3">
                        <div>Success {Math.round(item.success_rate * 100)}%</div>
                        <div>p95 {item.p95_ms ?? "--"}ms</div>
                        <div>Consecutive fails {item.consecutive_failures}</div>
                      </div>
                      <div className="mt-2 text-[11px] text-muted-foreground">
                        Last success {formatRelativeTime(item.last_success_at)}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-foreground">Destination controls</p>
                <p className="text-xs text-muted-foreground">Pause, resume, or disable alert delivery per destination.</p>
              </div>
              <div className="flex items-center gap-2">
                {(["1h", "24h"] as const).map((window) => (
                  <Button
                    key={window}
                    size="sm"
                    variant={destinationWindow === window ? "default" : "outline"}
                    onClick={() => setDestinationWindow(window)}
                  >
                    {window}
                  </Button>
                ))}
                {import.meta.env.MODE !== "production" && canComputeEndpoints && (
                  <Button size="sm" variant="outline" onClick={computeAutoPause}>
                    Auto-pause compute
                  </Button>
                )}
              </div>
            </div>
            {destinationLoading ? (
              <Skeleton className="h-16 w-full" />
            ) : destinationError ? (
              <p className="text-xs text-destructive">{destinationError}</p>
            ) : destinationItems.length === 0 ? (
              <p className="text-xs text-muted-foreground">No destinations discovered yet.</p>
            ) : (
              <div className="space-y-3">
                {destinationItems.map((item) => {
                  const resumeReady = Boolean(item.resume_ready_at);
                  const destinationLabel = item.destination
                    ? item.destination
                    : item.destination_key
                      ? `key:${item.destination_key.slice(0, 8)}…`
                      : "--";
                  return (
                    <div
                      key={`${item.destination_type}-${item.destination_key}`}
                      className="rounded-lg border border-border/60 bg-background/40 p-3 text-xs text-muted-foreground"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <div className="text-foreground">{item.destination_type}</div>
                        <div className="flex items-center gap-2">
                          <span className={
                            item.state === "ACTIVE"
                              ? "text-emerald-400"
                              : item.state === "AUTO_PAUSED"
                                ? "text-orange-300"
                                : item.state === "PAUSED"
                                  ? "text-yellow-300"
                                  : "text-red-400"
                          }>
                            {item.state}
                          </span>
                        </div>
                      </div>
                      <div className="mt-2 truncate text-xs">{destinationLabel}</div>
                      <div className="mt-2 grid gap-2 md:grid-cols-3">
                        <div>Health: {item.health?.status ?? "UNKNOWN"}</div>
                        <div>p95: {item.health?.p95_ms ?? "--"}ms</div>
                        <div>Success: {item.health?.success_rate != null ? `${Math.round(item.health.success_rate * 100)}%` : "--"}</div>
                      </div>
                      {item.reason && (
                        <div className="mt-2 text-[11px] text-muted-foreground">Reason: {item.reason}</div>
                      )}
                      <div className="mt-3 flex flex-wrap gap-2">
                        {canEditDestinations ? (
                          <>
                            {item.state === "ACTIVE" && (
                              <Button size="sm" variant="outline" onClick={() => updateDestinationState(item, "PAUSED")}>
                                Pause
                              </Button>
                            )}
                            {item.state === "PAUSED" && (
                              <Button size="sm" variant="outline" onClick={() => updateDestinationState(item, "ACTIVE")}>
                                Resume
                              </Button>
                            )}
                            {item.state === "AUTO_PAUSED" && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!resumeReady}
                                onClick={() => updateDestinationState(item, "ACTIVE")}
                              >
                                {resumeReady ? "Resume" : "Resume (not ready)"}
                              </Button>
                            )}
                            {item.state === "DISABLED" && (
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!canDisableDestinations}
                                onClick={() => updateDestinationState(item, "ACTIVE")}
                              >
                                Enable
                              </Button>
                            )}
                            {item.state !== "DISABLED" && canDisableDestinations && (
                              <Button size="sm" variant="destructive" onClick={() => updateDestinationState(item, "DISABLED")}>
                                Disable
                              </Button>
                            )}
                          </>
                        ) : (
                          <p className="text-xs text-muted-foreground">Operator role required to manage destinations.</p>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
              <div>
                <p className="text-sm font-semibold text-foreground">Noise budgets</p>
                <p className="text-xs text-muted-foreground">Cap alert volume per destination and window.</p>
              </div>
              <div className="flex items-center gap-2">
                {(["24h", "7d"] as const).map((window) => (
                  <Button
                    key={window}
                    size="sm"
                    variant={noiseWindow === window ? "default" : "outline"}
                    onClick={() => setNoiseWindow(window)}
                  >
                    {window}
                  </Button>
                ))}
              </div>
            </div>
            {noiseLoading ? (
              <Skeleton className="h-16 w-full" />
            ) : noiseError ? (
              <p className="text-xs text-destructive">{noiseError}</p>
            ) : (
              <div className="grid gap-4 md:grid-cols-2">
                {noiseItems.map((item) => (
                  <div key={item.destination_type} className="rounded-lg border border-border/60 bg-background/40 p-4 space-y-3">
                    <div className="flex items-center justify-between">
                      <p className="text-sm font-semibold">{item.destination_type}</p>
                      <span className="text-[11px] text-muted-foreground">{item.source}</span>
                    </div>
                    <div>
                      <label className="text-xs text-muted-foreground">Max deliveries</label>
                      <Input
                        type="number"
                        min={1}
                        value={noiseDrafts[item.destination_type] ?? ""}
                        onChange={(event) =>
                          setNoiseDrafts((prev) => ({
                            ...prev,
                            [item.destination_type]: event.target.value,
                          }))
                        }
                        disabled={!canEditNoise}
                        className="mt-2"
                      />
                    </div>
                    {canEditNoise ? (
                      <Button
                        size="sm"
                        onClick={async () => {
                          const draftValue = Number(noiseDrafts[item.destination_type]);
                          if (!Number.isFinite(draftValue) || draftValue <= 0) {
                            toast({
                              title: "Invalid value",
                              description: "Enter a positive delivery cap.",
                              variant: "destructive",
                            });
                            return;
                          }
                          try {
                            await apiFetchJson("/v1/alert-noise-budgets", {
                              method: "PATCH",
                              headers: { "Content-Type": "application/json" },
                              body: JSON.stringify({
                                window: noiseWindow,
                                destination_type: item.destination_type,
                                max_deliveries: Math.floor(draftValue),
                              }),
                            });
                            toast({ title: "Noise budget updated", description: `${item.destination_type} cap saved.` });
                            setNoiseItems((prev) =>
                              prev.map((row) =>
                                row.destination_type === item.destination_type
                                  ? { ...row, max_deliveries: Math.floor(draftValue), source: "CUSTOM" }
                                  : row,
                              ),
                            );
                          } catch (error: any) {
                            toast({
                              title: "Update failed",
                              description: error?.message ?? "Unable to update noise budget.",
                              variant: "destructive",
                            });
                          }
                        }}
                      >
                        Save
                      </Button>
                    ) : (
                      <p className="text-xs text-muted-foreground">Owner role required to edit noise budgets.</p>
                    )}
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
