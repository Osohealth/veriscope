import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useLocation } from "wouter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import { LoadingState } from "@/components/loading-state";
import { cn } from "@/lib/utils";
import { getAuthHeaders } from "@/lib/queryClient";
import { TerminalStoreProvider, useTerminalStore } from "@/hooks/use-terminal-store";
import { buildAlertContextLink, buildAlertSelection } from "@/lib/alert-context";
import { createInvestigation } from "@/lib/investigations";
import { saveView } from "@/lib/saved-views";
import { useToast } from "@/hooks/use-toast";
import {
  AlertTriangle,
  ArrowLeft,
  CheckCircle2,
  Clock,
  Download,
  Filter,
  RefreshCw,
  Search,
  ShieldAlert,
  Target,
  XCircle,
} from "lucide-react";

type Incident = {
  id: string;
  type: string;
  destination_key?: string | null;
  status: "OPEN" | "ACKED" | "RESOLVED";
  severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  title: string;
  summary: string;
  opened_at: string;
  acked_at?: string | null;
  resolved_at?: string | null;
};

type IncidentsResponse = {
  items: Incident[];
  summary?: {
    open_count: number;
    acked_count: number;
    resolved_count: number;
  };
};

type AlertRule = {
  id: string;
  name: string;
  type: string;
  severity: string;
  is_active: boolean;
  is_muted?: boolean;
  conditions: Record<string, any>;
  channels?: string[];
  cooldown_minutes?: number;
};

const COMMODITIES = ["Crude", "Products", "LNG", "LPG", "Dry Bulk", "Metals", "Agri"];
const MODES = ["Sea", "Air", "Rail", "Cross-modal"];
const TIME_MODES = ["Live", "24h", "7d", "30d"];

const buildQuery = (filters: any) => {
  const params = new URLSearchParams();
  if (filters.status) params.set("status", filters.status);
  if (filters.severity) params.set("severity_min", filters.severity);
  params.set("limit", "100");
  return params.toString();
};

const severityStyles: Record<string, string> = {
  CRITICAL: "bg-red-500/20 text-red-300 border-red-500/40",
  HIGH: "bg-orange-500/20 text-orange-300 border-orange-500/40",
  MEDIUM: "bg-amber-500/20 text-amber-300 border-amber-500/40",
  LOW: "bg-emerald-500/20 text-emerald-300 border-emerald-500/40",
};

const statusStyles: Record<string, string> = {
  OPEN: "bg-sky-500/15 text-sky-300 border-sky-500/30",
  ACKED: "bg-indigo-500/15 text-indigo-300 border-indigo-500/30",
  RESOLVED: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
};

const severityRank: Record<string, number> = {
  CRITICAL: 4,
  HIGH: 3,
  MEDIUM: 2,
  LOW: 1,
};

const formatAge = (value: string) => {
  const ms = Date.now() - new Date(value).getTime();
  if (Number.isNaN(ms) || ms < 0) return "-";
  const minutes = Math.floor(ms / 60000);
  if (minutes < 1) return "just now";
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
};

function AlertsCommandContent() {
  const { filters, updateFilters, resetFilters, selectedEntity, setSelectedEntity } = useTerminalStore();
  const [search, setSearch] = useState("");
  const [state, setState] = useState<"idle" | "loading" | "error">("idle");
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [summary, setSummary] = useState<IncidentsResponse["summary"] | null>(null);
  const [selectedIncident, setSelectedIncident] = useState<Incident | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [demoError, setDemoError] = useState<string | null>(null);
  const [creatingDemo, setCreatingDemo] = useState(false);
  const [rulesState, setRulesState] = useState<"idle" | "loading" | "error">("idle");
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [rulesDialogOpen, setRulesDialogOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null);
  const [, setLocation] = useLocation();
  const incidentsRequestIdRef = useRef(0);
  const rulesRequestIdRef = useRef(0);
  const [ruleForm, setRuleForm] = useState({
    name: "",
    type: "congestion",
    severity: "high",
    target: "",
    threshold: "",
    cooldownMinutes: 60,
    channels: ["in_app"] as string[],
  });
  const { toast } = useToast();

  const query = useMemo(() => buildQuery(filters), [filters]);

  useEffect(() => {
    if (!filters.status) updateFilters({ status: "OPEN" });
    if (!filters.severity) updateFilters({ severity: "HIGH" });
  }, [filters.severity, filters.status, updateFilters]);

  const loadIncidents = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++incidentsRequestIdRef.current;
      setState("loading");
      try {
        const res = await fetch(`/v1/incidents?${query}`, { signal, headers: getAuthHeaders() });
        if (!res.ok) throw new Error("Failed to load incidents");
        const data: IncidentsResponse = await res.json();
        const items = Array.isArray(data.items) ? data.items : [];
        if (requestId !== incidentsRequestIdRef.current) return;
        setIncidents(items);
        setSummary(data.summary ?? null);
        if (selectedIncident) {
          const next = items.find((item) => item.id === selectedIncident.id);
          if (next) {
            setSelectedIncident(next);
          } else {
            setSelectedIncident(null);
            setDrawerOpen(false);
          }
        }
        setState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== incidentsRequestIdRef.current) return;
        setState("error");
      }
    },
    [query, selectedIncident]
  );

  const loadRules = useCallback(
    async (signal?: AbortSignal) => {
      const requestId = ++rulesRequestIdRef.current;
      setRulesState("loading");
      try {
        const res = await fetch("/v1/alert-rules", { signal, headers: getAuthHeaders() });
        if (!res.ok) throw new Error("Failed to load rules");
        const data = await res.json();
        if (requestId !== rulesRequestIdRef.current) return;
        setRules(Array.isArray(data.items) ? data.items : []);
        setRulesState("idle");
      } catch (error) {
        if ((error as Error)?.name === "AbortError") return;
        if (requestId !== rulesRequestIdRef.current) return;
        setRulesState("error");
      }
    },
    []
  );

  const refreshIncidents = useCallback(() => {
    const controller = new AbortController();
    loadIncidents(controller.signal);
    return controller;
  }, [loadIncidents]);

  const refreshRules = useCallback(() => {
    const controller = new AbortController();
    loadRules(controller.signal);
    return controller;
  }, [loadRules]);

  const handleCreateDemo = async () => {
    setDemoError(null);
    setCreatingDemo(true);
    try {
      const res = await fetch("/api/dev/incidents/create-demo", {
        method: "POST",
        headers: getAuthHeaders({ "Content-Type": "application/json" }),
      });
      if (!res.ok) {
        const data = await res.json().catch(() => null);
        throw new Error(data?.error || "Failed to create demo incident");
      }
      await loadIncidents();
    } catch (error) {
      setDemoError((error as Error)?.message || "Failed to create demo incident");
    } finally {
      setCreatingDemo(false);
    }
  };

  useEffect(() => {
    const controller = refreshIncidents();
    return () => controller.abort();
  }, [query]);

  useEffect(() => {
    const controller = refreshRules();
    return () => controller.abort();
  }, [refreshRules]);

  const filteredIncidents = incidents.filter((item) => {
    if (!search) return true;
    const value = `${item.title} ${item.summary}`.toLowerCase();
    return value.includes(search.toLowerCase());
  });

  const sortedIncidents = useMemo(() => {
    return [...filteredIncidents].sort((a, b) => {
      const severityDelta = (severityRank[b.severity] ?? 0) - (severityRank[a.severity] ?? 0);
      if (severityDelta !== 0) return severityDelta;
      return new Date(b.opened_at).getTime() - new Date(a.opened_at).getTime();
    });
  }, [filteredIncidents]);

  const openDrawer = (incident: Incident) => {
    setSelectedIncident(incident);
    setSelectedEntity(buildAlertSelection(incident));
    setDrawerOpen(true);
  };

  const resetRuleForm = () => {
    setRuleForm({
      name: "",
      type: "congestion",
      severity: "high",
      target: "",
      threshold: "",
      cooldownMinutes: 60,
      channels: ["in_app"],
    });
  };

  const openCreateRule = () => {
    setEditingRule(null);
    resetRuleForm();
    setRulesDialogOpen(true);
  };

  const openEditRule = (rule: AlertRule) => {
    setEditingRule(rule);
    const conditions = rule.conditions ?? {};
    setRuleForm({
      name: rule.name ?? "",
      type: rule.type ?? "congestion",
      severity: rule.severity ?? "high",
      target: conditions.target ?? "",
      threshold: conditions.threshold ?? "",
      cooldownMinutes: rule.cooldown_minutes ?? 60,
      channels: Array.isArray(rule.channels) && rule.channels.length ? rule.channels : ["in_app"],
    });
    setRulesDialogOpen(true);
  };

  const saveRule = async () => {
    const payload = {
      name: ruleForm.name,
      type: ruleForm.type,
      severity: ruleForm.severity,
      conditions: {
        target: ruleForm.target,
        threshold: ruleForm.threshold === "" ? undefined : Number(ruleForm.threshold),
        operator: ">",
        metric: "queue",
      },
      channels: ruleForm.channels,
      cooldown_minutes: ruleForm.cooldownMinutes,
      is_active: editingRule?.is_active ?? true,
    };
    const url = editingRule ? `/v1/alert-rules/${editingRule.id}` : "/v1/alert-rules";
    const method = editingRule ? "PATCH" : "POST";
    const res = await fetch(url, {
      method,
      headers: getAuthHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      toast({ title: "Failed to save rule", variant: "destructive" });
      return;
    }
    await loadRules();
    setRulesDialogOpen(false);
    setEditingRule(null);
    resetRuleForm();
    toast({ title: editingRule ? "Rule updated" : "Rule created" });
  };

  const toggleRule = async (rule: AlertRule) => {
    const res = await fetch(`/v1/alert-rules/${rule.id}`, {
      method: "PATCH",
      headers: getAuthHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify({ is_active: !rule.is_active }),
    });
    if (!res.ok) {
      toast({ title: "Failed to update rule", variant: "destructive" });
      return;
    }
    await loadRules();
  };

  const deleteRule = async (rule: AlertRule) => {
    const res = await fetch(`/v1/alert-rules/${rule.id}`, {
      method: "DELETE",
      headers: getAuthHeaders(),
    });
    if (!res.ok) {
      toast({ title: "Failed to delete rule", variant: "destructive" });
      return;
    }
    await loadRules();
  };

  const handleAck = async () => {
    if (!selectedIncident) return;
    await fetch(`/v1/incidents/${selectedIncident.id}/ack`, {
      method: "POST",
      headers: getAuthHeaders({ "Content-Type": "application/json" }),
    });
    await loadIncidents();
  };

  const handleResolve = async () => {
    if (!selectedIncident) return;
    await fetch(`/v1/incidents/${selectedIncident.id}/resolve`, {
      method: "POST",
      headers: getAuthHeaders({ "Content-Type": "application/json" }),
    });
    await loadIncidents();
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
            <ShieldAlert className="h-5 w-5 text-primary" />
            Alerts
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
                placeholder="Search alert title"
                className="h-8 border-none bg-transparent px-0 text-sm shadow-none focus-visible:ring-0"
              />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Badge variant="outline" className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-emerald-400" />
              Freshness 24s
            </Badge>
            <Badge variant="outline" className="flex items-center gap-2">
              <Target className="h-3.5 w-3.5 text-sky-400" />
              Confidence 0.81
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                const name = window.prompt("Name this view", "Alerts view");
                if (!name) return;
                saveView({
                  name,
                  route: "/alerts",
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
            {import.meta.env.DEV && (
              <Button variant="outline" size="sm" onClick={handleCreateDemo} disabled={creatingDemo}>
                {creatingDemo ? "Creating..." : "Create demo incident"}
              </Button>
            )}
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
            <div className="flex items-center gap-2">
              <Button variant="ghost" size="sm" onClick={resetFilters}>
                <RefreshCw className="mr-2 h-4 w-4" />
                Reset
              </Button>
              <Button variant="ghost" size="sm" onClick={() => refreshIncidents()}>
                Refresh
              </Button>
            </div>
          </div>
          <Card>
            <CardContent className="space-y-4 pt-4">
              <div>
                <label className="text-xs text-muted-foreground">Status</label>
                <Select
                  value={filters.status ?? "OPEN"}
                  onValueChange={(value) => updateFilters({ status: value === "ALL" ? undefined : (value as any) })}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="OPEN">Open</SelectItem>
                    <SelectItem value="ACKED">Acked</SelectItem>
                    <SelectItem value="RESOLVED">Resolved</SelectItem>
                    <SelectItem value="ALL">All</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <label className="text-xs text-muted-foreground">Severity</label>
                <Select
                  value={filters.severity ?? "HIGH"}
                  onValueChange={(value) => updateFilters({ severity: value === "ALL" ? undefined : (value as any) })}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="CRITICAL">Critical+</SelectItem>
                    <SelectItem value="HIGH">High+</SelectItem>
                    <SelectItem value="MEDIUM">Medium+</SelectItem>
                    <SelectItem value="LOW">Low+</SelectItem>
                    <SelectItem value="ALL">All</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <Button className="w-full" onClick={() => refreshIncidents()}>
                <Filter className="mr-2 h-4 w-4" />
                Apply filters
              </Button>
            </CardContent>
          </Card>
        </aside>

        <section className="space-y-6">
          {summary && (
            <div className="grid grid-cols-3 gap-4">
              {[
                { label: "Open", value: summary.open_count ?? 0 },
                { label: "Acked", value: summary.acked_count ?? 0 },
                { label: "Resolved", value: summary.resolved_count ?? 0 },
              ].map((kpi) => (
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
              <CardTitle className="text-sm font-semibold">Alert Queue</CardTitle>
              <Button variant="ghost" size="sm" onClick={() => refreshIncidents()}>
                Refresh
              </Button>
            </CardHeader>
            <CardContent>
              {demoError && (
                <div className="mb-3 rounded-md border border-destructive/30 bg-destructive/10 p-2 text-xs text-destructive">
                  {demoError}
                </div>
              )}
              {state === "loading" ? (
                <LoadingState label="Loading alerts..." />
              ) : state === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load alerts.</span>
                  <Button size="sm" variant="outline" onClick={() => refreshIncidents()}>
                    Retry
                  </Button>
                </div>
              ) : sortedIncidents.length === 0 ? (
                <div className="text-sm text-muted-foreground">No alerts in this window. Adjust filters or create a demo incident.</div>
              ) : (
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="text-left text-xs text-muted-foreground">
                        <th className="py-2">Severity</th>
                        <th>Status</th>
                        <th>Title</th>
                        <th>Opened</th>
                        <th>Age</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sortedIncidents.map((incident) => (
                        <tr
                          key={incident.id}
                          className={cn(
                            "border-t border-border/40 cursor-pointer",
                            selectedIncident?.id === incident.id && "bg-primary/10"
                          )}
                          onClick={() => openDrawer(incident)}
                        >
                          <td className="py-2">
                            <Badge variant="outline" className={severityStyles[incident.severity]}>
                              {incident.severity}
                            </Badge>
                          </td>
                          <td>
                            <Badge variant="outline" className={statusStyles[incident.status]}>
                              {incident.status}
                            </Badge>
                          </td>
                          <td className="font-medium">{incident.title}</td>
                          <td className="text-xs text-muted-foreground">
                            {new Date(incident.opened_at).toLocaleString()}
                          </td>
                          <td className="text-xs text-muted-foreground">{formatAge(incident.opened_at)}</td>
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
              <CardTitle className="text-sm font-semibold">Alert Rules</CardTitle>
              <div className="flex items-center gap-2">
                <Button variant="ghost" size="sm" onClick={() => refreshRules()}>
                  Refresh
                </Button>
                <Button variant="outline" size="sm" onClick={openCreateRule}>
                  New rule
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              {rulesState === "loading" ? (
                <LoadingState label="Loading rules..." />
              ) : rulesState === "error" ? (
                <div className="flex items-center justify-between text-sm text-destructive">
                  <span>Failed to load rules.</span>
                  <Button size="sm" variant="outline" onClick={() => refreshRules()}>
                    Retry
                  </Button>
                </div>
              ) : rules.length === 0 ? (
                <div className="text-sm text-muted-foreground">No alert rules yet.</div>
              ) : (
                <div className="space-y-3">
                  {rules.map((rule) => (
                    <div key={rule.id} className="flex items-center justify-between rounded-md border border-border/50 px-3 py-2">
                      <div className="space-y-1">
                        <div className="text-sm font-semibold">{rule.name}</div>
                        <div className="text-xs text-muted-foreground">
                          {rule.type} | {rule.severity?.toUpperCase() ?? "MEDIUM"} | target {rule.conditions?.target ?? "-"} | threshold {rule.conditions?.threshold ?? "-"}
                        </div>
                        <div className="text-[11px] text-muted-foreground">
                          Cooldown {rule.cooldown_minutes ?? 60}m | Channels {rule.channels?.join(", ") || "in_app"}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <Switch checked={rule.is_active} onCheckedChange={() => toggleRule(rule)} />
                        <Button size="sm" variant="ghost" onClick={() => openEditRule(rule)}>
                          Edit
                        </Button>
                        <Button size="sm" variant="ghost" onClick={() => deleteRule(rule)}>
                          Delete
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </section>

        <aside className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-sm font-semibold uppercase text-muted-foreground">Signal Rail</h2>
            <Badge variant="secondary">{filteredIncidents.length} active</Badge>
          </div>
          <Card className="border-border/60">
            <CardContent className="space-y-3 pt-4">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <AlertTriangle className="h-4 w-4 text-amber-400" />
                Selected alert
              </div>
              <div className="text-xs text-muted-foreground">
                {selectedIncident ? selectedIncident.title : "Select an alert to see details."}
              </div>
            </CardContent>
          </Card>
        </aside>
      </div>

      <Sheet open={drawerOpen} onOpenChange={setDrawerOpen}>
        <SheetContent side="right" className="w-[420px] sm:max-w-md">
          <SheetHeader>
            <SheetTitle>Alert Detail</SheetTitle>
          </SheetHeader>
          {selectedIncident ? (
            <div className="mt-4 space-y-4">
              <div className="space-y-1">
                <div className="text-sm font-semibold">{selectedIncident.title}</div>
                <div className="text-xs text-muted-foreground">{selectedIncident.summary}</div>
                {selectedIncident.destination_key && (
                  <div className="text-[11px] text-muted-foreground">Destination key: {selectedIncident.destination_key}</div>
                )}
              </div>
              <div className="flex flex-wrap gap-2">
                <Badge variant="outline" className={severityStyles[selectedIncident.severity]}>
                  {selectedIncident.severity}
                </Badge>
                <Badge variant="outline" className={statusStyles[selectedIncident.status]}>
                  {selectedIncident.status}
                </Badge>
              </div>
              <div className="text-xs text-muted-foreground">
                Opened {new Date(selectedIncident.opened_at).toLocaleString()}
              </div>
              <div className="rounded-md border border-border/50 bg-muted/20 p-3 text-xs text-muted-foreground">
                <div className="font-semibold text-foreground">Threshold vs baseline</div>
                <div>Threshold: {selectedIncident.summary?.match(/\d+(\.\d+)?%/)?.[0] ?? "-"}</div>
                <div>Baseline: {selectedIncident.summary?.match(/baseline\s+(\d+(\.\d+)?%)/i)?.[1] ?? "-"}</div>
              </div>
              <div className="rounded-md border border-border/50 bg-muted/20 p-3 text-xs text-muted-foreground">
                <div className="font-semibold text-foreground">Status history</div>
                <div>Opened | {new Date(selectedIncident.opened_at).toLocaleString()}</div>
                {selectedIncident.acked_at && <div>Acked | {new Date(selectedIncident.acked_at).toLocaleString()}</div>}
                {selectedIncident.resolved_at && <div>Resolved | {new Date(selectedIncident.resolved_at).toLocaleString()}</div>}
              </div>
              <div className="rounded-md border border-border/50 bg-muted/20 p-3 text-xs text-muted-foreground">
                <div className="font-semibold text-foreground">Metric preview</div>
                <div>Linked metric preview is not available yet for this alert.</div>
              </div>
              <div className="flex gap-2">
                <Button size="sm" variant="outline" onClick={handleAck} disabled={selectedIncident.status !== "OPEN"}>
                  <CheckCircle2 className="mr-2 h-4 w-4" />
                  Ack
                </Button>
                <Button size="sm" variant="outline" onClick={handleResolve} disabled={selectedIncident.status === "RESOLVED"}>
                  <XCircle className="mr-2 h-4 w-4" />
                  Resolve
                </Button>
              </div>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  const name = window.prompt("Name this alert view", selectedIncident.title);
                  if (!name) return;
                  saveView({
                    name,
                    route: buildAlertContextLink(selectedIncident, filters, buildAlertSelection(selectedIncident)),
                    filters,
                    selection: buildAlertSelection(selectedIncident),
                  });
                  toast({ title: "View saved", description: name });
                }}
              >
                Save alert context as view
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  const selection = buildAlertSelection(selectedIncident);
                  const investigation = createInvestigation({
                    title: `Investigation: ${selectedIncident.title}`,
                    hypothesis: selectedIncident.summary,
                    sourceRoute: buildAlertContextLink(selectedIncident, filters, selection),
                    linkedAlertId: selectedIncident.id,
                    linkedEntityId: selection?.id,
                    linkedEntityName: selection?.name,
                    linkedEntityType: selection?.type,
                  });
                  toast({ title: "Investigation created" });
                  setLocation(`/investigations/${investigation.id}`);
                }}
              >
                Create investigation
              </Button>
              <Link href={buildAlertContextLink(selectedIncident, filters, buildAlertSelection(selectedIncident))}>
                <Button size="sm" className="w-full">
                  Open context
                </Button>
              </Link>
            </div>
          ) : (
            <div className="mt-4 text-sm text-muted-foreground">Select an alert to view details.</div>
          )}
        </SheetContent>
      </Sheet>

      <Dialog open={rulesDialogOpen} onOpenChange={setRulesDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{editingRule ? "Edit alert rule" : "Create alert rule"}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <Label>Name</Label>
              <Input
                value={ruleForm.name}
                onChange={(event) => setRuleForm((prev) => ({ ...prev, name: event.target.value }))}
                placeholder="Port congestion threshold"
              />
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Type</Label>
                <Select value={ruleForm.type} onValueChange={(value) => setRuleForm((prev) => ({ ...prev, type: value }))}>
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="congestion">Congestion</SelectItem>
                    <SelectItem value="flow">Flow</SelectItem>
                    <SelectItem value="price_threshold">Price threshold</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div>
                <Label>Severity</Label>
                <Select value={ruleForm.severity} onValueChange={(value) => setRuleForm((prev) => ({ ...prev, severity: value }))}>
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="critical">Critical</SelectItem>
                    <SelectItem value="high">High</SelectItem>
                    <SelectItem value="medium">Medium</SelectItem>
                    <SelectItem value="low">Low</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Target hub / entity</Label>
                <Input
                  value={ruleForm.target}
                  onChange={(event) => setRuleForm((prev) => ({ ...prev, target: event.target.value }))}
                  placeholder="NLRTM"
                />
              </div>
              <div>
                <Label>Threshold</Label>
                <Input
                  value={ruleForm.threshold}
                  onChange={(event) => setRuleForm((prev) => ({ ...prev, threshold: event.target.value }))}
                  placeholder="80"
                />
              </div>
            </div>
            <div className="grid grid-cols-2 gap-3">
              <div>
                <Label>Cooldown (minutes)</Label>
                <Input
                  type="number"
                  value={ruleForm.cooldownMinutes}
                  onChange={(event) => setRuleForm((prev) => ({ ...prev, cooldownMinutes: Number(event.target.value) }))}
                />
              </div>
              <div>
                <Label>Channels</Label>
                <div className="mt-2 flex flex-wrap gap-2 text-xs text-muted-foreground">
                  <Badge variant={ruleForm.channels.includes("in_app") ? "default" : "outline"}>In-app</Badge>
                  <Badge variant="outline">Email (placeholder)</Badge>
                  <Badge variant="outline">Webhook (placeholder)</Badge>
                </div>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRulesDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={saveRule}>{editingRule ? "Save changes" : "Create rule"}</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

export default function AlertsCommandPage() {
  return (
    <TerminalStoreProvider basePath="/alerts">
      <AlertsCommandContent />
    </TerminalStoreProvider>
  );
}
