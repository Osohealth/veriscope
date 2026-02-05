import { useEffect, useMemo, useState } from "react";
import { Link } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Sheet, SheetContent, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";
import { apiFetchJson } from "@/lib/apiFetch";
import { ArrowLeft, Plus, RefreshCcw, RotateCw, Send } from "lucide-react";

type Severity = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
type DestinationType = "WEBHOOK" | "EMAIL";

type AlertSubscription = {
  id: string;
  user_id: string;
  scope: "PORT" | "GLOBAL";
  entity_type: string;
  entity_id: string;
  destination_type: DestinationType;
  destination: string;
  severity_min: Severity;
  enabled: boolean;
  signature_version: string;
  has_secret: boolean;
  created_at: string;
  updated_at: string;
  last_test_at: string | null;
  last_test_status: "SENT" | "FAILED" | null;
  last_test_error: string | null;
};

type AlertDelivery = {
  id: string;
  subscription_id: string;
  cluster_id: string;
  status: string;
  error?: string | null;
  sent_at?: string | null;
  created_at?: string | null;
  cluster_severity?: Severity | null;
  cluster_type?: string | null;
  endpoint: string;
};

type PortOption = {
  id: string;
  name: string;
  unlocode: string;
};

const SEVERITY_OPTIONS: Severity[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
const DESTINATION_OPTIONS: DestinationType[] = ["WEBHOOK", "EMAIL"];

const SEVERITY_STYLES: Record<Severity, string> = {
  LOW: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  MEDIUM: "bg-yellow-500/10 text-yellow-400 border-yellow-500/30",
  HIGH: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  CRITICAL: "bg-red-500/10 text-red-400 border-red-500/30",
};

const STATUS_STYLES: Record<string, string> = {
  SENT: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  FAILED: "bg-red-500/10 text-red-400 border-red-500/30",
  DISABLED: "bg-slate-500/10 text-slate-300 border-slate-500/30",
};

const DOT = "\u00B7";


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

type CreateFormState = {
  destination_type: DestinationType;
  destination: string;
  severity_min: Severity;
  enabled: boolean;
  secret: string;
  scope: "PORT" | "GLOBAL";
  entity_id: string;
};

const defaultCreateForm: CreateFormState = {
  destination_type: "WEBHOOK",
  destination: "",
  severity_min: "HIGH",
  enabled: true,
  secret: "",
  scope: "PORT",
  entity_id: "",
};

export default function AlertSubscriptionsPage() {
  const { toast } = useToast();
  const [subscriptions, setSubscriptions] = useState<AlertSubscription[]>([]);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);
  const [selected, setSelected] = useState<AlertSubscription | null>(null);
  const [deliveries, setDeliveries] = useState<AlertDelivery[]>([]);
  const [deliveriesLoading, setDeliveriesLoading] = useState(false);
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [isEditOpen, setIsEditOpen] = useState(false);
  const [createForm, setCreateForm] = useState<CreateFormState>(defaultCreateForm);
  const [editForm, setEditForm] = useState<CreateFormState>(defaultCreateForm);
  const [rotatedSecret, setRotatedSecret] = useState<string | null>(null);
  const [ports, setPorts] = useState<PortOption[]>([]);
  const [portSearch, setPortSearch] = useState("");
  const [portsLoading, setPortsLoading] = useState(false);

  const loadSubscriptions = async (cursor?: string | null, append = false) => {
    if (append) {
      setLoadingMore(true);
    } else {
      setLoading(true);
      setError(undefined);
      setNextCursor(null);
    }
    try {
      const params = new URLSearchParams();
      params.set("limit", "50");
      if (cursor) params.set("cursor", cursor);
      const payload = await apiFetchJson(`/v1/alert-subscriptions?${params.toString()}`);
      const items = Array.isArray(payload?.items) ? payload.items : [];
      setSubscriptions((prev) => (append ? [...prev, ...items] : items));
      setNextCursor(payload?.next_cursor ?? null);
    } catch (err: any) {
      if (!append) {
        setError(err?.message ?? "Unable to load subscriptions");
      } else {
        toast({ title: "Load more failed", description: err?.message ?? "Unable to load more subscriptions.", variant: "destructive" });
      }
    } finally {
      if (append) {
        setLoadingMore(false);
      } else {
        setLoading(false);
      }
    }
  };

  const loadPorts = async (query?: string) => {
    setPortsLoading(true);
    try {
      const q = query?.trim() ?? "";
      const payload = await apiFetchJson(`/v1/ports?limit=200${q ? `&q=${encodeURIComponent(q)}` : ""}`);
      setPorts(Array.isArray(payload?.items) ? payload.items : []);
    } catch {
      setPorts([]);
    } finally {
      setPortsLoading(false);
    }
  };

  const loadDeliveries = async (subscriptionId: string) => {
    setDeliveriesLoading(true);
    try {
      const payload = await apiFetchJson(`/v1/alert-deliveries?subscription_id=${subscriptionId}&days=30&limit=20`);
      setDeliveries(Array.isArray(payload?.items) ? payload.items : []);
    } catch {
      setDeliveries([]);
    } finally {
      setDeliveriesLoading(false);
    }
  };

  useEffect(() => {
    loadSubscriptions();
  }, []);

  useEffect(() => {
    if (isCreateOpen || isEditOpen) {
      loadPorts(portSearch);
    }
  }, [isCreateOpen, isEditOpen, portSearch]);

  useEffect(() => {
    if (selected) {
      loadDeliveries(selected.id);
    } else {
      setDeliveries([]);
    }
  }, [selected]);

  const portMap = useMemo(() => {
    const map = new Map<string, PortOption>();
    ports.forEach((port) => map.set(port.id, port));
    return map;
  }, [ports]);

  const handleCreate = async () => {
    try {
      if (createForm.scope === "PORT" && !createForm.entity_id) {
        toast({ title: "Port required", description: "Select a port for a PORT-scoped subscription.", variant: "destructive" });
        return;
      }
      const body: any = {
        destination_type: createForm.destination_type,
        destination: createForm.destination.trim(),
        severity_min: createForm.severity_min,
        enabled: createForm.enabled,
        signature_version: "v1",
        scope: createForm.scope,
      };
      if (createForm.scope === "PORT") {
        body.entity_id = createForm.entity_id;
      }
      if (createForm.destination_type === "WEBHOOK" && createForm.secret.trim()) {
        body.secret = createForm.secret.trim();
      }
      await apiFetchJson("/v1/alert-subscriptions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      toast({ title: "Subscription created" });
      setIsCreateOpen(false);
      setCreateForm(defaultCreateForm);
      setPortSearch("");
      loadSubscriptions();
    } catch (err: any) {
      toast({ title: "Create failed", description: err?.message ?? "Unable to create subscription.", variant: "destructive" });
    }
  };

  const handleEdit = async () => {
    if (!selected) return;
    try {
      if (editForm.scope === "PORT" && !editForm.entity_id) {
        toast({ title: "Port required", description: "Select a port for a PORT-scoped subscription.", variant: "destructive" });
        return;
      }
      const body: any = {
        destination: editForm.destination.trim(),
        severity_min: editForm.severity_min,
        enabled: editForm.enabled,
        scope: editForm.scope,
      };
      if (editForm.scope === "PORT") {
        body.entity_id = editForm.entity_id;
      }
      await apiFetchJson(`/v1/alert-subscriptions/${selected.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      toast({ title: "Subscription updated" });
      setIsEditOpen(false);
      loadSubscriptions();
    } catch (err: any) {
      toast({ title: "Update failed", description: err?.message ?? "Unable to update subscription.", variant: "destructive" });
    }
  };

  const handleToggle = async (sub: AlertSubscription) => {
    try {
      await apiFetchJson(`/v1/alert-subscriptions/${sub.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled: !sub.enabled }),
      });
      loadSubscriptions();
    } catch (err: any) {
      toast({ title: "Update failed", description: err?.message ?? "Unable to update subscription.", variant: "destructive" });
    }
  };

  const openEdit = (sub: AlertSubscription) => {
    setSelected(sub);
    setEditForm({
      destination_type: sub.destination_type,
      destination: sub.destination,
      severity_min: sub.severity_min,
      enabled: sub.enabled,
      secret: "",
      scope: sub.scope ?? "PORT",
      entity_id: sub.entity_id ?? "",
    });
    setIsEditOpen(true);
  };

  const handleTest = async (sub: AlertSubscription) => {
    try {
      await apiFetchJson(`/v1/alert-subscriptions/${sub.id}/test`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mode: "synthetic", severity: sub.severity_min, include_sample_signal: true }),
      });
      toast({ title: "Test sent", description: "Test delivery executed." });
      loadSubscriptions();
      if (selected?.id === sub.id) {
        loadDeliveries(sub.id);
      }
    } catch (err: any) {
      toast({ title: "Test failed", description: err?.message ?? "Unable to send test.", variant: "destructive" });
    }
  };

  const handleRotateSecret = async (sub: AlertSubscription) => {
    try {
      const payload = await apiFetchJson(`/v1/alert-subscriptions/${sub.id}/rotate-secret`, { method: "POST" });
      const newSecret = payload?.secret ?? null;
      setRotatedSecret(newSecret);
      toast({ title: "Secret rotated", description: "Copy the new secret now." });
      loadSubscriptions();
    } catch (err: any) {
      toast({ title: "Rotate failed", description: err?.message ?? "Unable to rotate secret.", variant: "destructive" });
    }
  };

  const lastResult = useMemo(() => {
    if (!selected?.last_test_status) return "--";
    return selected.last_test_status;
  }, [selected]);

  return (
    <TooltipProvider>
      <div className="min-h-screen bg-background">
        <div className="border-b border-border bg-card">
          <div className="container mx-auto px-6 py-6">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <Link href="/platform">
                  <Button variant="ghost" size="sm">
                    <ArrowLeft className="mr-2 h-4 w-4" />
                    Back to Menu
                  </Button>
                </Link>
                <h1 className="mt-2 text-2xl font-semibold text-foreground">Alert Subscriptions</h1>
                <p className="text-sm text-muted-foreground">
                  Create and test delivery targets for Veriscope signals.
                </p>
              </div>
              <div className="flex items-center gap-3">
                <Button onClick={() => setIsCreateOpen(true)}>
                  <Plus className="mr-2 h-4 w-4" />
                  New subscription
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div className="container mx-auto px-6 py-8">
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
              Unable to load subscriptions. Please retry.
            </div>
          ) : subscriptions.length === 0 ? (
            <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center">
              <p className="text-sm text-muted-foreground">No subscriptions yet.</p>
              <p className="mt-2 text-xs text-muted-foreground">Create one to start alert delivery.</p>
            </div>
          ) : (
            <Card className="border-border/60 bg-card/70">
              <CardContent className="p-0">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Destination</TableHead>
                      <TableHead>Scope</TableHead>
                      <TableHead>Severity min</TableHead>
                      <TableHead>Status</TableHead>
                      <TableHead>Last test</TableHead>
                      <TableHead>Last result</TableHead>
                      <TableHead>Last error</TableHead>
                      <TableHead>Created</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {subscriptions.map((sub) => (
                      <TableRow key={sub.id} className="cursor-pointer" onClick={() => setSelected(sub)}>
                        <TableCell>
                          <div className="text-xs text-muted-foreground">{sub.destination_type}</div>
                          <div className="text-sm">{sub.destination}</div>
                        </TableCell>
                        <TableCell>
                          <div className="text-xs text-muted-foreground">
                            {sub.scope === "GLOBAL"
                              ? "All ports"
                              : portMap.get(sub.entity_id)?.name
                                ? `${portMap.get(sub.entity_id)?.name} (${portMap.get(sub.entity_id)?.unlocode})`
                                : sub.entity_id}
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("border text-xs", SEVERITY_STYLES[sub.severity_min])}>
                            {sub.severity_min}
                          </Badge>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("border text-xs", sub.enabled ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/30" : STATUS_STYLES.DISABLED)}>
                            {sub.enabled ? "ENABLED" : "DISABLED"}
                          </Badge>
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatRelativeTime(sub.last_test_at)}
                        </TableCell>
                        <TableCell>
                          {sub.last_test_status ? (
                            <Badge className={cn("border text-xs", STATUS_STYLES[sub.last_test_status])}>
                              {sub.last_test_status}
                            </Badge>
                          ) : (
                            <span className="text-xs text-muted-foreground">--</span>
                          )}
                        </TableCell>
                        <TableCell>
                          {sub.last_test_error ? (
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span className="text-xs text-muted-foreground line-clamp-1">
                                  {sub.last_test_error}
                                </span>
                              </TooltipTrigger>
                              <TooltipContent>
                                <p className="max-w-xs text-xs">{sub.last_test_error}</p>
                              </TooltipContent>
                            </Tooltip>
                          ) : (
                            <span className="text-xs text-muted-foreground">--</span>
                          )}
                        </TableCell>
                        <TableCell className="text-xs text-muted-foreground">
                          {formatRelativeTime(sub.created_at)}
                        </TableCell>
                      <TableCell onClick={(event) => event.stopPropagation()}>
                        <div className="flex items-center gap-2">
                          <Button size="sm" variant="outline" onClick={() => handleTest(sub)}>
                            <Send className="mr-2 h-3 w-3" />
                            Test
                          </Button>
                          <Button size="sm" variant="ghost" onClick={() => openEdit(sub)}>
                            Edit
                          </Button>
                          <Switch checked={sub.enabled} onCheckedChange={() => handleToggle(sub)} />
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                  </TableBody>
                </Table>
                {nextCursor && (
                  <div className="flex justify-center border-t border-border/60 px-4 py-4">
                    <Button
                      variant="outline"
                      onClick={() => loadSubscriptions(nextCursor, true)}
                      disabled={loadingMore}
                    >
                      {loadingMore ? "Loading..." : "Load more"}
                    </Button>
                  </div>
                )}
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      <Sheet open={Boolean(selected)} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent side="right" className="w-full overflow-y-auto sm:max-w-2xl">
          {selected && (
            <div className="space-y-6">
              <SheetHeader className="space-y-2">
                <SheetTitle className="text-xl">Subscription details</SheetTitle>
                <div className="flex flex-wrap items-center gap-3 text-sm text-muted-foreground">
                  <Badge className={cn("border text-xs", selected.enabled ? "bg-emerald-500/10 text-emerald-400 border-emerald-500/30" : STATUS_STYLES.DISABLED)}>
                    {selected.enabled ? "ENABLED" : "DISABLED"}
                  </Badge>
                  <span>{selected.destination_type}</span>
                  <span>{selected.destination}</span>
                </div>
              </SheetHeader>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                <div className="text-sm font-semibold text-foreground">Config</div>
                <div className="text-sm text-muted-foreground">
                  Scope:{" "}
                  <span className="text-foreground">
                    {selected.scope === "GLOBAL"
                      ? "All ports"
                      : portMap.get(selected.entity_id)
                        ? `${portMap.get(selected.entity_id)?.name} (${portMap.get(selected.entity_id)?.unlocode})`
                        : selected.entity_id}
                  </span>
                </div>
                <div className="text-sm text-muted-foreground">
                  Severity min: <span className="text-foreground">{selected.severity_min}</span>
                </div>
                <div className="text-sm text-muted-foreground">
                  Signature: <span className="text-foreground">{selected.signature_version}</span>
                </div>
                {selected.destination_type === "WEBHOOK" && (
                  <div className="text-sm text-muted-foreground flex items-center gap-3">
                    Secret: <span className="text-foreground">{selected.has_secret ? "set" : "none"}</span>
                    <Button size="sm" variant="outline" onClick={() => handleRotateSecret(selected)}>
                      <RotateCw className="mr-2 h-3 w-3" />
                      Rotate
                    </Button>
                  </div>
                )}
                {rotatedSecret && (
                  <div className="rounded-lg border border-border/60 bg-card/60 p-3 text-xs text-muted-foreground">
                    New secret: <span className="text-foreground">{rotatedSecret}</span>
                  </div>
                )}
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-2">
                <div className="text-sm font-semibold text-foreground">Test delivery</div>
                <div className="flex items-center gap-3 text-sm text-muted-foreground">
                  Last test: {formatRelativeTime(selected.last_test_at)} {DOT} Result: {lastResult}
                </div>
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="outline" onClick={() => handleTest(selected)}>
                    <RefreshCcw className="mr-2 h-3 w-3" />
                    Send test alert
                  </Button>
                  <Button size="sm" variant="ghost" onClick={() => openEdit(selected)}>
                    Edit subscription
                  </Button>
                </div>
              </div>

              <div className="rounded-xl border border-border/60 bg-card/70 p-4 space-y-3">
                <div className="text-sm font-semibold text-foreground">Recent deliveries</div>
                {deliveriesLoading ? (
                  <Skeleton className="h-20 w-full" />
                ) : deliveries.length === 0 ? (
                  <div className="text-xs text-muted-foreground">No deliveries yet.</div>
                ) : (
                  <div className="space-y-2">
                    {deliveries.slice(0, 20).map((row) => (
                      <div key={row.id} className="flex items-start justify-between gap-4 text-sm">
                        <div>
                          <div className="text-xs text-muted-foreground">{formatRelativeTime(row.sent_at ?? row.created_at)}</div>
                          <div className="text-sm">{row.cluster_type ?? "Event"} {DOT} {row.cluster_id}</div>
                        </div>
                        <div className="flex items-center gap-2">
                          <Badge className={cn("border text-xs", STATUS_STYLES[row.status] ?? STATUS_STYLES.FAILED)}>
                            {row.status}
                          </Badge>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                <Link href={`/alerts?subscription_id=${selected.id}`}>
                  <Button variant="ghost" size="sm">View in Alert Activity</Button>
                </Link>
              </div>
            </div>
          )}
        </SheetContent>
      </Sheet>

      <Dialog open={isCreateOpen} onOpenChange={setIsCreateOpen}>
        <DialogContent className="bg-slate-900 border-slate-700 max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-white">New subscription</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground">Destination type</label>
              <Select
                value={createForm.destination_type}
                onValueChange={(value) => setCreateForm((prev) => ({ ...prev, destination_type: value as DestinationType }))}
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select destination" />
                </SelectTrigger>
                <SelectContent>
                  {DESTINATION_OPTIONS.map((dest) => (
                    <SelectItem key={dest} value={dest}>{dest}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">
                {createForm.destination_type === "EMAIL" ? "Email address" : "Webhook URL"}
              </label>
              <Input
                className="mt-2"
                placeholder={createForm.destination_type === "EMAIL" ? "alerts@veriscope.dev" : "https://example.com/webhook"}
                value={createForm.destination}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, destination: event.target.value }))}
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Scope</label>
              <Select
                value={createForm.scope}
                onValueChange={(value) =>
                  setCreateForm((prev) => ({
                    ...prev,
                    scope: value as "PORT" | "GLOBAL",
                    entity_id: value === "GLOBAL" ? "" : prev.entity_id,
                  }))
                }
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select scope" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="PORT">Specific port</SelectItem>
                  <SelectItem value="GLOBAL">All ports</SelectItem>
                </SelectContent>
              </Select>
              {createForm.scope === "GLOBAL" && (
                <p className="mt-2 text-xs text-muted-foreground">
                  You’ll receive alerts for any port meeting your severity threshold.
                </p>
              )}
            </div>
            {createForm.scope === "PORT" && (
              <div>
                <label className="text-xs font-medium text-muted-foreground">Port</label>
                <Input
                  className="mt-2"
                  placeholder="Search ports..."
                  value={portSearch}
                  onChange={(event) => setPortSearch(event.target.value)}
                />
                <Select
                  value={createForm.entity_id}
                  onValueChange={(value) => setCreateForm((prev) => ({ ...prev, entity_id: value }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder={portsLoading ? "Loading ports..." : "Select a port"} />
                  </SelectTrigger>
                  <SelectContent>
                    {ports.map((port) => (
                      <SelectItem key={port.id} value={port.id}>
                        {port.name} ({port.unlocode})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            {createForm.destination_type === "WEBHOOK" && (
              <div>
                <label className="text-xs font-medium text-muted-foreground">Secret (optional)</label>
                <Input
                  className="mt-2"
                  placeholder="Leave blank to auto-generate"
                  value={createForm.secret}
                  onChange={(event) => setCreateForm((prev) => ({ ...prev, secret: event.target.value }))}
                />
              </div>
            )}
            <div>
              <label className="text-xs font-medium text-muted-foreground">Severity min</label>
              <Select
                value={createForm.severity_min}
                onValueChange={(value) => setCreateForm((prev) => ({ ...prev, severity_min: value as Severity }))}
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select severity" />
                </SelectTrigger>
                <SelectContent>
                  {SEVERITY_OPTIONS.map((severity) => (
                    <SelectItem key={severity} value={severity}>{severity}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center justify-between rounded-lg border border-slate-700 px-4 py-3">
              <div className="text-sm text-slate-200">Enabled</div>
              <Switch
                checked={createForm.enabled}
                onCheckedChange={(value) => setCreateForm((prev) => ({ ...prev, enabled: Boolean(value) }))}
              />
            </div>
          </div>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setIsCreateOpen(false)}>Cancel</Button>
            <Button onClick={handleCreate}>Create</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isEditOpen} onOpenChange={setIsEditOpen}>
        <DialogContent className="bg-slate-900 border-slate-700 max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-white">Edit subscription</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground">
                {editForm.destination_type === "EMAIL" ? "Email address" : "Webhook URL"}
              </label>
              <Input
                className="mt-2"
                value={editForm.destination}
                onChange={(event) => setEditForm((prev) => ({ ...prev, destination: event.target.value }))}
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Scope</label>
              <Select
                value={editForm.scope}
                onValueChange={(value) =>
                  setEditForm((prev) => ({
                    ...prev,
                    scope: value as "PORT" | "GLOBAL",
                    entity_id: value === "GLOBAL" ? "" : prev.entity_id,
                  }))
                }
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select scope" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="PORT">Specific port</SelectItem>
                  <SelectItem value="GLOBAL">All ports</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {editForm.scope === "PORT" && (
              <div>
                <label className="text-xs font-medium text-muted-foreground">Port</label>
                <Input
                  className="mt-2"
                  placeholder="Search ports..."
                  value={portSearch}
                  onChange={(event) => setPortSearch(event.target.value)}
                />
                <Select
                  value={editForm.entity_id}
                  onValueChange={(value) => setEditForm((prev) => ({ ...prev, entity_id: value }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder={portsLoading ? "Loading ports..." : "Select a port"} />
                  </SelectTrigger>
                  <SelectContent>
                    {ports.map((port) => (
                      <SelectItem key={port.id} value={port.id}>
                        {port.name} ({port.unlocode})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            <div>
              <label className="text-xs font-medium text-muted-foreground">Severity min</label>
              <Select
                value={editForm.severity_min}
                onValueChange={(value) => setEditForm((prev) => ({ ...prev, severity_min: value as Severity }))}
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select severity" />
                </SelectTrigger>
                <SelectContent>
                  {SEVERITY_OPTIONS.map((severity) => (
                    <SelectItem key={severity} value={severity}>{severity}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="flex items-center justify-between rounded-lg border border-slate-700 px-4 py-3">
              <div className="text-sm text-slate-200">Enabled</div>
              <Switch
                checked={editForm.enabled}
                onCheckedChange={(value) => setEditForm((prev) => ({ ...prev, enabled: Boolean(value) }))}
              />
            </div>
          </div>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setIsEditOpen(false)}>Cancel</Button>
            <Button onClick={handleEdit}>Save</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </TooltipProvider>
  );
}
