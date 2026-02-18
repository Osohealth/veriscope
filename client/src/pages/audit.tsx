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
import { cn } from "@/lib/utils";
import { ArrowLeft } from "lucide-react";

type AuditSeverity = "INFO" | "WARN" | "SECURITY";
type AuditStatus = "SUCCESS" | "DENIED" | "FAILED";

type AuditEvent = {
  id: string;
  actor_type: string;
  actor_user_id?: string | null;
  actor_api_key_id?: string | null;
  actor_label?: string | null;
  action: string;
  resource_type: string;
  resource_id?: string | null;
  severity: AuditSeverity;
  status: AuditStatus;
  message: string;
  metadata?: Record<string, unknown>;
  ip?: string | null;
  user_agent?: string | null;
  request_id?: string | null;
  created_at?: string | null;
};

type Summary = {
  total: number;
  security: number;
  denied: number;
  failed: number;
};

type Filters = {
  days: number;
  severity_min: AuditSeverity;
  action?: string;
  resource_type?: string;
  resource_id?: string;
  actor?: string;
};

const SEVERITY_OPTIONS: AuditSeverity[] = ["INFO", "WARN", "SECURITY"];

const SEVERITY_STYLES: Record<AuditSeverity, string> = {
  INFO: "bg-blue-500/10 text-blue-300 border-blue-500/30",
  WARN: "bg-yellow-500/10 text-yellow-300 border-yellow-500/30",
  SECURITY: "bg-red-500/10 text-red-300 border-red-500/30",
};

const STATUS_STYLES: Record<AuditStatus, string> = {
  SUCCESS: "bg-emerald-500/10 text-emerald-400 border-emerald-500/30",
  DENIED: "bg-orange-500/10 text-orange-400 border-orange-500/30",
  FAILED: "bg-red-500/10 text-red-400 border-red-500/30",
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

const shortenId = (value?: string | null) => {
  if (!value) return "--";
  return `${value.slice(0, 8)}...`;
};

const parseFiltersFromUrl = (location: string): Filters => {
  const params = new URLSearchParams(location.split("?")[1] ?? "");
  const days = Number(params.get("days") ?? 7);
  const severity = (params.get("severity_min") ?? "INFO").toUpperCase() as AuditSeverity;
  return {
    days: Number.isFinite(days) ? Math.min(Math.max(days, 1), 365) : 7,
    severity_min: SEVERITY_OPTIONS.includes(severity) ? severity : "INFO",
    action: params.get("action") ?? undefined,
    resource_type: params.get("resource_type") ?? undefined,
    resource_id: params.get("resource_id") ?? undefined,
    actor: params.get("actor") ?? undefined,
  };
};

const buildQueryString = (filters: Filters, cursor?: string | null) => {
  const params = new URLSearchParams();
  params.set("days", String(filters.days));
  if (filters.severity_min) params.set("severity_min", filters.severity_min);
  if (filters.action) params.set("action", filters.action);
  if (filters.resource_type) params.set("resource_type", filters.resource_type);
  if (filters.resource_id) params.set("resource_id", filters.resource_id);
  if (filters.actor) params.set("actor", filters.actor);
  params.set("limit", "50");
  if (cursor) params.set("cursor", cursor);
  return params.toString();
};

export default function AuditPage() {
  const { toast } = useToast();
  const [location, setLocation] = useLocation();
  const [filters, setFilters] = useState<Filters>(() => parseFiltersFromUrl(location));
  const [items, setItems] = useState<AuditEvent[]>([]);
  const [summary, setSummary] = useState<Summary | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [exportFormat, setExportFormat] = useState<"csv" | "jsonl">("csv");
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<AuditEvent | null>(null);
  const [selectedLoading, setSelectedLoading] = useState(false);

  const syncedFilters = useMemo(() => parseFiltersFromUrl(location), [location]);
  useEffect(() => setFilters(syncedFilters), [syncedFilters]);

  useEffect(() => {
    const query = buildQueryString(filters);
    const target = `/settings/audit?${query}`;
    if (location !== target) {
      setLocation(target, { replace: true });
    }
  }, [filters, location, setLocation]);

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const payload = await apiFetchJson(`/v1/audit-events?${buildQueryString(filters)}`, { signal: controller.signal });
        setItems(Array.isArray(payload?.items) ? payload.items : []);
        setSummary(payload?.summary ?? null);
        setNextCursor(payload?.next_cursor ?? null);
      } catch (err: any) {
        if (controller.signal.aborted || err?.name === "AbortError" || String(err?.message ?? "").toLowerCase().includes("aborted")) {
          return;
        }
        setError(err?.message ?? "Unable to load audit events.");
        toast({ title: "Failed to load audit events", description: err?.message ?? "Unable to load audit events.", variant: "destructive" });
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [filters]);

  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      return;
    }
    const controller = new AbortController();
    const load = async () => {
      setSelectedLoading(true);
      try {
        const payload = await apiFetchJson(`/v1/audit-events/${selectedId}`, { signal: controller.signal });
        setSelected(payload?.item ?? null);
      } catch (err: any) {
        if (controller.signal.aborted || err?.name === "AbortError" || String(err?.message ?? "").toLowerCase().includes("aborted")) {
          return;
        }
        toast({ title: "Failed to load audit event", description: err?.message ?? "Unable to load audit event.", variant: "destructive" });
      } finally {
        setSelectedLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [selectedId]);

  const handleLoadMore = async () => {
    if (!nextCursor) return;
    setLoadingMore(true);
    try {
      const payload = await apiFetchJson(`/v1/audit-events?${buildQueryString(filters, nextCursor)}`);
      const nextItems = Array.isArray(payload?.items) ? payload.items : [];
      setItems((prev) => [...prev, ...nextItems]);
      setNextCursor(payload?.next_cursor ?? null);
    } catch (err: any) {
      toast({ title: "Load more failed", description: err?.message ?? "Unable to load more events.", variant: "destructive" });
    } finally {
      setLoadingMore(false);
    }
  };

  const handleExport = async () => {
    setExporting(true);
    try {
      const payload = await apiFetchJson("/v1/audit-events/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          days: filters.days,
          severity_min: filters.severity_min,
          action: filters.action,
          resource_type: filters.resource_type,
          actor: filters.actor,
          format: exportFormat,
          max_rows: 50000,
        }),
      });
      if (payload?.download_url) {
        const apiKey = getApiKey();
        const headers = new Headers();
        if (apiKey) {
          headers.set("Authorization", `Bearer ${apiKey}`);
        }
        const res = await fetch(payload.download_url, { headers });
        if (!res.ok) {
          const text = await res.text();
          throw new Error(`HTTP ${res.status} ${res.statusText}: ${text.slice(0, 300)}`);
        }
        const blob = await res.blob();
        const disposition = res.headers.get("content-disposition") || "";
        const match = disposition.match(/filename=\"?([^\";]+)\"?/i);
        const fallbackExt = exportFormat === "jsonl" ? "jsonl" : "csv";
        const filename = match?.[1] || `audit-events.${fallbackExt}`;
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      }
      toast({ title: "Export ready", description: "Download started." });
    } catch (err: any) {
      toast({ title: "Export failed", description: err?.message ?? "Unable to export audit events.", variant: "destructive" });
    } finally {
      setExporting(false);
    }
  };

  const summaryCards = [
    { label: "Events", value: summary?.total ?? "--" },
    { label: "Security", value: summary?.security ?? "--" },
    { label: "Denied", value: summary?.denied ?? "--" },
    { label: "Failed", value: summary?.failed ?? "--" },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-950 to-black text-white">
      <div className="mx-auto max-w-6xl px-6 py-10">
        <div className="mb-6">
          <Link href="/alerts">
            <a className="inline-flex items-center gap-2 text-sm text-slate-300 hover:text-white">
              <ArrowLeft className="h-4 w-4" /> Back to Menu
            </a>
          </Link>
        </div>

        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h1 className="text-3xl font-semibold">Audit Log</h1>
            <p className="text-sm text-slate-400">Immutable security trail for tenant operations.</p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <div className="min-w-[140px]">
              <Select value={exportFormat} onValueChange={(value) => setExportFormat(value as "csv" | "jsonl")}>
                <SelectTrigger>
                  <SelectValue placeholder="Export format" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="csv">CSV</SelectItem>
                  <SelectItem value="jsonl">JSONL</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <Button onClick={handleExport} disabled={exporting}>
              {exporting ? "Exporting..." : "Export"}
            </Button>
          </div>
        </div>

        <div className="mt-8 grid gap-4 md:grid-cols-4">
          {summaryCards.map((card) => (
            <Card key={card.label} className="bg-slate-900/60 border-slate-800">
              <CardContent className="py-4">
                <p className="text-sm text-slate-400">{card.label}</p>
                <p className="text-2xl font-semibold">{card.value}</p>
              </CardContent>
            </Card>
          ))}
        </div>

        <Card className="mt-6 bg-slate-900/60 border-slate-800">
          <CardContent className="py-4">
            <div className="grid gap-4 md:grid-cols-6">
              <div>
                <p className="text-xs uppercase text-slate-500">Days</p>
                <Select value={String(filters.days)} onValueChange={(value) => setFilters((prev) => ({ ...prev, days: Number(value) }))}>
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Days" />
                  </SelectTrigger>
                  <SelectContent>
                    {[7, 30, 90].map((value) => (
                      <SelectItem key={value} value={String(value)}>
                        {value}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Severity min</p>
                <Select value={filters.severity_min} onValueChange={(value) => setFilters((prev) => ({ ...prev, severity_min: value as AuditSeverity }))}>
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Severity" />
                  </SelectTrigger>
                  <SelectContent>
                    {SEVERITY_OPTIONS.map((value) => (
                      <SelectItem key={value} value={value}>
                        {value}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Action</p>
                <Input
                  className="mt-2"
                  placeholder="ALERT.SUBSCRIPTION_CREATED"
                  value={filters.action ?? ""}
                  onChange={(event) => setFilters((prev) => ({ ...prev, action: event.target.value || undefined }))}
                />
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Resource type</p>
                <Input
                  className="mt-2"
                  placeholder="ALERT_DELIVERY"
                  value={filters.resource_type ?? ""}
                  onChange={(event) => setFilters((prev) => ({ ...prev, resource_type: event.target.value || undefined }))}
                />
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Resource id</p>
                <Input
                  className="mt-2"
                  placeholder="destination key"
                  value={filters.resource_id ?? ""}
                  onChange={(event) => setFilters((prev) => ({ ...prev, resource_id: event.target.value || undefined }))}
                />
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Actor</p>
                <Input
                  className="mt-2"
                  placeholder="email or key label"
                  value={filters.actor ?? ""}
                  onChange={(event) => setFilters((prev) => ({ ...prev, actor: event.target.value || undefined }))}
                />
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="mt-6 bg-slate-900/60 border-slate-800">
          <CardContent className="py-4">
            {loading ? (
              <div className="space-y-3">
                <Skeleton className="h-6 w-full bg-slate-800" />
                <Skeleton className="h-6 w-full bg-slate-800" />
                <Skeleton className="h-6 w-full bg-slate-800" />
              </div>
            ) : error ? (
              <div className="text-sm text-red-400">{error}</div>
            ) : items.length === 0 ? (
              <div className="text-sm text-slate-400">No audit events in this window.</div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Time</TableHead>
                    <TableHead>Severity</TableHead>
                    <TableHead>Action</TableHead>
                    <TableHead>Resource</TableHead>
                    <TableHead>Actor</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Message</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {items.map((row) => {
                    const actor = row.actor_label || row.actor_user_id || row.actor_api_key_id || "--";
                    return (
                      <TableRow
                        key={row.id}
                        className="cursor-pointer hover:bg-slate-800/40"
                        onClick={() => setSelectedId(row.id)}
                      >
                        <TableCell>
                          <TooltipProvider>
                            <Tooltip>
                              <TooltipTrigger asChild>
                                <span>{formatRelativeTime(row.created_at)}</span>
                              </TooltipTrigger>
                              <TooltipContent>{row.created_at ?? "--"}</TooltipContent>
                            </Tooltip>
                          </TooltipProvider>
                        </TableCell>
                        <TableCell>
                          <Badge className={cn("border", SEVERITY_STYLES[row.severity])}>{row.severity}</Badge>
                        </TableCell>
                        <TableCell>{row.action}</TableCell>
                        <TableCell>{row.resource_type} {row.resource_id ? shortenId(row.resource_id) : "--"}</TableCell>
                        <TableCell>{actor}</TableCell>
                        <TableCell>
                          <Badge className={cn("border", STATUS_STYLES[row.status])}>{row.status}</Badge>
                        </TableCell>
                        <TableCell className="max-w-xs truncate">{row.message}</TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            )}

            {nextCursor && (
              <div className="mt-4 flex justify-center">
                <Button onClick={handleLoadMore} disabled={loadingMore} variant="outline">
                  {loadingMore ? "Loading..." : "Load more"}
                </Button>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <Sheet open={Boolean(selectedId)} onOpenChange={(open) => !open && setSelectedId(null)}>
        <SheetContent side="right" className="w-full max-w-2xl overflow-y-auto bg-slate-950 text-white">
          <SheetHeader>
            <SheetTitle>Audit Event</SheetTitle>
          </SheetHeader>
          {selectedLoading ? (
            <div className="mt-6 space-y-3">
              <Skeleton className="h-6 w-full bg-slate-800" />
              <Skeleton className="h-6 w-full bg-slate-800" />
              <Skeleton className="h-6 w-full bg-slate-800" />
            </div>
          ) : selected ? (
            <div className="mt-6 space-y-4 text-sm">
              <div className="flex flex-wrap gap-2">
                <Badge className={cn("border", SEVERITY_STYLES[selected.severity])}>{selected.severity}</Badge>
                <Badge className={cn("border", STATUS_STYLES[selected.status])}>{selected.status}</Badge>
              </div>

              <div>
                <p className="text-xs text-slate-500">Action</p>
                <p className="text-sm">{selected.action}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Resource</p>
                <p>{selected.resource_type} {selected.resource_id ?? "--"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Actor</p>
                <p>{selected.actor_label || selected.actor_user_id || selected.actor_api_key_id || "--"}</p>
              </div>
              <div>
                <p className="text-xs text-slate-500">Message</p>
                <p>{selected.message}</p>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div>
                  <p className="text-xs text-slate-500">Request ID</p>
                  <p>{selected.request_id ?? "--"}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500">Created</p>
                  <p>{selected.created_at ?? "--"}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500">IP</p>
                  <p>{selected.ip ?? "--"}</p>
                </div>
                <div>
                  <p className="text-xs text-slate-500">User agent</p>
                  <p className="break-words">{selected.user_agent ?? "--"}</p>
                </div>
              </div>
              <div>
                <p className="text-xs text-slate-500">Metadata</p>
                <pre className="mt-2 rounded-md bg-slate-900 p-3 text-xs text-slate-200">
                  {JSON.stringify(selected.metadata ?? {}, null, 2)}
                </pre>
              </div>
            </div>
          ) : (
            <div className="mt-6 text-sm text-slate-400">Select an event to view details.</div>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}
