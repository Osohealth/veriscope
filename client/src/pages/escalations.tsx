import { useEffect, useMemo, useState } from "react";
import { Link } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/use-toast";
import { apiFetchJson } from "@/lib/apiFetch";
import { cn } from "@/lib/utils";
import AlertsSubnav from "@/components/alerts-subnav";
import { useAuth } from "@/auth/useAuth";
import { ArrowLeft, Pencil, Plus, Trash2 } from "lucide-react";

type IncidentEscalationType = "ALL" | "SLA_AT_RISK" | "ENDPOINT_DOWN";
type SeverityMin = "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
type TargetType = "ROLE" | "USER" | "EMAIL" | "WEBHOOK";
type TargetRole = "OWNER" | "OPERATOR" | "VIEWER";

type EscalationPolicy = {
  id: string;
  tenant_id: string;
  enabled: boolean;
  level: number;
  after_minutes: number;
  incident_type: IncidentEscalationType;
  severity_min: SeverityMin;
  target_type?: TargetType;
  target_ref?: string;
  target_name?: string | null;
  routing_health?: {
    routes_total: number;
    routes_allowed: number;
    routes_blocked: number;
    blocked_reasons?: string[];
    warnings_count: number;
  } | null;
  created_at?: string;
  updated_at?: string;
};

type PolicyForm = {
  enabled: boolean;
  level: number;
  after_minutes: number;
  incident_type: IncidentEscalationType;
  severity_min: SeverityMin;
  target_type: TargetType;
  target_ref: string;
  target_name?: string | null;
};

type RoutingPreviewRecipient = {
  user_id: string;
  display_name?: string | null;
  email?: string | null;
};

type RoutingPreviewRoute = {
  user_id?: string;
  destination_type: "WEBHOOK" | "EMAIL";
  destination: string;
  destination_key: string;
  contact_method_id?: string;
  contact_method_type?: "WEBHOOK" | "EMAIL";
  contact_method_primary?: boolean;
  allowed: boolean;
  gate?: {
    state?: "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
    reason?: "DESTINATION_DISABLED" | "DESTINATION_PAUSED" | "DESTINATION_AUTO_PAUSED";
    ready_to_resume?: boolean | null;
    endpoint_health?: "OK" | "DEGRADED" | "DOWN" | null;
  };
  allowlist?: { ok: boolean; rule?: string };
};

type RoutingPreviewResult = {
  ok: boolean;
  reason?: string | null;
  resolved?: {
    recipients?: RoutingPreviewRecipient[];
    routes?: RoutingPreviewRoute[];
    warnings?: string[];
  };
};

type ValidationError = { code: string; path: string; message: string };
type ValidationWarning = { code: string; path: string; message: string };

const TYPE_OPTIONS: IncidentEscalationType[] = ["ALL", "SLA_AT_RISK", "ENDPOINT_DOWN"];
const SEVERITY_OPTIONS: SeverityMin[] = ["LOW", "MEDIUM", "HIGH", "CRITICAL"];
const TARGET_TYPE_OPTIONS: TargetType[] = ["ROLE", "USER", "EMAIL", "WEBHOOK"];
const TARGET_ROLE_OPTIONS: TargetRole[] = ["OWNER", "OPERATOR", "VIEWER"];

const TYPE_LABELS: Record<IncidentEscalationType, string> = {
  ALL: "All incidents",
  SLA_AT_RISK: "SLA at risk",
  ENDPOINT_DOWN: "Endpoint down",
};

const TYPE_ORDER: Record<IncidentEscalationType, number> = {
  ALL: 0,
  SLA_AT_RISK: 1,
  ENDPOINT_DOWN: 2,
};

const TARGET_TYPE_LABELS: Record<TargetType, string> = {
  ROLE: "Role",
  USER: "User",
  EMAIL: "Email",
  WEBHOOK: "Webhook",
};

const formatMinutes = (value: number) => `${value}m`;

const computeSummary = (items: EscalationPolicy[]) => {
  if (items.length === 0) {
    return {
      enabled: 0,
      maxLevel: 0,
      minMinutes: 0,
      maxMinutes: 0,
    };
  }
  const enabled = items.filter((item) => item.enabled).length;
  const levels = items.map((item) => item.level);
  const minutes = items.map((item) => item.after_minutes);
  return {
    enabled,
    maxLevel: Math.max(...levels),
    minMinutes: Math.min(...minutes),
    maxMinutes: Math.max(...minutes),
  };
};

const incidentTypeToFilter = (type: IncidentEscalationType) => {
  if (type === "SLA_AT_RISK") return "SLA";
  if (type === "ENDPOINT_DOWN") return "ENDPOINT";
  return "ALL";
};

const buildRoutingBadge = (policy: EscalationPolicy) => {
  const health = policy.routing_health;
  if (!health) {
    return { label: "--", className: "text-muted-foreground" };
  }
  const total = health.routes_total;
  const allowed = health.routes_allowed;
  if (total === 0) {
    return { label: "No routes (0/0)", className: "border-amber-500/40 text-amber-200" };
  }
  if (allowed === total) {
    return { label: `Routable (${allowed}/${total})`, className: "border-emerald-500/40 text-emerald-200" };
  }
  if (allowed === 0) {
    return { label: `Blocked (0/${total})`, className: "border-red-500/40 text-red-200" };
  }
  return { label: `Partial (${allowed}/${total})`, className: "border-amber-500/40 text-amber-200" };
};

const buildStatusBadges = (policy: EscalationPolicy) => {
  const badges: Array<{ label: string; className: string }> = [];
  if (policy.enabled) {
    badges.push({ label: "Enabled", className: "border-emerald-500/40 text-emerald-200" });
  } else {
    badges.push({ label: "Disabled", className: "border-muted/60 text-muted-foreground" });
  }
  if (policy.routing_health && policy.routing_health.routes_total === 0) {
    badges.push({ label: "Not routable", className: "border-amber-500/40 text-amber-200" });
  }
  return badges;
};

export default function EscalationsPage() {
  const { toast } = useToast();
  const { role } = useAuth();
  const [items, setItems] = useState<EscalationPolicy[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [editingPolicy, setEditingPolicy] = useState<EscalationPolicy | null>(null);
  const [form, setForm] = useState<PolicyForm>({
    enabled: true,
    level: 1,
    after_minutes: 5,
    incident_type: "ALL",
    severity_min: "HIGH",
    target_type: "ROLE",
    target_ref: "OWNER",
    target_name: null,
  });
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [userQuery, setUserQuery] = useState("");
  const [userResults, setUserResults] = useState<any[]>([]);
  const [userLoading, setUserLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewResult, setPreviewResult] = useState<RoutingPreviewResult | null>(null);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [validationErrors, setValidationErrors] = useState<ValidationError[]>([]);
  const [validationWarnings, setValidationWarnings] = useState<ValidationWarning[]>([]);

  const canEdit = role === "OWNER";

  const sortedItems = useMemo(() => {
    return [...items].sort((a, b) => {
      const typeRank = (TYPE_ORDER[a.incident_type] ?? 99) - (TYPE_ORDER[b.incident_type] ?? 99);
      if (typeRank !== 0) return typeRank;
      if (a.level !== b.level) return a.level - b.level;
      const targetTypeDiff = String(a.target_type ?? "").localeCompare(String(b.target_type ?? ""));
      if (targetTypeDiff !== 0) return targetTypeDiff;
      const targetRefDiff = String(a.target_ref ?? "").localeCompare(String(b.target_ref ?? ""));
      if (targetRefDiff !== 0) return targetRefDiff;
      if (a.after_minutes !== b.after_minutes) return a.after_minutes - b.after_minutes;
      return String(a.updated_at ?? "").localeCompare(String(b.updated_at ?? ""));
    });
  }, [items]);

  const summary = useMemo(() => computeSummary(items), [items]);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await apiFetchJson("/v1/incident-escalation-policies");
      setItems(Array.isArray(payload?.items) ? payload.items : []);
    } catch (err: any) {
      setError(err?.message ?? "Failed to load escalation policies.");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    if (form.target_type !== "USER") {
      setUserResults([]);
      setUserLoading(false);
      return;
    }
    const query = userQuery.trim();
    if (!query) {
      setUserResults([]);
      setUserLoading(false);
      return;
    }
    setUserLoading(true);
    const handle = setTimeout(async () => {
      try {
        const payload = await apiFetchJson(`/v1/team/users?query=${encodeURIComponent(query)}&limit=20`);
        setUserResults(Array.isArray(payload?.items) ? payload.items : []);
      } catch {
        setUserResults([]);
      } finally {
        setUserLoading(false);
      }
    }, 250);
    return () => clearTimeout(handle);
  }, [form.target_type, userQuery]);

  useEffect(() => {
    if (!dialogOpen) {
      setPreviewResult(null);
      setPreviewError(null);
      setPreviewLoading(false);
      setValidationErrors([]);
      setValidationWarnings([]);
      return;
    }
    setPreviewResult(null);
    setPreviewError(null);
    setValidationErrors([]);
    setValidationWarnings([]);
  }, [dialogOpen, form.target_type, form.target_ref]);

  const nextLevelForType = (type: IncidentEscalationType) => {
    const levels = items.filter((item) => item.incident_type === type).map((item) => item.level);
    const maxLevel = levels.length ? Math.max(...levels) : 0;
    return Math.min(maxLevel + 1, 10);
  };

  const openCreate = () => {
    const defaultType: IncidentEscalationType = "ALL";
    setEditingPolicy(null);
    setUserQuery("");
    setUserResults([]);
    setForm({
      enabled: true,
      incident_type: defaultType,
      severity_min: "HIGH",
      after_minutes: 5,
      level: nextLevelForType(defaultType),
      target_type: "ROLE",
      target_ref: "OWNER",
      target_name: null,
    });
    setDialogOpen(true);
  };

  const openEdit = (policy: EscalationPolicy) => {
    setEditingPolicy(policy);
    if ((policy.target_type ?? "ROLE") === "USER") {
      setUserQuery(policy.target_name ?? policy.target_ref ?? "");
    } else {
      setUserQuery("");
    }
    setUserResults([]);
    setForm({
      enabled: policy.enabled,
      incident_type: policy.incident_type,
      severity_min: policy.severity_min,
      after_minutes: policy.after_minutes,
      level: policy.level,
      target_type: policy.target_type ?? "ROLE",
      target_ref: policy.target_ref ?? "OWNER",
      target_name: policy.target_name ?? null,
    });
    setDialogOpen(true);
  };

  const monotonicWarning = useMemo(() => {
    const peers = items.filter((item) => item.incident_type === form.incident_type);
    const lowerLevels = peers.filter((item) => item.level < form.level).map((item) => item.after_minutes);
    if (lowerLevels.length === 0) return null;
    const maxLower = Math.max(...lowerLevels);
    if (form.after_minutes < maxLower) {
      return "L" + form.level + " triggers earlier than a lower level. This is unusual.";
    }
    return null;
  }, [form.after_minutes, form.incident_type, form.level, items]);

  const previewRecipients = previewResult?.resolved?.recipients ?? [];
  const previewRoutes = previewResult?.resolved?.routes ?? [];
  const previewWarnings = previewResult?.resolved?.warnings ?? [];
  const previewAllowedCount = previewRoutes.filter((route) => route.allowed).length;
  const formatWarning = (warning: string) => {
    if (warning === "NO_USER_CONTACT_METHOD") {
      return "No contact methods for one or more recipients.";
    }
    if (warning.startsWith("ROLE_HAS_RECIPIENTS_BUT_NO_ROUTES")) {
      const parts = warning.split(":");
      const count = parts[1];
      return count
        ? `Role has ${count} recipient${count === "1" ? "" : "s"} but no routes.`
        : "Role has recipients but no routes.";
    }
    return warning;
  };

  const parseApiError = (err: any) => {
    const message = String(err?.message ?? "");
    const parts = message.split(": ");
    if (parts.length < 2) return null;
    const raw = parts.slice(1).join(": ").trim();
    try {
      return JSON.parse(raw);
    } catch {
      return null;
    }
  };

  const validateForm = () => {
    if (!Number.isFinite(form.level) || form.level < 1 || form.level > 10) {
      toast({ title: "Level must be 1-10", variant: "destructive" });
      return false;
    }
    if (!Number.isFinite(form.after_minutes) || form.after_minutes < 1 || form.after_minutes > 10080) {
      toast({ title: "After minutes must be 1-10080", variant: "destructive" });
      return false;
    }
    if (!form.target_ref || !form.target_ref.trim()) {
      toast({ title: "Target is required", variant: "destructive" });
      return false;
    }
    if (form.target_type === "ROLE" && !TARGET_ROLE_OPTIONS.includes(form.target_ref.toUpperCase() as TargetRole)) {
      toast({ title: "Role must be OWNER, OPERATOR, or VIEWER", variant: "destructive" });
      return false;
    }
    if (form.target_type === "USER") {
      const uuidOk = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(form.target_ref.trim());
      if (!uuidOk) {
        toast({ title: "User target must be a valid UUID", variant: "destructive" });
        return false;
      }
    }
    if (form.target_type === "EMAIL") {
      const emailOk = /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(form.target_ref.trim());
      if (!emailOk) {
        toast({ title: "Email target must be valid", variant: "destructive" });
        return false;
      }
    }
    if (form.target_type === "WEBHOOK") {
      try {
        const url = new URL(form.target_ref.trim());
        if (!["http:", "https:"].includes(url.protocol)) {
          toast({ title: "Webhook must be http(s)", variant: "destructive" });
          return false;
        }
      } catch {
        toast({ title: "Webhook must be a valid URL", variant: "destructive" });
        return false;
      }
    }
    return true;
  };

  const runPreview = async () => {
    if (!form.target_ref || !form.target_ref.trim()) {
      setPreviewError("Target is required for preview.");
      return;
    }
    setPreviewLoading(true);
    setPreviewError(null);
    try {
      const payload = await apiFetchJson("/v1/routing/preview?include_blocked=true", {
        method: "POST",
        body: JSON.stringify({
          target_type: form.target_type,
          target_ref: form.target_ref.trim(),
        }),
      });
      setPreviewResult(payload?.result ?? null);
    } catch (err: any) {
      setPreviewError(err?.message ?? "Unable to preview routing.");
    } finally {
      setPreviewLoading(false);
    }
  };

  const savePolicy = async () => {
    if (!validateForm()) return;
    setSaving(true);
    setValidationErrors([]);
    setValidationWarnings([]);
    try {
      const validation = await apiFetchJson("/v1/routing/validate-policy", {
        method: "POST",
        body: JSON.stringify({
          incident_type: form.incident_type,
          severity_min: form.severity_min,
          level: form.level,
          after_minutes: form.after_minutes,
          include_blocked: true,
          targets: [{
            target_type: form.target_type,
            target_ref: form.target_ref.trim(),
            target_name: form.target_name ?? null,
          }],
        }),
      });

      const errors = Array.isArray(validation?.errors) ? validation.errors : [];
      const warnings = Array.isArray(validation?.warnings) ? validation.warnings : [];
      if (errors.length > 0 || validation?.ok === false) {
        setValidationErrors(errors);
        setValidationWarnings(warnings);
        toast({
          title: "Validation failed",
          description: "Fix the highlighted issues before saving.",
          variant: "destructive",
        });
        return;
      }

      setValidationWarnings(warnings);

      const normalized = validation?.normalized_policy;
      const normalizedTarget = normalized?.targets?.[0];
      if (!normalized || !normalizedTarget) {
        toast({
          title: "Validation failed",
          description: "No valid targets returned from validation.",
          variant: "destructive",
        });
        return;
      }

      await apiFetchJson("/v1/incident-escalation-policies", {
        method: "PATCH",
        body: JSON.stringify({
          enabled: form.enabled,
          level: normalized.level,
          after_minutes: normalized.after_minutes,
          incident_type: normalized.incident_type,
          severity_min: normalized.severity_min,
          target_type: normalizedTarget.target_type,
          target_ref: normalizedTarget.target_ref,
          target_name: normalizedTarget.target_name ?? null,
        }),
      });
      toast({ title: editingPolicy ? "Policy updated" : "Policy created" });
      setDialogOpen(false);
      await refresh();
    } catch (err: any) {
      const apiError = parseApiError(err);
      if (apiError?.error === "POLICY_NOT_ROUTABLE") {
        const reasons = Array.isArray(apiError.blocked_reasons) ? apiError.blocked_reasons.join(", ") : "";
        const warnings = Array.isArray(apiError.warnings) ? apiError.warnings.map((w: any) => w.message ?? w).join(", ") : "";
        toast({
          title: "Can't enable policy",
          description: [reasons && `Blocked: ${reasons}`, warnings && `Warnings: ${warnings}`].filter(Boolean).join(" | "),
          variant: "destructive",
        });
        return;
      }
      toast({
        title: "Save failed",
        description: err?.message ?? "Unable to save policy.",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  const toggleEnabled = async (policy: EscalationPolicy, enabled: boolean) => {
    if (!canEdit) return;
    try {
      await apiFetchJson("/v1/incident-escalation-policies", {
        method: "PATCH",
        body: JSON.stringify({
          enabled,
          level: policy.level,
          after_minutes: policy.after_minutes,
          incident_type: policy.incident_type,
          severity_min: policy.severity_min,
          target_type: policy.target_type ?? "ROLE",
          target_ref: policy.target_ref ?? "OWNER",
          target_name: policy.target_name ?? null,
        }),
      });
      await refresh();
    } catch (err: any) {
      const apiError = parseApiError(err);
      if (apiError?.error === "POLICY_NOT_ROUTABLE") {
        const reasons = Array.isArray(apiError.blocked_reasons) ? apiError.blocked_reasons.join(", ") : "";
        const warnings = Array.isArray(apiError.warnings) ? apiError.warnings.map((w: any) => w.message ?? w).join(", ") : "";
        toast({
          title: "Can't enable policy",
          description: [reasons && `Blocked: ${reasons}`, warnings && `Warnings: ${warnings}`].filter(Boolean).join(" | "),
          variant: "destructive",
        });
        return;
      }
      toast({
        title: "Update failed",
        description: err?.message ?? "Unable to update policy.",
        variant: "destructive",
      });
    }
  };

  const deletePolicy = async (policy: EscalationPolicy) => {
    setDeletingId(policy.id);
    try {
      await apiFetchJson(`/v1/incident-escalation-policies/${policy.id}`, { method: "DELETE" });
      toast({ title: "Policy deleted" });
      await refresh();
    } catch (err: any) {
      toast({
        title: "Delete failed",
        description: err?.message ?? "Unable to delete policy.",
        variant: "destructive",
      });
    } finally {
      setDeletingId(null);
    }
  };

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
              <h1 className="mt-2 text-2xl font-semibold text-foreground">Incident Escalations</h1>
              <p className="text-sm text-muted-foreground">
                Escalate unresolved incidents to OWNER alert channels based on time and severity.
              </p>
              <AlertsSubnav />
            </div>
            {canEdit && (
              <Button onClick={openCreate}>
                <Plus className="mr-2 h-4 w-4" />
                New policy
              </Button>
            )}
          </div>

          <div className="mt-6 grid gap-4 md:grid-cols-4">
            <Card className="border-border/60 bg-card/70">
              <CardContent className="py-4">
                <p className="text-xs text-muted-foreground">Policies enabled</p>
                <p className="mt-2 text-xl font-semibold">{summary.enabled}</p>
              </CardContent>
            </Card>
            <Card className="border-border/60 bg-card/70">
              <CardContent className="py-4">
                <p className="text-xs text-muted-foreground">Max level</p>
                <p className="mt-2 text-xl font-semibold">L{summary.maxLevel}</p>
              </CardContent>
            </Card>
            <Card className="border-border/60 bg-card/70">
              <CardContent className="py-4">
                <p className="text-xs text-muted-foreground">Shortest escalation</p>
                <p className="mt-2 text-xl font-semibold">{summary.minMinutes ? formatMinutes(summary.minMinutes) : "--"}</p>
              </CardContent>
            </Card>
            <Card className="border-border/60 bg-card/70">
              <CardContent className="py-4">
                <p className="text-xs text-muted-foreground">Longest escalation</p>
                <p className="mt-2 text-xl font-semibold">{summary.maxMinutes ? formatMinutes(summary.maxMinutes) : "--"}</p>
              </CardContent>
            </Card>
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
            {error}
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-xl border border-border/60 bg-card/40 p-10 text-center">
            <p className="text-sm text-muted-foreground">No escalation policies yet. Create L1 to start escalating unresolved incidents.</p>
          </div>
        ) : (
          <Card className="border-border/60 bg-card/70">
            <CardContent className="p-0">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Enabled</TableHead>
                    <TableHead>Level</TableHead>
                    <TableHead>After</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Severity min</TableHead>
                    <TableHead>Target</TableHead>
                    <TableHead>Routing</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {sortedItems.map((policy) => (
                    <TableRow key={policy.id}>
                      <TableCell>
                        <Switch
                          checked={policy.enabled}
                          disabled={!canEdit}
                          onCheckedChange={(value) => toggleEnabled(policy, Boolean(value))}
                        />
                        <div className="mt-2 flex flex-wrap gap-1">
                          {buildStatusBadges(policy).map((badge) => (
                            <Badge key={badge.label} variant="outline" className={badge.className}>
                              {badge.label}
                            </Badge>
                          ))}
                        </div>
                      </TableCell>
                      <TableCell className="text-xs">L{policy.level}</TableCell>
                      <TableCell className="text-xs">{formatMinutes(policy.after_minutes)}</TableCell>
                      <TableCell className="text-xs">{TYPE_LABELS[policy.incident_type] ?? policy.incident_type}</TableCell>
                      <TableCell className="text-xs">{policy.severity_min}</TableCell>
                      <TableCell className="text-xs">
                        <Badge variant="outline">
                          {(policy.target_type ?? "ROLE")}:{policy.target_name ?? policy.target_ref ?? "OWNER"}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-xs">
                        {(() => {
                          const badge = buildRoutingBadge(policy);
                          const reasons = policy.routing_health?.blocked_reasons?.length
                            ? `Blocked: ${policy.routing_health.blocked_reasons.join(", ")}`
                            : "";
                          const warnings = policy.routing_health?.warnings_count
                            ? `Warnings: ${policy.routing_health.warnings_count}`
                            : "";
                          const title = [reasons, warnings].filter(Boolean).join(" | ");
                          return (
                            <Badge
                              variant="outline"
                              className={badge.className}
                              title={title}
                            >
                              {badge.label}
                            </Badge>
                          );
                        })()}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Link
                            href={
                              incidentTypeToFilter(policy.incident_type) === "ALL"
                                ? `/incidents?severity_min=${policy.severity_min}`
                                : `/incidents?type=${incidentTypeToFilter(policy.incident_type)}&severity_min=${policy.severity_min}`
                            }
                          >
                            <a className="text-xs text-primary">Incidents</a>
                          </Link>
                          <Button
                            size="sm"
                            variant="outline"
                            disabled={!canEdit}
                            onClick={() => openEdit(policy)}
                          >
                            <Pencil className="mr-1 h-3 w-3" />
                            Edit
                          </Button>
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={!canEdit}
                              >
                                <Trash2 className="mr-1 h-3 w-3" />
                                Delete
                              </Button>
                            </AlertDialogTrigger>
                            <AlertDialogContent>
                              <AlertDialogHeader>
                                <AlertDialogTitle>Delete policy?</AlertDialogTitle>
                                <AlertDialogDescription>
                                  Deleting this policy stops escalations at this level for new incidents. Existing escalations already sent remain in history.
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel>Cancel</AlertDialogCancel>
                                <AlertDialogAction
                                  onClick={() => deletePolicy(policy)}
                                  disabled={deletingId === policy.id}
                                >
                                  Delete
                                </AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        )}
      </div>

      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>{editingPolicy ? "Edit escalation policy" : "Create escalation policy"}</DialogTitle>
            <DialogDescription>
              Escalate unresolved incidents to OWNER alert channels based on time and severity.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Enabled</label>
              <Switch checked={form.enabled} onCheckedChange={(value) => setForm((prev) => ({ ...prev, enabled: Boolean(value) }))} />
            </div>
            <div>
              <label className="text-sm font-medium">Level</label>
              <Input
                type="number"
                min={1}
                max={10}
                value={form.level}
                onChange={(event) => setForm((prev) => ({ ...prev, level: Number(event.target.value) }))}
                className="mt-2"
              />
            </div>
            <div>
              <label className="text-sm font-medium">After (minutes)</label>
              <Input
                type="number"
                min={1}
                max={10080}
                value={form.after_minutes}
                onChange={(event) => setForm((prev) => ({ ...prev, after_minutes: Number(event.target.value) }))}
                className="mt-2"
              />
            </div>
            <div>
              <label className="text-sm font-medium">Incident type</label>
              <Select
                value={form.incident_type}
                onValueChange={(value) => setForm((prev) => ({
                  ...prev,
                  incident_type: value as IncidentEscalationType,
                  level: nextLevelForType(value as IncidentEscalationType),
                }))}
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select type" />
                </SelectTrigger>
                <SelectContent>
                  {TYPE_OPTIONS.map((type) => (
                    <SelectItem key={type} value={type}>
                      {TYPE_LABELS[type]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm font-medium">Severity minimum</label>
              <Select
                value={form.severity_min}
                onValueChange={(value) => setForm((prev) => ({ ...prev, severity_min: value as SeverityMin }))}
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
            <div>
              <label className="text-sm font-medium">Target type</label>
              <Select
                value={form.target_type}
                onValueChange={(value) => {
                  setUserQuery("");
                  setUserResults([]);
                  setForm((prev) => ({
                    ...prev,
                    target_type: value as TargetType,
                    target_ref: value === "ROLE" ? "OWNER" : "",
                    target_name: null,
                  }));
                }}
              >
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select target type" />
                </SelectTrigger>
                <SelectContent>
                  {TARGET_TYPE_OPTIONS.map((type) => (
                    <SelectItem key={type} value={type}>
                      {TARGET_TYPE_LABELS[type]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div>
              <label className="text-sm font-medium">Target</label>
              {form.target_type === "ROLE" ? (
                <Select
                  value={(form.target_ref || "OWNER").toUpperCase()}
                  onValueChange={(value) => setForm((prev) => ({
                    ...prev,
                    target_ref: value,
                  }))}
                >
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Select role" />
                  </SelectTrigger>
                  <SelectContent>
                    {TARGET_ROLE_OPTIONS.map((role) => (
                      <SelectItem key={role} value={role}>
                        {role}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                <div className="mt-2 space-y-2">
                  {form.target_type === "USER" ? (
                    <>
                      <Input
                        type="text"
                        value={userQuery}
                        onChange={(event) => setUserQuery(event.target.value)}
                        placeholder="Search by name or email"
                      />
                      {userLoading && (
                        <p className="text-xs text-muted-foreground">Searching…</p>
                      )}
                      {!userLoading && userResults.length > 0 && (
                        <div className="max-h-40 space-y-1 overflow-auto rounded-md border border-border/60 bg-background/60 p-2">
                          {userResults.map((user) => (
                            <button
                              type="button"
                              key={user.user_id}
                              className={cn(
                                "flex w-full items-center justify-between rounded px-2 py-1 text-left text-xs transition hover:bg-muted/60",
                                form.target_ref === user.user_id && "bg-muted/60",
                              )}
                              onClick={() => {
                                setForm((prev) => ({
                                  ...prev,
                                  target_ref: user.user_id,
                                  target_name: user.name ?? user.email ?? null,
                                }));
                                setUserQuery(user.name ?? user.email ?? "");
                                setUserResults([]);
                              }}
                            >
                              <span className="font-medium">{user.name ?? user.email}</span>
                              <span className="text-muted-foreground">{user.email}</span>
                            </button>
                          ))}
                        </div>
                      )}
                      {form.target_ref && (
                        <p className="text-xs text-muted-foreground">
                          Selected: {form.target_name ?? form.target_ref}
                        </p>
                      )}
                    </>
                  ) : (
                    <Input
                      type={form.target_type === "EMAIL" ? "email" : "url"}
                      value={form.target_ref}
                      onChange={(event) => setForm((prev) => ({ ...prev, target_ref: event.target.value }))}
                      placeholder={
                        form.target_type === "EMAIL"
                          ? "alerts@example.com"
                          : "https://hooks.example.com/incident"
                      }
                    />
                  )}
                </div>
              )}
              <p className="mt-1 text-xs text-muted-foreground">
                {form.target_type === "ROLE" && "Escalations go to subscriptions owned by this role."}
                {form.target_type === "USER" && "Escalations route via this user's contact methods."}
                {form.target_type === "EMAIL" && "Escalations are delivered directly to this address."}
                {form.target_type === "WEBHOOK" && "Escalations are delivered directly to this webhook URL."}
              </p>
            </div>
            <div className="rounded-lg border border-border/60 bg-card/60 px-3 py-3">
              <div className="flex items-center justify-between">
                <p className="text-sm font-medium">Routing preview</p>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={runPreview}
                  disabled={previewLoading || !form.target_ref.trim()}
                >
                  {previewLoading ? "Previewing..." : "Preview routing"}
                </Button>
              </div>
              <p className="mt-1 text-xs text-muted-foreground">Read-only. No deliveries are created.</p>
              {previewError && (
                <p className="mt-2 text-xs text-destructive">{previewError}</p>
              )}
              {previewResult && (
                <div className="mt-3 space-y-3 text-xs">
                  <div className="flex items-center gap-2">
                    <Badge variant={previewResult.ok ? "secondary" : "destructive"}>
                      {previewResult.ok ? "Allowed" : "Blocked"}
                    </Badge>
                    {!previewResult.ok && previewResult.reason && (
                      <span className="text-muted-foreground">{previewResult.reason}</span>
                    )}
                    {previewResult.ok && (
                      <span className="text-muted-foreground">
                        {previewAllowedCount} route{previewAllowedCount === 1 ? "" : "s"} allowed
                      </span>
                    )}
                  </div>
                  <div className="text-muted-foreground">
                    Recipients: {previewRecipients.length}
                  </div>
                  {previewWarnings.length > 0 && (
                    <div className="space-y-1">
                      {previewWarnings.map((warning, idx) => (
                        <div key={idx} className="text-xs text-amber-200">
                          {formatWarning(warning)}
                        </div>
                      ))}
                    </div>
                  )}
                  {previewRoutes.length === 0 ? (
                    <p className="text-muted-foreground">No routes resolved.</p>
                  ) : (
                    <div className="space-y-2">
                      {previewRoutes.slice(0, 3).map((route, idx) => {
                        const blockedReason =
                          route.gate?.reason ??
                          (route.gate?.endpoint_health === "DOWN"
                            ? "ENDPOINT_DOWN"
                            : route.allowlist?.ok === false
                              ? "DESTINATION_NOT_ALLOWED"
                              : "BLOCKED");
                        return (
                          <div key={`${route.destination_key}-${idx}`} className="rounded-md border border-border/40 bg-background/40 p-2">
                            <div className="flex items-center justify-between">
                              <span className="font-medium">{route.destination_type}</span>
                              <Badge variant={route.allowed ? "secondary" : "outline"}>
                                {route.allowed ? "Allowed" : "Blocked"}
                              </Badge>
                            </div>
                            <div className="mt-1 break-all">{route.destination}</div>
                            <div className="mt-1 font-mono text-[10px] text-muted-foreground">{route.destination_key}</div>
                            {!route.allowed && (
                              <div className="mt-1 text-[10px] text-muted-foreground">Blocked: {blockedReason}</div>
                            )}
                            {route.gate?.state && (
                              <div className="mt-1 text-[10px] text-muted-foreground">
                                State: {route.gate.state}
                                {route.gate.ready_to_resume ? " (ready)" : ""}
                              </div>
                            )}
                            {route.gate?.endpoint_health && (
                              <div className="mt-1 text-[10px] text-muted-foreground">
                                Endpoint health: {route.gate.endpoint_health}
                              </div>
                            )}
                            {route.contact_method_type && (
                              <div className="mt-1 text-[10px] text-muted-foreground">
                                Contact: {route.contact_method_type}
                                {route.contact_method_primary ? " (primary)" : ""}
                              </div>
                            )}
                          </div>
                        );
                      })}
                      {previewRoutes.length > 3 && (
                        <div className="text-xs text-muted-foreground">
                          +{previewRoutes.length - 3} more route{previewRoutes.length - 3 === 1 ? "" : "s"}
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
            {(validationErrors.length > 0 || validationWarnings.length > 0) && (
              <div className="space-y-2 rounded-lg border border-border/60 bg-background/50 px-3 py-2 text-xs">
                {validationErrors.length > 0 && (
                  <div className="space-y-1 text-destructive">
                    <p className="font-medium">Validation errors</p>
                    {validationErrors.map((error, idx) => (
                      <div key={`${error.code}-${idx}`}>
                        {error.message}
                      </div>
                    ))}
                  </div>
                )}
                {validationWarnings.length > 0 && (
                  <div className="space-y-1 text-amber-200">
                    <p className="font-medium">Warnings</p>
                    {validationWarnings.map((warning, idx) => (
                      <div key={`${warning.code}-${idx}`}>
                        {warning.message}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
            {monotonicWarning && (
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-200">
                {monotonicWarning}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              Cancel
            </Button>
            <Button onClick={savePolicy} disabled={saving || !canEdit} className={cn(!canEdit && "opacity-70")}>
              {saving ? "Saving..." : "Save policy"}
            </Button>
          </DialogFooter>
          {!canEdit && (
            <div className="mt-2 text-xs text-muted-foreground">Owner role required to edit policies.</div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
