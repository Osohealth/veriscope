import { useEffect, useMemo, useState } from "react";
import { Link } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { useToast } from "@/hooks/useToast";
import { apiFetchJson } from "@/lib/apiFetch";
import { useAuth } from "@/auth/useAuth";
import { ArrowLeft, Copy, ExternalLink, Plus, RotateCw, Trash2 } from "lucide-react";

type Role = "OWNER" | "OPERATOR" | "VIEWER";
type Status = "ACTIVE" | "INVITED" | "DISABLED";

type Member = {
  id: string;
  user_id: string;
  email: string;
  display_name?: string | null;
  role: Role;
  status: Status;
  created_at: string;
};

type Invite = {
  id: string;
  email: string;
  role: Role;
  expires_at: string;
  created_at: string;
  accepted_at?: string | null;
  revoked_at?: string | null;
  invite_link?: string;
  invite_token?: string;
};

type ApiKeyRow = {
  id: string;
  user_id: string;
  name?: string | null;
  role?: Role | null;
  created_at: string;
  revoked_at?: string | null;
};

const ROLE_OPTIONS: Role[] = ["OWNER", "OPERATOR", "VIEWER"];

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

export default function TeamPage() {
  const { toast } = useToast();
  const { auth, role } = useAuth();
  const canManage = role === "OWNER";
  const showDevTools = import.meta.env.DEV || import.meta.env.VITE_SHOW_DEV_TOOLS === "true";
  const [members, setMembers] = useState<Member[]>([]);
  const [invites, setInvites] = useState<Invite[]>([]);
  const [keys, setKeys] = useState<ApiKeyRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [retentionDays, setRetentionDays] = useState<number>(90);
  const [retentionSaving, setRetentionSaving] = useState(false);
  const [inviteOpen, setInviteOpen] = useState(false);
  const [createKeyOpen, setCreateKeyOpen] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<Role>("VIEWER");
  const [keyName, setKeyName] = useState("");
  const [keyUserId, setKeyUserId] = useState<string>("");
  const [cachedInviteTokens, setCachedInviteTokens] = useState<Record<string, string>>({});

  const loadMembers = async () => {
    const payload = await apiFetchJson("/v1/team/members");
    setMembers(Array.isArray(payload?.items) ? payload.items : []);
  };

  const pruneInviteTokens = (pendingIds: Set<string>) => {
    if (!showDevTools) return;
    setCachedInviteTokens(prev => {
      const next = { ...prev };
      for (const id of Object.keys(next)) {
        if (!pendingIds.has(id)) delete next[id];
      }
      return next;
    });
  };

  const loadInvites = async () => {
    if (!canManage) {
      setInvites([]);
      return;
    }
    const payload = await apiFetchJson("/v1/team/invites");
    const items = Array.isArray(payload?.items) ? payload.items : [];
    const now = Date.now();
    const hydrated = showDevTools
      ? items.map((invite: Invite) => {
        const expiresAt = invite.expires_at ? new Date(invite.expires_at).getTime() : null;
        const isPending = !invite.accepted_at && !invite.revoked_at && (!expiresAt || expiresAt > now);
        const storedToken = isPending ? cachedInviteTokens[invite.id] ?? "" : "";
        const token = invite.invite_token ?? (storedToken || undefined);
        const link =
          invite.invite_link ??
          (token ? `${window.location.origin}/invite/accept?token=${token}` : undefined);
        return { ...invite, invite_token: token, invite_link: link };
      })
      : items;
    setInvites(hydrated);

    const pendingIds = new Set<string>(
      hydrated
        .filter((invite: Invite) => {
          const expiresAt = invite.expires_at ? new Date(invite.expires_at).getTime() : null;
          return !invite.accepted_at && !invite.revoked_at && (!expiresAt || expiresAt > now);
        })
        .map((invite: Invite) => invite.id),
    );
    pruneInviteTokens(pendingIds);
  };

  const loadKeys = async () => {
    const payload = await apiFetchJson("/v1/team/api-keys");
    setKeys(Array.isArray(payload?.items) ? payload.items : []);
  };

  const loadSettings = async () => {
    try {
      const payload = await apiFetchJson("/v1/tenant-settings");
      if (payload?.audit_retention_days) {
        setRetentionDays(Number(payload.audit_retention_days));
      }
    } catch {
      // ignore for non-operator roles
    }
  };

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        await Promise.all([loadMembers(), loadInvites(), loadKeys(), loadSettings()]);
      } catch (error: any) {
        toast({ title: "Failed to load team data", description: error?.message ?? "Unable to load team data.", variant: "destructive" });
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const memberOptions = useMemo(() => members.filter((m) => m.status === "ACTIVE"), [members]);

  const handleInvite = async () => {
    try {
      const payload = await apiFetchJson("/v1/team/invites", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: inviteEmail, role: inviteRole }),
      });
      toast({ title: "Invite created", description: payload?.invite_link ?? "Invitation created." });
      if (showDevTools && payload?.id && payload?.invite_token) {
        setCachedInviteTokens(prev => ({ ...prev, [payload.id]: payload.invite_token }));
      }
      setInviteEmail("");
      setInviteRole("VIEWER");
      setInviteOpen(false);
      await loadInvites();
      await loadMembers();
    } catch (error: any) {
      toast({ title: "Invite failed", description: error?.message ?? "Unable to invite member.", variant: "destructive" });
    }
  };

  const handleRoleChange = async (member: Member, nextRole: Role) => {
    try {
      await apiFetchJson(`/v1/team/members/${member.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ role: nextRole }),
      });
      await loadMembers();
      toast({ title: "Role updated" });
    } catch (error: any) {
      toast({ title: "Update failed", description: error?.message ?? "Unable to update role.", variant: "destructive" });
    }
  };

  const handleDisable = async (member: Member) => {
    try {
      await apiFetchJson(`/v1/team/members/${member.id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ status: "DISABLED" }),
      });
      await loadMembers();
      toast({ title: "Member disabled" });
    } catch (error: any) {
      toast({ title: "Disable failed", description: error?.message ?? "Unable to disable member.", variant: "destructive" });
    }
  };

  const handleRevoke = async (member: Member) => {
    try {
      await apiFetchJson(`/v1/team/members/${member.id}`, { method: "DELETE" });
      await loadMembers();
      toast({ title: "Member revoked" });
    } catch (error: any) {
      toast({ title: "Revoke failed", description: error?.message ?? "Unable to revoke member.", variant: "destructive" });
    }
  };

  const handleCreateKey = async () => {
    try {
      const body: any = { name: keyName || undefined };
      const targetUserId = canManage && keyUserId ? keyUserId : auth?.user_id;
      if (targetUserId) body.user_id = targetUserId;
      const payload = await apiFetchJson("/v1/team/api-keys", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      toast({ title: "API key created", description: payload?.api_key ?? "Key created. Copy it now." });
      setKeyName("");
      setKeyUserId("");
      setCreateKeyOpen(false);
      await loadKeys();
    } catch (error: any) {
      toast({ title: "Create key failed", description: error?.message ?? "Unable to create key.", variant: "destructive" });
    }
  };

  const handleRotateKey = async (key: ApiKeyRow) => {
    try {
      const payload = await apiFetchJson(`/v1/team/api-keys/${key.id}/rotate`, { method: "POST" });
      toast({ title: "Key rotated", description: payload?.api_key ?? "New key issued." });
      await loadKeys();
    } catch (error: any) {
      toast({ title: "Rotate failed", description: error?.message ?? "Unable to rotate key.", variant: "destructive" });
    }
  };

  const handleRetentionSave = async () => {
    setRetentionSaving(true);
    try {
      const payload = await apiFetchJson("/v1/tenant-settings", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ audit_retention_days: retentionDays }),
      });
      setRetentionDays(Number(payload?.audit_retention_days ?? retentionDays));
      toast({ title: "Retention updated" });
    } catch (error: any) {
      toast({ title: "Update failed", description: error?.message ?? "Unable to update retention.", variant: "destructive" });
    } finally {
      setRetentionSaving(false);
    }
  };

  const handleRevokeKey = async (key: ApiKeyRow) => {
    try {
      await apiFetchJson(`/v1/team/api-keys/${key.id}`, { method: "DELETE" });
      await loadKeys();
      toast({ title: "Key revoked" });
    } catch (error: any) {
      toast({ title: "Revoke failed", description: error?.message ?? "Unable to revoke key.", variant: "destructive" });
    }
  };

  const copyToClipboard = async (value: string, label: string) => {
    if (!value) return;
    try {
      await navigator.clipboard?.writeText(value);
      toast({ title: "Copied", description: label });
    } catch {
      toast({ title: "Copy failed", description: "Unable to copy to clipboard.", variant: "destructive" });
    }
  };

  return (
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
              <h1 className="mt-2 text-2xl font-semibold text-foreground">Team</h1>
              <p className="text-sm text-muted-foreground">Manage members, invites, and API keys.</p>
            </div>
            <div className="flex items-center gap-3">
              {showDevTools && (
                <Link href="/invite/accept">
                  <Button variant="outline">Accept invite</Button>
                </Link>
              )}
              <Button onClick={() => setInviteOpen(true)} disabled={!canManage}>
                <Plus className="mr-2 h-4 w-4" />
                Invite member
              </Button>
              <Button variant="outline" onClick={() => setCreateKeyOpen(true)}>
                <RotateCw className="mr-2 h-4 w-4" />
                New API key
              </Button>
            </div>
          </div>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8 space-y-8">
        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-0">
            {loading ? (
              <div className="p-6">
                <Skeleton className="h-6 w-1/3 mb-4" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Email</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {members.map((member) => (
                    <TableRow key={member.id}>
                      <TableCell>
                        <div className="text-sm text-foreground">{member.email}</div>
                        {auth?.user_id === member.user_id && (
                          <div className="text-xs text-muted-foreground">You</div>
                        )}
                      </TableCell>
                      <TableCell>
                        {canManage ? (
                          <Select
                            value={member.role}
                            onValueChange={(value) => handleRoleChange(member, value as Role)}
                          >
                            <SelectTrigger className="w-[140px]">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {ROLE_OPTIONS.map((r) => (
                                <SelectItem key={r} value={r}>
                                  {r}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        ) : (
                          <Badge variant="secondary">{member.role}</Badge>
                        )}
                      </TableCell>
                      <TableCell>
                        <Badge variant="secondary">{member.status}</Badge>
                      </TableCell>
                      <TableCell className="text-xs text-muted-foreground">
                        {formatRelativeTime(member.created_at)}
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          {canManage && member.status === "ACTIVE" && (
                            <Button size="sm" variant="outline" onClick={() => handleDisable(member)}>
                              Disable
                            </Button>
                          )}
                          {canManage && (
                            <Button size="sm" variant="ghost" onClick={() => handleRevoke(member)}>
                              <Trash2 className="h-3 w-3 mr-1" />
                              Revoke
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-0">
            <div className="px-6 py-4 border-b border-border/60">
              <h2 className="text-lg font-semibold">Invites</h2>
            </div>
            {loading ? (
              <div className="p-6">
                <Skeleton className="h-6 w-1/3 mb-4" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Email</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Expires</TableHead>
                    <TableHead>Status</TableHead>
                    {showDevTools && <TableHead>Actions</TableHead>}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {invites.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={showDevTools ? 5 : 4} className="text-center text-xs text-muted-foreground">
                        No pending invites.
                      </TableCell>
                    </TableRow>
                  ) : invites.map((invite) => (
                    <TableRow key={invite.id}>
                      <TableCell>{invite.email}</TableCell>
                      <TableCell>{invite.role}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatRelativeTime(invite.expires_at)}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">
                          {invite.accepted_at ? "ACCEPTED" : invite.revoked_at ? "REVOKED" : "PENDING"}
                        </Badge>
                      </TableCell>
                      {showDevTools && (
                        <TableCell>
                          {invite.invite_token ? (
                            <div className="flex flex-wrap items-center gap-2">
                              <Button
                                size="sm"
                                variant="outline"
                                onClick={() => copyToClipboard(invite.invite_token as string, "Invite token copied")}
                              >
                                <Copy className="mr-2 h-3 w-3" />
                                Copy token
                              </Button>
                              {invite.invite_link && (
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  onClick={() => copyToClipboard(invite.invite_link as string, "Invite link copied")}
                                >
                                  <Copy className="mr-2 h-3 w-3" />
                                  Copy link
                                </Button>
                              )}
                              {invite.invite_link && (
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  onClick={() => window.open(invite.invite_link, "_blank", "noopener")}
                                >
                                  <ExternalLink className="mr-2 h-3 w-3" />
                                  Open
                                </Button>
                              )}
                            </div>
                          ) : (
                            <span className="text-xs text-muted-foreground">--</span>
                          )}
                        </TableCell>
                      )}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-0">
            <div className="px-6 py-4 border-b border-border/60">
              <h2 className="text-lg font-semibold">Audit retention</h2>
              <p className="text-xs text-muted-foreground">Audit events retained for N days (min 7, max 3650).</p>
            </div>
            <div className="px-6 py-5 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
              <div className="max-w-xs w-full">
                <label className="text-xs font-medium text-muted-foreground">Retention days</label>
                <Input
                  className="mt-2"
                  type="number"
                  min={7}
                  max={3650}
                  value={retentionDays}
                  disabled={!canManage}
                  onChange={(event) => setRetentionDays(Number(event.target.value))}
                />
              </div>
              {canManage && (
                <Button onClick={handleRetentionSave} disabled={retentionSaving}>
                  {retentionSaving ? "Saving..." : "Save"}
                </Button>
              )}
              {!canManage && (
                <Badge variant="secondary">OWNER only</Badge>
              )}
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/60 bg-card/70">
          <CardContent className="p-0">
            <div className="px-6 py-4 border-b border-border/60">
              <h2 className="text-lg font-semibold">API Keys</h2>
            </div>
            {loading ? (
              <div className="p-6">
                <Skeleton className="h-6 w-1/3 mb-4" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : (
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Name</TableHead>
                    <TableHead>Role</TableHead>
                    <TableHead>Created</TableHead>
                    <TableHead>Status</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {keys.length === 0 ? (
                    <TableRow>
                      <TableCell colSpan={5} className="text-center text-xs text-muted-foreground">
                        No keys yet.
                      </TableCell>
                    </TableRow>
                  ) : keys.map((key) => (
                    <TableRow key={key.id}>
                      <TableCell>{key.name ?? "API key"}</TableCell>
                      <TableCell>{key.role ?? "--"}</TableCell>
                      <TableCell className="text-xs text-muted-foreground">{formatRelativeTime(key.created_at)}</TableCell>
                      <TableCell>
                        <Badge variant="secondary">{key.revoked_at ? "REVOKED" : "ACTIVE"}</Badge>
                      </TableCell>
                      <TableCell>
                        <div className="flex items-center gap-2">
                          <Button size="sm" variant="outline" onClick={() => handleRotateKey(key)}>
                            Rotate
                          </Button>
                          <Button size="sm" variant="ghost" onClick={() => handleRevokeKey(key)}>
                            Revoke
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            )}
          </CardContent>
        </Card>
      </div>

      <Dialog open={inviteOpen} onOpenChange={setInviteOpen}>
        <DialogContent className="bg-slate-900 border-slate-700 max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-white">Invite member</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground">Email</label>
              <Input
                className="mt-2"
                placeholder="teammate@company.com"
                value={inviteEmail}
                onChange={(event) => setInviteEmail(event.target.value)}
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Role</label>
              <Select value={inviteRole} onValueChange={(value) => setInviteRole(value as Role)}>
                <SelectTrigger className="mt-2">
                  <SelectValue placeholder="Select role" />
                </SelectTrigger>
                <SelectContent>
                  {ROLE_OPTIONS.map((r) => (
                    <SelectItem key={r} value={r}>
                      {r}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setInviteOpen(false)}>Cancel</Button>
            <Button onClick={handleInvite} disabled={!canManage}>Send invite</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={createKeyOpen} onOpenChange={setCreateKeyOpen}>
        <DialogContent className="bg-slate-900 border-slate-700 max-w-lg">
          <DialogHeader>
            <DialogTitle className="text-white">Create API key</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground">Key name</label>
              <Input className="mt-2" placeholder="Laptop key" value={keyName} onChange={(e) => setKeyName(e.target.value)} />
            </div>
            {canManage && (
              <div>
                <label className="text-xs font-medium text-muted-foreground">Member</label>
                <Select value={keyUserId} onValueChange={setKeyUserId}>
                  <SelectTrigger className="mt-2">
                    <SelectValue placeholder="Select member (optional)" />
                  </SelectTrigger>
                  <SelectContent>
                    {memberOptions.map((member) => (
                      <SelectItem key={member.user_id} value={member.user_id}>
                        {member.email}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
          </div>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setCreateKeyOpen(false)}>Cancel</Button>
            <Button onClick={handleCreateKey}>Create key</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
