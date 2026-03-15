import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
import { ArrowLeft, ClipboardList, Plus, Trash2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import { createInvestigation, deleteInvestigation, getInvestigationsError, listInvestigations, type Investigation } from "@/lib/investigations";
import { LoadingState } from "@/components/loading-state";

const statusStyles: Record<Investigation["status"], string> = {
  open: "bg-sky-500/15 text-sky-300 border-sky-500/30",
  in_progress: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  closed: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
};

export default function InvestigationsPage() {
  const [, setLocation] = useLocation();
  const [items, setItems] = useState<Investigation[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createOpen, setCreateOpen] = useState(false);
  const [form, setForm] = useState({ title: "", hypothesis: "" });

  const load = () => {
    setItems(listInvestigations());
    setError(getInvestigationsError());
  };

  useEffect(() => {
    load();
    setLoaded(true);
  }, []);

  const handleCreate = () => {
    const title = form.title.trim() || "New investigation";
    const investigation = createInvestigation({
      title,
      hypothesis: form.hypothesis.trim(),
      status: "open",
    });
    setForm({ title: "", hypothesis: "" });
    setCreateOpen(false);
    load();
    setLocation(`/investigations/${investigation.id}`);
  };

  const handleDelete = (id: string) => {
    deleteInvestigation(id);
    load();
  };

  const rows = useMemo(() => items, [items]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-40 border-b border-border bg-card/95 backdrop-blur">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={() => window.history.length > 1 ? window.history.back() : (window.location.href = "/")}
              className="flex items-center gap-2 text-sm text-muted-foreground hover:text-primary"
            >
              <ArrowLeft className="h-4 w-4" />
              Back
            </button>
            <div className="flex items-center gap-2 text-lg font-semibold">
              <ClipboardList className="h-5 w-5 text-primary" />
              Investigations
            </div>
          </div>
          <Button onClick={() => setCreateOpen(true)}>
            <Plus className="mr-2 h-4 w-4" />
            New investigation
          </Button>
        </div>
      </header>

      <div className="mx-auto w-full max-w-6xl px-6 py-6">
        <Card className="border-border/60">
          <CardHeader className="flex-row items-center justify-between space-y-0">
            <CardTitle className="text-sm font-semibold">Active investigations</CardTitle>
            <Badge variant="secondary">{rows.length} tracked</Badge>
          </CardHeader>
          <CardContent>
            {!loaded ? (
              <div className="rounded-md border border-border/50 bg-background/40 p-4">
                <LoadingState label="Loading investigations..." />
              </div>
            ) : error ? (
              <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-4 text-sm text-destructive">
                <span>{error}</span>
                <Button size="sm" variant="outline" onClick={load}>
                  Reload
                </Button>
              </div>
            ) : rows.length === 0 ? (
              <div className="rounded-md border border-border/50 bg-background/40 p-4 text-sm text-muted-foreground">
                No investigations yet. Create one from Terminal, Flows, Congestion, or Alerts.
              </div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-left text-xs text-muted-foreground">
                      <th className="py-2">Title</th>
                      <th>Status</th>
                      <th>Linked</th>
                      <th>Owner</th>
                      <th>Updated</th>
                      <th className="text-right">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((item) => (
                      <tr key={item.id} className="border-t border-border/40">
                        <td className="py-2">
                          <Link href={`/investigations/${item.id}`} className="font-medium text-primary hover:underline">
                            {item.title}
                          </Link>
                        </td>
                        <td>
                          <Badge variant="outline" className={cn(statusStyles[item.status])}>
                            {item.status.replace("_", " ")}
                          </Badge>
                        </td>
                        <td className="text-xs text-muted-foreground">
                          {item.linkedAlertId
                            ? "Alert"
                            : item.linkedEntityName
                            ? `${item.linkedEntityName} (${item.linkedEntityType ?? "entity"})`
                            : "-"}
                        </td>
                        <td className="text-xs text-muted-foreground">{item.owner ?? "You"}</td>
                        <td className="text-xs text-muted-foreground">{new Date(item.updatedAt).toLocaleString()}</td>
                        <td className="text-right">
                          <Button variant="ghost" size="sm" onClick={() => handleDelete(item.id)}>
                            <Trash2 className="h-4 w-4" />
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create investigation</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <Label>Title</Label>
              <Input value={form.title} onChange={(event) => setForm((prev) => ({ ...prev, title: event.target.value }))} />
            </div>
            <div>
              <Label>Hypothesis</Label>
              <Textarea
                value={form.hypothesis}
                onChange={(event) => setForm((prev) => ({ ...prev, hypothesis: event.target.value }))}
                placeholder="Describe the working hypothesis."
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreate}>Create</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
