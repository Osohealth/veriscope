import { useEffect, useState } from "react";
import { Link, useLocation, useRoute } from "wouter";
import { ArrowLeft, ExternalLink, Save, Trash2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";
import { deleteInvestigation, getInvestigation, updateInvestigation, type Investigation } from "@/lib/investigations";

const statusStyles: Record<Investigation["status"], string> = {
  open: "bg-sky-500/15 text-sky-300 border-sky-500/30",
  in_progress: "bg-amber-500/15 text-amber-300 border-amber-500/30",
  closed: "bg-emerald-500/15 text-emerald-300 border-emerald-500/30",
};

export default function InvestigationDetailPage() {
  const [, setLocation] = useLocation();
  const [match, params] = useRoute<{ id: string }>("/investigations/:id");
  const [investigation, setInvestigation] = useState<Investigation | null>(null);
  const [form, setForm] = useState({
    title: "",
    status: "open" as Investigation["status"],
    hypothesis: "",
    notes: "",
  });

  useEffect(() => {
    if (!match) return;
    const item = getInvestigation(params.id);
    setInvestigation(item);
    if (item) {
      setForm({
        title: item.title,
        status: item.status,
        hypothesis: item.hypothesis ?? "",
        notes: item.notes ?? "",
      });
    }
  }, [match, params?.id]);

  if (!match) return null;

  if (!investigation) {
    return (
      <div className="min-h-screen bg-background text-foreground">
        <div className="mx-auto max-w-4xl px-6 py-12">
          <Card className="border-border/60">
            <CardContent className="space-y-4 pt-6">
              <div className="text-lg font-semibold">Investigation not found</div>
              <Link href="/investigations">
                <Button variant="outline">Back to investigations</Button>
              </Link>
            </CardContent>
          </Card>
        </div>
      </div>
    );
  }

  const handleSave = () => {
    const updated = updateInvestigation(investigation.id, {
      title: form.title.trim() || "Untitled investigation",
      status: form.status,
      hypothesis: form.hypothesis,
      notes: form.notes,
    });
    if (updated) setInvestigation(updated);
  };

  const handleDelete = () => {
    deleteInvestigation(investigation.id);
    setLocation("/investigations");
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-40 border-b border-border bg-card/95 backdrop-blur">
        <div className="mx-auto flex w-full max-w-5xl items-center justify-between px-6 py-4">
          <div className="flex items-center gap-4">
            <button
              type="button"
              onClick={() => window.history.length > 1 ? window.history.back() : (window.location.href = "/")}
              className="flex items-center gap-2 text-sm text-muted-foreground hover:text-primary"
            >
              <ArrowLeft className="h-4 w-4" />
              Back
            </button>
            <Link href="/investigations" className="flex items-center gap-2 text-sm text-muted-foreground hover:text-primary">
              <ArrowLeft className="h-4 w-4" />
              Back to investigations
            </Link>
            <div className="flex items-center gap-2 text-lg font-semibold">
              Investigation detail
            </div>
            <Badge variant="outline" className={cn(statusStyles[investigation.status])}>
              {investigation.status.replace("_", " ")}
            </Badge>
          </div>
          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={handleSave}>
              <Save className="mr-2 h-4 w-4" />
              Save
            </Button>
            <Button variant="ghost" onClick={handleDelete}>
              <Trash2 className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto w-full max-w-5xl space-y-6 px-6 py-6">
        <Card className="border-border/60">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">Summary</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div>
              <Label>Title</Label>
              <Input value={form.title} onChange={(event) => setForm((prev) => ({ ...prev, title: event.target.value }))} />
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label>Status</Label>
                <Select value={form.status} onValueChange={(value) => setForm((prev) => ({ ...prev, status: value as Investigation["status"] }))}>
                  <SelectTrigger className="mt-2">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="open">Open</SelectItem>
                    <SelectItem value="in_progress">In progress</SelectItem>
                    <SelectItem value="closed">Closed</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Linked</Label>
                <div className="text-xs text-muted-foreground">
                  {investigation.linkedAlertId
                    ? `Alert ${investigation.linkedAlertId}`
                    : investigation.linkedEntityName
                      ? `${investigation.linkedEntityName} (${investigation.linkedEntityType ?? "entity"})`
                      : "No linked entity or alert."}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="border-border/60">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">Hypothesis</CardTitle>
          </CardHeader>
          <CardContent>
            <Textarea
              value={form.hypothesis}
              onChange={(event) => setForm((prev) => ({ ...prev, hypothesis: event.target.value }))}
              placeholder="Describe the working hypothesis."
              className="min-h-[120px]"
            />
          </CardContent>
        </Card>

        <Card className="border-border/60">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">Notes</CardTitle>
          </CardHeader>
          <CardContent>
            <Textarea
              value={form.notes}
              onChange={(event) => setForm((prev) => ({ ...prev, notes: event.target.value }))}
              placeholder="Capture findings, sources, or next actions."
              className="min-h-[140px]"
            />
          </CardContent>
        </Card>

        <Card className="border-border/60">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">Source context</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3 text-sm text-muted-foreground">
            <div>Source route: {investigation.sourceRoute ?? "-"}</div>
            {investigation.sourceViewId && <div>Saved view: {investigation.sourceViewId}</div>}
            <div className="flex items-center gap-2">
              <Link href={investigation.sourceRoute ?? "/terminal"}>
                <Button variant="outline" size="sm">
                  Open source context
                  <ExternalLink className="ml-2 h-4 w-4" />
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
