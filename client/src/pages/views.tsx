import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { buildQueryFromFilters } from "@/hooks/use-terminal-store";
import { deleteView, getSavedViewsError, listSavedViews, updateView, type SavedView } from "@/lib/saved-views";
import { LoadingState } from "@/components/loading-state";
import { ArrowLeft, Eye, Pencil, Trash2 } from "lucide-react";

const buildViewUrl = (view: SavedView) => {
  const selection = view.selection?.entityId
    ? {
        id: view.selection.entityId,
        name: view.selection.entityName ?? view.selection.entityId,
        type: (view.selection.entityType ?? "unknown") as any,
      }
    : null;
  const query = buildQueryFromFilters(view.filters, selection ?? undefined);
  return query ? `${view.route}?${query}` : view.route;
};

export default function ViewsPage() {
  const [, setLocation] = useLocation();
  const [views, setViews] = useState<SavedView[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingName, setEditingName] = useState<string>("");

  const loadViews = () => {
    setViews(listSavedViews());
    setError(getSavedViewsError());
    setLoaded(true);
  };

  useEffect(() => {
    loadViews();
  }, []);

  const handleOpen = (view: SavedView) => {
    setLocation(buildViewUrl(view));
  };

  const handleRename = (view: SavedView) => {
    setEditingId(view.id);
    setEditingName(view.name);
  };

  const handleRenameSave = () => {
    if (!editingId || !editingName.trim()) return;
    updateView(editingId, { name: editingName.trim() });
    loadViews();
    setEditingId(null);
  };

  const handleDelete = (id: string) => {
    deleteView(id);
    loadViews();
  };

  const viewRows = useMemo(() => views, [views]);

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="sticky top-0 z-40 border-b border-border bg-card/95 backdrop-blur">
        <div className="mx-auto flex w-full max-w-5xl items-center gap-4 px-6 py-4">
          <button
            type="button"
            onClick={() => window.history.length > 1 ? window.history.back() : (window.location.href = "/")}
            className="flex items-center gap-2 text-sm text-muted-foreground hover:text-primary"
          >
            <ArrowLeft className="h-4 w-4" />
            Back
          </button>
          <div className="text-lg font-semibold">Saved Views</div>
          <Badge variant="secondary">{views.length} total</Badge>
        </div>
      </header>

      <div className="mx-auto w-full max-w-5xl space-y-6 px-6 py-6">
        <Card className="border-border/60">
          <CardHeader>
            <CardTitle className="text-sm font-semibold">Views list</CardTitle>
          </CardHeader>
          <CardContent>
            {!loaded ? (
              <LoadingState label="Loading saved views..." />
            ) : error ? (
              <div className="flex items-center justify-between rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
                <span>{error}</span>
                <Button size="sm" variant="outline" onClick={loadViews}>
                  Reload
                </Button>
              </div>
            ) : viewRows.length === 0 ? (
              <div className="text-sm text-muted-foreground">No saved views yet.</div>
            ) : (
              <div className="space-y-3">
                {viewRows.map((view) => (
                  <div
                    key={view.id}
                    className="flex flex-wrap items-center justify-between gap-3 rounded-md border border-border/50 bg-background/40 p-3"
                  >
                    <div>
                      {editingId === view.id ? (
                        <div className="flex items-center gap-2">
                          <Input value={editingName} onChange={(event) => setEditingName(event.target.value)} />
                          <Button size="sm" onClick={handleRenameSave}>
                            Save
                          </Button>
                        </div>
                      ) : (
                        <div className="text-sm font-semibold">{view.name}</div>
                      )}
                      <div className="text-xs text-muted-foreground">
                        {view.route} | Updated {new Date(view.updatedAt).toLocaleString()}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button size="sm" variant="outline" onClick={() => handleOpen(view)}>
                        <Eye className="mr-2 h-4 w-4" />
                        Open
                      </Button>
                      <Button size="sm" variant="outline" onClick={() => handleRename(view)}>
                        <Pencil className="mr-2 h-4 w-4" />
                        Rename
                      </Button>
                      <Button size="sm" variant="outline" onClick={() => handleDelete(view.id)}>
                        <Trash2 className="mr-2 h-4 w-4" />
                        Delete
                      </Button>
                    </div>
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
