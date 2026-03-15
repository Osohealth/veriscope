import type { TerminalFilters, SelectedEntity } from "@/hooks/use-terminal-store";

export type SavedView = {
  id: string;
  name: string;
  route: string;
  filters: TerminalFilters;
  selection?: {
    entityId?: string;
    entityName?: string;
    entityType?: string;
  };
  createdAt: string;
  updatedAt: string;
};

const STORAGE_KEY = "veriscope_saved_views";
let lastReadError: string | null = null;

const readViews = (): SavedView[] => {
  if (typeof window === "undefined") return [];
  const raw = localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    lastReadError = null;
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    lastReadError = null;
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    lastReadError = "Saved views data was corrupted and has been cleared.";
    localStorage.removeItem(STORAGE_KEY);
    return [];
  }
};

const writeViews = (views: SavedView[]) => {
  if (typeof window === "undefined") return;
  localStorage.setItem(STORAGE_KEY, JSON.stringify(views));
};

const normalizeFilters = (filters: TerminalFilters): TerminalFilters => {
  const cleanList = (value?: string[]) => (value && value.length ? value : undefined);
  return {
    ...filters,
    commodity: cleanList(filters.commodity) ?? ["Crude"],
    region: cleanList(filters.region),
    origin: cleanList(filters.origin),
    destination: cleanList(filters.destination),
    hub: cleanList(filters.hub),
    riskTags: cleanList(filters.riskTags),
    severity: filters.severity,
    status: filters.status,
  };
};

export const listSavedViews = () => readViews().sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));

export const getSavedViewsError = () => lastReadError;

export const saveView = (args: {
  name: string;
  route: string;
  filters: TerminalFilters;
  selection?: SelectedEntity | null;
}) => {
  const now = new Date().toISOString();
  const views = readViews();
  const view: SavedView = {
    id: crypto.randomUUID(),
    name: args.name,
    route: args.route,
    filters: normalizeFilters(args.filters),
    selection: args.selection
      ? {
          entityId: args.selection.id,
          entityName: args.selection.name,
          entityType: args.selection.type,
        }
      : undefined,
    createdAt: now,
    updatedAt: now,
  };
  const updated = [view, ...views];
  writeViews(updated);
  return view;
};

export const updateView = (id: string, patch: Partial<Pick<SavedView, "name" | "route" | "filters" | "selection">>) => {
  const views = readViews();
  const updated = views.map((view) =>
    view.id === id
      ? {
          ...view,
          ...patch,
          filters: patch.filters ? normalizeFilters(patch.filters) : view.filters,
          updatedAt: new Date().toISOString(),
        }
      : view
  );
  writeViews(updated);
  return updated.find((view) => view.id === id) ?? null;
};

export const deleteView = (id: string) => {
  const views = readViews().filter((view) => view.id !== id);
  writeViews(views);
  return views;
};
