export type Investigation = {
  id: string;
  title: string;
  status: "open" | "in_progress" | "closed";
  hypothesis?: string;
  notes?: string;
  sourceRoute?: string;
  sourceViewId?: string;
  linkedEntityId?: string;
  linkedEntityName?: string;
  linkedEntityType?: string;
  linkedAlertId?: string;
  owner?: string;
  createdAt: string;
  updatedAt: string;
};

const STORAGE_KEY = "veriscope:investigations";
let lastReadError: string | null = null;

const readAll = (): Investigation[] => {
  if (typeof window === "undefined") return [];
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    lastReadError = null;
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    lastReadError = null;
    return parsed;
  } catch {
    lastReadError = "Investigation data was corrupted and has been cleared.";
    window.localStorage.removeItem(STORAGE_KEY);
    return [];
  }
};

const writeAll = (items: Investigation[]) => {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(items));
};

const generateId = () => {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `inv_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
};

export const listInvestigations = () => {
  return readAll().sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
};

export const getInvestigationsError = () => lastReadError;

export const getInvestigation = (id: string) => {
  return readAll().find((item) => item.id === id) ?? null;
};

export const createInvestigation = (payload: Partial<Investigation>) => {
  const now = new Date().toISOString();
  const investigation: Investigation = {
    id: generateId(),
    title: payload.title ?? "New investigation",
    status: payload.status ?? "open",
    hypothesis: payload.hypothesis ?? "",
    notes: payload.notes ?? "",
    sourceRoute: payload.sourceRoute,
    sourceViewId: payload.sourceViewId,
    linkedEntityId: payload.linkedEntityId,
    linkedEntityName: payload.linkedEntityName,
    linkedEntityType: payload.linkedEntityType,
    linkedAlertId: payload.linkedAlertId,
    owner: payload.owner ?? "You",
    createdAt: now,
    updatedAt: now,
  };
  const items = readAll();
  writeAll([investigation, ...items]);
  return investigation;
};

export const updateInvestigation = (id: string, patch: Partial<Investigation>) => {
  const items = readAll();
  const next = items.map((item) =>
    item.id === id
      ? { ...item, ...patch, updatedAt: new Date().toISOString() }
      : item
  );
  writeAll(next);
  return next.find((item) => item.id === id) ?? null;
};

export const deleteInvestigation = (id: string) => {
  const items = readAll().filter((item) => item.id !== id);
  writeAll(items);
};
