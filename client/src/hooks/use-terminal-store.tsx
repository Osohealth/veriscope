import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useLocation } from "wouter";

export type TerminalFilters = {
  commodity: string[];
  mode: "sea" | "air" | "rail" | "cross";
  timeMode: "live" | "range";
  timeWindow?: "24h" | "7d" | "30d";
  region?: string[];
  origin?: string[];
  destination?: string[];
  hub?: string[];
  riskTags?: string[];
  severity?: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  status?: "OPEN" | "ACKED" | "RESOLVED";
};

export type SelectedEntity = {
  id: string;
  name: string;
  type: "port" | "corridor" | "zone" | "lane" | "unknown";
};

type TerminalStore = {
  filters: TerminalFilters;
  setFilters: (next: TerminalFilters) => void;
  updateFilters: (patch: Partial<TerminalFilters>) => void;
  resetFilters: () => void;
  selectedEntity: SelectedEntity | null;
  setSelectedEntity: (next: SelectedEntity | null) => void;
};

export const DEFAULT_FILTERS: TerminalFilters = {
  commodity: ["Crude"],
  mode: "cross",
  timeMode: "live",
  timeWindow: undefined,
  region: undefined,
  origin: undefined,
  destination: undefined,
  hub: undefined,
  riskTags: ["Sanctions", "Weather", "Conflict"],
};

const TerminalStoreContext = createContext<TerminalStore | null>(null);

const parseList = (value: string | null) =>
  value ? value.split(",").map((item) => item.trim()).filter(Boolean) : undefined;

const serializeList = (value?: string[]) => {
  if (!value || value.length === 0) return undefined;
  const normalized = Array.from(new Set(value.map((item) => item.trim()).filter(Boolean))).sort();
  return normalized.length ? normalized.join(",") : undefined;
};

const parseFiltersFromUrl = (location: string): TerminalFilters => {
  const query = location.split("?")[1] ?? "";
  const params = new URLSearchParams(query);
  const commodity = parseList(params.get("commodity")) ?? DEFAULT_FILTERS.commodity;
  const mode = (params.get("mode") as TerminalFilters["mode"]) ?? DEFAULT_FILTERS.mode;
  const time = (params.get("time") ?? "live").toLowerCase();
  const timeMode = time === "live" ? "live" : "range";
  const timeWindow = time === "live" ? undefined : (time as TerminalFilters["timeWindow"]);

  return {
    commodity,
    mode: ["sea", "air", "rail", "cross"].includes(mode) ? mode : DEFAULT_FILTERS.mode,
    timeMode,
    timeWindow: ["24h", "7d", "30d"].includes(timeWindow ?? "") ? timeWindow : undefined,
    region: parseList(params.get("region")),
    origin: parseList(params.get("origin")),
    destination: parseList(params.get("destination")),
    hub: parseList(params.get("hub")),
    riskTags: parseList(params.get("risk")) ?? DEFAULT_FILTERS.riskTags,
    severity: (params.get("severity")?.toUpperCase() as TerminalFilters["severity"]) ?? undefined,
    status: (params.get("status")?.toUpperCase() as TerminalFilters["status"]) ?? undefined,
  };
};

const parseSelectionFromUrl = (location: string): SelectedEntity | null => {
  const query = location.split("?")[1] ?? "";
  const params = new URLSearchParams(query);
  const id = params.get("sel_id");
  if (!id) return null;
  const name = params.get("sel_name") ?? id;
  const type = (params.get("sel_type") ?? "unknown") as SelectedEntity["type"];
  return { id, name, type };
};

const isSameList = (value?: string[], baseline?: string[]) => {
  const normalized = serializeList(value);
  const baselineNormalized = serializeList(baseline);
  return normalized === baselineNormalized;
};

export const buildQueryFromFilters = (filters: TerminalFilters, selection?: SelectedEntity | null) => {
  const params = new URLSearchParams();
  const commodity = serializeList(filters.commodity);
  if (commodity && !isSameList(filters.commodity, DEFAULT_FILTERS.commodity)) params.set("commodity", commodity);
  if (filters.mode && filters.mode !== DEFAULT_FILTERS.mode) params.set("mode", filters.mode);
  const timeValue = filters.timeMode === "live" ? "live" : filters.timeWindow ?? "24h";
  if (timeValue !== "live") params.set("time", timeValue);

  const region = serializeList(filters.region);
  if (region) params.set("region", region);
  const origin = serializeList(filters.origin);
  if (origin) params.set("origin", origin);
  const destination = serializeList(filters.destination);
  if (destination) params.set("destination", destination);
  const hub = serializeList(filters.hub);
  if (hub) params.set("hub", hub);
  const risk = serializeList(filters.riskTags);
  if (risk && !isSameList(filters.riskTags, DEFAULT_FILTERS.riskTags)) params.set("risk", risk);
  if (filters.severity) params.set("severity", filters.severity);
  if (filters.status) params.set("status", filters.status);
  if (selection?.id) {
    params.set("sel_id", selection.id);
    if (selection.name) params.set("sel_name", selection.name);
    if (selection.type) params.set("sel_type", selection.type);
  }

  return params.toString();
};

export function TerminalStoreProvider({
  children,
  basePath = "/terminal",
}: {
  children: React.ReactNode;
  basePath?: string;
}) {
  const [location, setLocation] = useLocation();
  const syncRef = useRef(false);
  const [filters, setFilters] = useState<TerminalFilters>(() => parseFiltersFromUrl(location));
  const [selectedEntity, setSelectedEntity] = useState<SelectedEntity | null>(() => parseSelectionFromUrl(location));

  useEffect(() => {
    if (syncRef.current) {
      syncRef.current = false;
      return;
    }
    setFilters(parseFiltersFromUrl(location));
    setSelectedEntity(parseSelectionFromUrl(location));
  }, [location]);

  useEffect(() => {
    const query = buildQueryFromFilters(filters, selectedEntity);
    const target = query ? `${basePath}?${query}` : basePath;
    if (location !== target) {
      syncRef.current = true;
      setLocation(target);
    }
  }, [basePath, filters, location, selectedEntity, setLocation]);

  const updateFilters = useCallback((patch: Partial<TerminalFilters>) => {
    setFilters((prev) => ({ ...prev, ...patch }));
  }, []);

  const resetFilters = useCallback(() => {
    setFilters({ ...DEFAULT_FILTERS });
    setSelectedEntity(null);
  }, [setSelectedEntity]);

  const value = useMemo(
    () => ({
      filters,
      setFilters,
      updateFilters,
      resetFilters,
      selectedEntity,
      setSelectedEntity,
    }),
    [filters, resetFilters, selectedEntity, updateFilters]
  );

  return <TerminalStoreContext.Provider value={value}>{children}</TerminalStoreContext.Provider>;
}

export function useTerminalStore() {
  const context = useContext(TerminalStoreContext);
  if (!context) {
    throw new Error("useTerminalStore must be used within TerminalStoreProvider");
  }
  return context;
}
