import type { TerminalFilters, SelectedEntity } from "@/hooks/use-terminal-store";
import { buildQueryFromFilters } from "@/hooks/use-terminal-store";

export type AlertIncident = {
  id: string;
  type: string;
  destination_key?: string | null;
  status?: string;
  severity?: string;
  title: string;
  summary: string;
  opened_at?: string;
  acked_at?: string | null;
  resolved_at?: string | null;
};

export const extractHubToken = (text?: string | null) => {
  if (!text) return null;
  const match = text.match(/[A-Z]{5}/);
  return match ? match[0] : null;
};

export const buildAlertContextBase = (incident: AlertIncident) => {
  const type = incident.type?.toLowerCase() ?? "";
  const destinationKey = incident.destination_key?.toLowerCase() ?? "";

  if (
    destinationKey.includes("congestion") ||
    destinationKey.includes("port") ||
    type.includes("congestion") ||
    type.includes("port")
  ) {
    return "/congestion";
  }
  if (
    destinationKey.includes("flow") ||
    destinationKey.includes("lane") ||
    destinationKey.includes("trade") ||
    type.includes("flow") ||
    type.includes("lane") ||
    type.includes("trade")
  ) {
    return "/flows";
  }
  return "/terminal";
};

export const buildAlertSelection = (incident: AlertIncident): SelectedEntity => {
  const hubToken =
    extractHubToken(incident.destination_key ?? "") ??
    extractHubToken(incident.title) ??
    extractHubToken(incident.summary);
  if (hubToken) {
    return { id: hubToken, name: hubToken, type: "port" };
  }
  return { id: incident.id, name: incident.title, type: "unknown" };
};

export const buildAlertContextLink = (
  incident: AlertIncident,
  filters?: TerminalFilters,
  selection?: SelectedEntity
) => {
  const base = buildAlertContextBase(incident);
  if (filters) {
    const query = buildQueryFromFilters(filters, selection ?? null);
    return query ? `${base}?${query}` : base;
  }
  return base;
};
