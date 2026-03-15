import type { TerminalFilters, SelectedEntity } from "@/hooks/use-terminal-store";
import { buildQueryFromFilters } from "@/hooks/use-terminal-store";

export const buildSelectionContextLink = (
  selection: SelectedEntity | null,
  filters: TerminalFilters,
  fallback = "/terminal"
) => {
  const base =
    selection?.type === "port"
      ? "/congestion"
      : selection?.type === "lane" || selection?.type === "corridor"
        ? "/flows"
        : fallback;
  const query = buildQueryFromFilters(filters, selection ?? null);
  return query ? `${base}?${query}` : base;
};
