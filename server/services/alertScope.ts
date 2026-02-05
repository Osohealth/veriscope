export const GLOBAL_SCOPE_ENTITY_ID = "00000000-0000-0000-0000-000000000000";

export type AlertScope = "PORT" | "GLOBAL";

export const normalizeScope = (value?: string | null): AlertScope => {
  if (!value) return "PORT";
  const upper = value.toUpperCase();
  return upper === "GLOBAL" ? "GLOBAL" : "PORT";
};
