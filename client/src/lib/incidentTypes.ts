export type IncidentTypeFilter = "ALL" | "SLA" | "ENDPOINT";

export const mapIncidentTypeToApi = (value: IncidentTypeFilter) => {
  if (value === "SLA") return "SLA_AT_RISK";
  if (value === "ENDPOINT") return "ENDPOINT_DOWN";
  return undefined;
};

export const parseIncidentTypeFilter = (value?: string | null): IncidentTypeFilter => {
  if (!value) return "ALL";
  const upper = value.toUpperCase();
  if (upper === "SLA" || upper === "SLA_AT_RISK") return "SLA";
  if (upper === "ENDPOINT" || upper === "ENDPOINT_DOWN") return "ENDPOINT";
  return "ALL";
};

export const getIncidentTypeLabel = (value: string) => {
  const upper = value.toUpperCase();
  if (upper.includes("SLA")) return "SLA";
  if (upper.includes("ENDPOINT")) return "ENDPOINT";
  return upper;
};
