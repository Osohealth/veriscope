export const METRIC_LABELS: Record<string, string> = {
  arrivals: "Arrivals",
  departures: "Departures",
  unique_vessels: "Unique vessels",
  avg_dwell_hours: "Avg dwell (hours)",
  open_calls: "Open calls",
};

export const METRIC_UNITS: Record<string, string> = {
  avg_dwell_hours: "h",
};

export const formatMetricValue = (metric: string, value: number): string => {
  const unit = METRIC_UNITS[metric];
  if (!unit) {
    return String(value);
  }
  return `${value}${unit}`;
};
