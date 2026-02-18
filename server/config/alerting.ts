const parseIntEnv = (key: string, fallback: number) => {
  const raw = process.env[key];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${key} value: ${raw}`);
  }
  return parsed;
};

const parseClampedInt = (key: string, fallback: number, min: number, max: number) => {
  const value = parseIntEnv(key, fallback);
  if (value < min) return min;
  if (value > max) return max;
  return value;
};

export const ALERT_RATE_LIMIT_PER_ENDPOINT = parseIntEnv("ALERT_RATE_LIMIT_PER_ENDPOINT", 50);
export const ALERT_DEDUPE_TTL_HOURS = parseIntEnv("ALERT_DEDUPE_TTL_HOURS", 24);
export const WEBHOOK_TIMEOUT_MS = parseIntEnv("WEBHOOK_TIMEOUT_MS", 5000);
export const WEBHOOK_RETRY_ATTEMPTS = parseIntEnv("WEBHOOK_RETRY_ATTEMPTS", 3);
export const DLQ_MAX_ATTEMPTS = parseIntEnv("DLQ_MAX_ATTEMPTS", 10);
export const ALERT_BUNDLE_TOP_N = parseClampedInt("ALERT_BUNDLE_TOP_N", 3, 1, 20);
export const ALERT_BUNDLE_MAX_BYTES = parseClampedInt("ALERT_BUNDLE_MAX_BYTES", 32000, 1000, 200000);
export const INCIDENT_AUTOMATION_ENABLED = process.env.INCIDENT_AUTOMATION_ENABLED === "true";
export const INCIDENT_AUTOMATION_INTERVAL_MS = parseIntEnv("INCIDENT_AUTOMATION_INTERVAL_MS", 300000);
