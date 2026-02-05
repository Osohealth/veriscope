const parseIntEnv = (key: string, fallback: number) => {
  const raw = process.env[key];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`Invalid ${key} value: ${raw}`);
  }
  return parsed;
};

export const ALERT_RATE_LIMIT_PER_ENDPOINT = parseIntEnv("ALERT_RATE_LIMIT_PER_ENDPOINT", 50);
export const ALERT_DEDUPE_TTL_HOURS = parseIntEnv("ALERT_DEDUPE_TTL_HOURS", 24);
export const WEBHOOK_TIMEOUT_MS = parseIntEnv("WEBHOOK_TIMEOUT_MS", 5000);
export const WEBHOOK_RETRY_ATTEMPTS = parseIntEnv("WEBHOOK_RETRY_ATTEMPTS", 3);
export const DLQ_MAX_ATTEMPTS = parseIntEnv("DLQ_MAX_ATTEMPTS", 10);
