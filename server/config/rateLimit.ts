const mustPosInt = (name: string, value: string | undefined, fallback: number) => {
  if (value == null || value === "") return fallback;
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0 || !Number.isInteger(parsed)) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
};

export const RATE_LIMIT_WINDOW_SECONDS = 60;

export const getRateLimits = () => ({
  GLOBAL_PER_MIN: mustPosInt("RATE_LIMIT_GLOBAL_PER_MIN", process.env.RATE_LIMIT_GLOBAL_PER_MIN, 300),
  WRITE_PER_MIN: mustPosInt("RATE_LIMIT_WRITE_PER_MIN", process.env.RATE_LIMIT_WRITE_PER_MIN, 60),
  EXPORT_PER_MIN: mustPosInt("RATE_LIMIT_EXPORT_PER_MIN", process.env.RATE_LIMIT_EXPORT_PER_MIN, 10),
});
