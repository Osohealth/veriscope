export const fmtMs = (value?: number | null) => {
  if (value == null || Number.isNaN(value) || value < 0) return "--";
  const seconds = Math.floor(value / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const minutesRemainder = minutes % 60;
  return `${hours}h ${minutesRemainder}m`;
};
