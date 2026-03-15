export const normalizeDomain = (value: string) => value.trim().toLowerCase().replace(/\.$/, "");

export const normalizeHost = (value: string) => {
  const trimmed = value.trim().toLowerCase().replace(/\.$/, "");
  const lastColon = trimmed.lastIndexOf(":");
  if (lastColon > -1 && trimmed.indexOf(":") === lastColon) {
    const portPart = trimmed.slice(lastColon + 1);
    if (/^\d+$/.test(portPart)) {
      return trimmed.slice(0, lastColon);
    }
  }
  return trimmed;
};

export const allowlistMatches = (value: string, entries: string[]) => {
  const normalizedValue = normalizeDomain(value);
  return entries.some((entry) => {
    const normalizedEntry = normalizeDomain(entry);
    if (!normalizedEntry) return false;
    if (normalizedEntry.startsWith("*.") || normalizedEntry.startsWith(".")) {
      const suffix = normalizedEntry.replace(/^\*\./, "").replace(/^\./, "");
      return normalizedValue === suffix || normalizedValue.endsWith(`.${suffix}`);
    }
    return normalizedValue === normalizedEntry;
  });
};

export const allowlistHostMatches = (host: string, entries: string[]) => {
  const normalizedHost = normalizeHost(host);
  return entries.some((entry) => {
    const normalizedEntry = normalizeHost(entry);
    if (!normalizedEntry) return false;
    if (normalizedEntry.startsWith("*.") || normalizedEntry.startsWith(".")) {
      const suffix = normalizedEntry.replace(/^\*\./, "").replace(/^\./, "");
      return normalizedHost === suffix || normalizedHost.endsWith(`.${suffix}`);
    }
    return normalizedHost === normalizedEntry;
  });
};
