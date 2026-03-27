// API key for machine-to-machine flows (separate from user JWT auth).
// Key is managed in-memory by the calling component — never persisted to localStorage.
export const getApiKey = (): string | null => null;

export const apiFetchJson = async (url: string, options?: RequestInit) => {
  const headers = new Headers(options?.headers ?? {});
  // Only attach API key header if explicitly set (machine API key flow)
  const apiKey = getApiKey();
  if (apiKey && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${apiKey}`);
  }
  const res = await fetch(url, { ...options, headers, credentials: "include" });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} ${res.statusText}: ${text.slice(0, 300)}`);
  }
  if (!text.trim()) return null;
  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`Non-JSON response from ${url}: ${text.slice(0, 300)}`);
  }
};
