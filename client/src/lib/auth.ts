const AUTH_STORAGE_KEYS = ["access_token", "refresh_token", "user", "api_key", "vs_authenticated"] as const;

export const isAuthenticated = () => {
  if (typeof window === "undefined") return false;
  return localStorage.getItem("vs_authenticated") === "true";
};

export const markAuthenticated = (apiKey?: string) => {
  if (typeof window === "undefined") return;
  localStorage.setItem("vs_authenticated", "true");
  if (apiKey && !localStorage.getItem("api_key")) {
    localStorage.setItem("api_key", apiKey);
  }
};

export const ensureDevApiKey = async () => {
  if (!import.meta.env.DEV) return null;
  try {
    const res = await fetch("/api/dev/demo-api-key", { method: "POST" });
    if (!res.ok) {
      const fallback = "vs_demo_key";
      localStorage.setItem("api_key", fallback);
      return fallback;
    }
    const data = await res.json();
    if (data?.api_key) {
      localStorage.setItem("api_key", data.api_key);
      return data.api_key as string;
    }
    const fallback = "vs_demo_key";
    localStorage.setItem("api_key", fallback);
    return fallback;
  } catch {
    const fallback = "vs_demo_key";
    localStorage.setItem("api_key", fallback);
    return fallback;
  }
};

export const hasAccessToken = () => {
  if (typeof window === "undefined") return false;
  return Boolean(localStorage.getItem("access_token"));
};

export const clearAuthStorage = () => {
  if (typeof window === "undefined") return;
  AUTH_STORAGE_KEYS.forEach((key) => localStorage.removeItem(key));
};
