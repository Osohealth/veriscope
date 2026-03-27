// Legacy keys — only kept to purge any old data from users' browsers
const AUTH_STORAGE_KEYS = ["access_token", "refresh_token", "user", "api_key", "vs_authenticated"] as const;

// Dev API key kept in-memory only — never persisted to localStorage
let _devApiKey: string | null = null;

// isAuthenticated is intentionally permissive here; real auth is enforced
// server-side via httpOnly cookies. Components that need the actual user
// object should use the `useCurrentUser()` hook which calls GET /api/auth/me.
export const isAuthenticated = () => true;

// markAuthenticated is a no-op — tokens live in httpOnly cookies set by the server.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const markAuthenticated = (_apiKey?: string) => { };

export const ensureDevApiKey = async () => {
  if (!import.meta.env.DEV) return null;
  if (_devApiKey) return _devApiKey;
  try {
    const res = await fetch("/api/dev/demo-api-key", { method: "POST" });
    if (!res.ok) {
      _devApiKey = "vs_demo_key";
      return _devApiKey;
    }
    const data = await res.json();
    _devApiKey = (data?.api_key as string | undefined) ?? "vs_demo_key";
    return _devApiKey;
  } catch {
    _devApiKey = "vs_demo_key";
    return _devApiKey;
  }
};

// hasAccessToken is always false — access tokens are httpOnly cookies, not accessible to JS.
export const hasAccessToken = () => false;

// Clears any legacy auth data that may have been stored in older app versions.
export const clearAuthStorage = () => {
  if (typeof window === "undefined") return;
  AUTH_STORAGE_KEYS.forEach((key) => localStorage.removeItem(key));
};
