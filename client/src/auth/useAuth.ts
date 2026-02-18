import { useEffect, useState } from "react";
import { apiFetchJson } from "@/lib/apiFetch";

export type AuthRole = "OWNER" | "OPERATOR" | "VIEWER";

type AuthInfo = {
  tenant_id: string;
  user_id: string;
  role: AuthRole;
};

const normalizeRole = (value?: string | null): AuthRole => {
  const upper = (value ?? "VIEWER").toUpperCase();
  if (upper === "OWNER" || upper === "OPERATOR" || upper === "VIEWER") return upper;
  return "VIEWER";
};

export const useAuth = () => {
  const [auth, setAuth] = useState<AuthInfo | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let active = true;
    const load = async () => {
      try {
        const payload = await apiFetchJson("/v1/me");
        if (!active) return;
        if (payload?.user_id && payload?.tenant_id) {
          setAuth({
            tenant_id: payload.tenant_id,
            user_id: payload.user_id,
            role: normalizeRole(payload.role),
          });
        } else {
          setAuth(null);
        }
      } catch {
        if (active) setAuth(null);
      } finally {
        if (active) setLoading(false);
      }
    };
    load();
    return () => {
      active = false;
    };
  }, []);

  return {
    auth,
    role: auth?.role ?? "VIEWER",
    loading,
  };
};
