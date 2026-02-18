import type { AuthContext, Role } from "./authTypes";
import { ROLE_RANK } from "./authTypes";

export function requireRole(ctx: AuthContext | undefined, minRole: Role): void {
  if (!ctx?.role) {
    const err: any = new Error("Unauthorized");
    err.status = 401;
    throw err;
  }
  if (ROLE_RANK[ctx.role] < ROLE_RANK[minRole]) {
    const err: any = new Error("Forbidden");
    err.status = 403;
    err.code = "FORBIDDEN";
    err.detail = `Requires role >= ${minRole}`;
    throw err;
  }
}
