export type Role = "OWNER" | "OPERATOR" | "VIEWER";

export const ROLE_RANK: Record<Role, number> = {
  VIEWER: 1,
  OPERATOR: 2,
  OWNER: 3,
};

export type AuthContext = {
  tenantId: string;
  userId: string;
  role: Role;
  apiKeyId?: string;
};
