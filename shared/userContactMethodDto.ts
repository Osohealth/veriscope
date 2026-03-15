export type ContactMethodType = "EMAIL" | "WEBHOOK";

export type UserContactMethodDTO = {
  id: string;
  tenant_id: string;
  user_id: string;
  type: ContactMethodType;
  value: string;
  label?: string | null;
  is_primary: boolean;
  is_verified: boolean;
  is_active: boolean;
  created_at: string;
};

export type CreateUserContactMethodInput = {
  type: ContactMethodType;
  value: string;
  label?: string | null;
  is_primary?: boolean;
};

export type UpdateUserContactMethodInput = {
  label?: string | null;
  is_primary?: boolean;
  is_active?: boolean;
};
