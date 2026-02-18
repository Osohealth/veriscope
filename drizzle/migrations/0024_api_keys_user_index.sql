CREATE INDEX IF NOT EXISTS "api_keys_tenant_user_created"
  ON "api_keys" ("tenant_id", "user_id", "created_at" DESC);
