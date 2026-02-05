-- Add tenant-aware indexes for alert subscriptions pagination and api_keys lookups

CREATE UNIQUE INDEX IF NOT EXISTS "api_keys_key_hash_unique"
  ON "api_keys" ("key_hash");

CREATE INDEX IF NOT EXISTS "api_keys_tenant_key_hash"
  ON "api_keys" ("tenant_id", "key_hash");

CREATE INDEX IF NOT EXISTS "alert_subscriptions_tenant_user_created_id"
  ON "alert_subscriptions" ("tenant_id", "user_id", "created_at" DESC, "id" DESC);
