-- B2: enforce NOT NULL on alert_subscriptions.user_id (delete orphaned rows first)
DELETE FROM "alert_subscriptions" WHERE "user_id" IS NULL;
ALTER TABLE "alert_subscriptions" ALTER COLUMN "user_id" SET NOT NULL;

-- C3: add optional expires_at to api_keys (nullable — no breaking change for existing keys)
ALTER TABLE "api_keys" ADD COLUMN IF NOT EXISTS "expires_at" timestamptz;
