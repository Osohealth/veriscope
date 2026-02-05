ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "user_id" uuid;

UPDATE "alert_deliveries" d
SET "user_id" = s."user_id"
FROM "alert_subscriptions" s
WHERE d."subscription_id" = s."id"
  AND d."user_id" IS NULL;

CREATE INDEX IF NOT EXISTS "alert_deliveries_user_created_id"
  ON "alert_deliveries" ("user_id", "created_at" DESC, "id" DESC);
