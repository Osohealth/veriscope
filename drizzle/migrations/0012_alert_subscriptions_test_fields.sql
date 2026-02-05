ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "last_test_at" timestamptz;
ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "last_test_status" text;
ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "last_test_error" text;
