ALTER TABLE "alert_subscriptions" ADD COLUMN IF NOT EXISTS "secret" text;
ALTER TABLE "alert_subscriptions" ADD COLUMN IF NOT EXISTS "signature_version" text NOT NULL DEFAULT 'v1';
