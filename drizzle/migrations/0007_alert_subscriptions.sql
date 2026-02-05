CREATE TABLE IF NOT EXISTS "alert_subscriptions" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "user_id" uuid NOT NULL,
  "entity_type" text NOT NULL DEFAULT 'port',
  "entity_id" uuid NOT NULL,
  "severity_min" text NOT NULL DEFAULT 'HIGH',
  "confidence_min" text,
  "channel" text NOT NULL DEFAULT 'WEBHOOK',
  "endpoint" text NOT NULL,
  "is_enabled" boolean NOT NULL DEFAULT true,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "idx_alert_subs_user_enabled"
  ON "alert_subscriptions" ("user_id", "is_enabled");

CREATE INDEX IF NOT EXISTS "idx_alert_subs_entity"
  ON "alert_subscriptions" ("entity_type", "entity_id");

CREATE UNIQUE INDEX IF NOT EXISTS "alert_subscriptions_unique"
  ON "alert_subscriptions" ("user_id", "channel", "endpoint", "entity_id");
