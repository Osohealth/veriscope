CREATE TABLE IF NOT EXISTS "alert_delivery_attempts" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "delivery_id" uuid NOT NULL,
  "attempt_no" integer NOT NULL,
  "status" text NOT NULL,
  "latency_ms" integer,
  "http_status" integer,
  "error" text,
  "sent_at" timestamptz,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "alert_delivery_attempts_delivery_id" ON "alert_delivery_attempts" ("delivery_id");
CREATE INDEX IF NOT EXISTS "alert_delivery_attempts_created_at" ON "alert_delivery_attempts" ("created_at");
