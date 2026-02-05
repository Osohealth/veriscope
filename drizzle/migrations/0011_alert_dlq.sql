CREATE TABLE IF NOT EXISTS "alert_dlq" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "delivery_id" uuid NOT NULL REFERENCES "alert_deliveries" ("id") ON DELETE CASCADE,
  "next_attempt_at" timestamptz NOT NULL,
  "attempt_count" int NOT NULL DEFAULT 0,
  "max_attempts" int NOT NULL DEFAULT 10,
  "last_error" text,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "alert_dlq_unique"
  ON "alert_dlq" ("delivery_id");

CREATE INDEX IF NOT EXISTS "alert_dlq_next_attempt"
  ON "alert_dlq" ("next_attempt_at");
