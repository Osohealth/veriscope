CREATE TABLE IF NOT EXISTS "alert_runs" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "day" date,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "finished_at" timestamptz,
  "status" text NOT NULL,
  "summary" jsonb,
  "error" jsonb
);

CREATE INDEX IF NOT EXISTS "idx_alert_runs_started_at"
  ON "alert_runs" ("started_at");

CREATE TABLE IF NOT EXISTS "alert_dedupe" (
  "cluster_id" text NOT NULL,
  "channel" text NOT NULL,
  "endpoint" text NOT NULL,
  "last_sent_at" timestamptz NOT NULL,
  "ttl_hours" int NOT NULL DEFAULT 24
);

CREATE UNIQUE INDEX IF NOT EXISTS "alert_dedupe_unique"
  ON "alert_dedupe" ("cluster_id", "channel", "endpoint");
