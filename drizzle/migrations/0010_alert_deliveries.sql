CREATE TABLE IF NOT EXISTS "alert_deliveries" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "run_id" uuid NOT NULL REFERENCES "alert_runs" ("id"),
  "subscription_id" uuid NOT NULL REFERENCES "alert_subscriptions" ("id"),
  "cluster_id" text NOT NULL,
  "entity_type" text NOT NULL,
  "entity_id" text NOT NULL,
  "day" date NOT NULL,
  "destination_type" text NOT NULL,
  "endpoint" text NOT NULL,
  "status" text NOT NULL,
  "attempts" int NOT NULL DEFAULT 0,
  "last_http_status" int,
  "latency_ms" int,
  "error" text,
  "sent_at" timestamptz,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS "alert_deliveries_run_id"
  ON "alert_deliveries" ("run_id");

CREATE INDEX IF NOT EXISTS "alert_deliveries_sub_time"
  ON "alert_deliveries" ("subscription_id", "created_at" DESC);

CREATE INDEX IF NOT EXISTS "alert_deliveries_day_entity"
  ON "alert_deliveries" ("day", "entity_id");

CREATE INDEX IF NOT EXISTS "alert_deliveries_cluster_id"
  ON "alert_deliveries" ("cluster_id");
