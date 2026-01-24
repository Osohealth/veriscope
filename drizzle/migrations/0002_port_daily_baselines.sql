CREATE TABLE IF NOT EXISTS "port_daily_baselines" (
  "port_id" uuid NOT NULL REFERENCES "ports"("id") ON DELETE CASCADE,
  "date" date NOT NULL,
  "arrivals" bigint NOT NULL DEFAULT 0,
  "departures" bigint NOT NULL DEFAULT 0,
  "unique_vessels" bigint NOT NULL DEFAULT 0,
  "avg_dwell_hours" double precision,
  "open_calls" bigint NOT NULL DEFAULT 0,
  "arrivals_30d_avg" double precision,
  "arrivals_30d_std" double precision,
  "dwell_30d_avg" double precision,
  "dwell_30d_std" double precision,
  "open_calls_30d_avg" double precision,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "port_daily_baselines_port_date_pk" UNIQUE ("port_id", "date")
);

CREATE INDEX IF NOT EXISTS "port_daily_baselines_date_idx"
  ON "port_daily_baselines" ("date");
