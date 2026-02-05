CREATE TABLE IF NOT EXISTS "port_daily_baselines" (
  "id" varchar PRIMARY KEY DEFAULT gen_random_uuid(),
  "port_id" varchar NOT NULL REFERENCES "ports"("id") ON DELETE CASCADE,
  "day" date NOT NULL,
  "arrivals" integer NOT NULL DEFAULT 0,
  "departures" integer NOT NULL DEFAULT 0,
  "unique_vessels" integer NOT NULL DEFAULT 0,
  "avg_dwell_hours" double precision,
  "open_calls" integer NOT NULL DEFAULT 0,
  "arrivals_30d_avg" double precision,
  "arrivals_30d_std" double precision,
  "dwell_30d_avg" double precision,
  "dwell_30d_std" double precision,
  "open_calls_30d_avg" double precision,
  "updated_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "port_daily_baselines_port_day"
  ON "port_daily_baselines" ("port_id", "day");

CREATE INDEX IF NOT EXISTS "port_daily_baselines_day_desc"
  ON "port_daily_baselines" ("day" DESC);

CREATE INDEX IF NOT EXISTS "port_daily_baselines_port_day_desc"
  ON "port_daily_baselines" ("port_id", "day" DESC);
