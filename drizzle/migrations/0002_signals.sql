CREATE TABLE IF NOT EXISTS "signals" (
  "id" varchar PRIMARY KEY DEFAULT gen_random_uuid(),
  "signal_type" text NOT NULL,
  "entity_type" text NOT NULL,
  "entity_id" varchar NOT NULL,
  "day" date NOT NULL,
  "severity" text NOT NULL,
  "value" double precision NOT NULL,
  "baseline" double precision,
  "stddev" double precision,
  "zscore" double precision,
  "delta_pct" double precision,
  "explanation" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "signals_unique"
  ON "signals" ("signal_type", "entity_type", "entity_id", "day");

CREATE INDEX IF NOT EXISTS "signals_day_desc"
  ON "signals" ("day" DESC);

CREATE INDEX IF NOT EXISTS "signals_severity"
  ON "signals" ("severity");

CREATE INDEX IF NOT EXISTS "signals_signal_type"
  ON "signals" ("signal_type");
