CREATE TABLE IF NOT EXISTS "signals" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "signal_type" text NOT NULL,
  "entity_type" text NOT NULL,
  "entity_id" text NOT NULL,
  "severity" text NOT NULL,
  "value" double precision NOT NULL,
  "baseline" double precision NOT NULL,
  "delta_pct" double precision NOT NULL,
  "explanation" text NOT NULL,
  "signal_date" date NOT NULL DEFAULT current_date,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "signals_unique_per_day" UNIQUE ("signal_type", "entity_type", "entity_id", "signal_date")
);

CREATE INDEX IF NOT EXISTS "signals_entity_idx"
  ON "signals" ("entity_type", "entity_id");

CREATE INDEX IF NOT EXISTS "signals_created_at_idx"
  ON "signals" ("created_at");
