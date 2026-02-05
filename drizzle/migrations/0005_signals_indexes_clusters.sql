ALTER TABLE "signals"
  ADD COLUMN IF NOT EXISTS "cluster_key" text,
  ADD COLUMN IF NOT EXISTS "cluster_severity" text;

CREATE INDEX IF NOT EXISTS "signals_day" ON "signals" ("day");
CREATE INDEX IF NOT EXISTS "signals_entity_day" ON "signals" ("entity_id", "day");
