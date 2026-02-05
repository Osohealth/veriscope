DROP INDEX IF EXISTS "signals_day_desc";

CREATE INDEX IF NOT EXISTS "signals_day_cluster"
  ON "signals" ("day", "cluster_id");
