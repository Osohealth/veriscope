CREATE TABLE IF NOT EXISTS "vessel_port_state" (
  "vessel_id" uuid PRIMARY KEY REFERENCES "vessels"("id") ON DELETE CASCADE,
  "in_port" boolean NOT NULL DEFAULT false,
  "current_port_id" uuid REFERENCES "ports"("id"),
  "current_port_call_id" uuid REFERENCES "port_calls"("id"),
  "last_position_time_utc" timestamptz,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT "vessel_port_state_in_port_check"
    CHECK (
      ("in_port" = true AND "current_port_id" IS NOT NULL)
      OR ("in_port" = false AND "current_port_id" IS NULL AND "current_port_call_id" IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS "vessel_port_state_current_port_idx"
  ON "vessel_port_state" ("current_port_id");
CREATE INDEX IF NOT EXISTS "vessel_port_state_in_port_idx"
  ON "vessel_port_state" ("in_port");
