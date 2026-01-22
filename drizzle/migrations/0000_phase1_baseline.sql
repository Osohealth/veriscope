CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS "users" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "email" text NOT NULL UNIQUE,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "ports" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "name" text NOT NULL,
  "country_code" text NOT NULL,
  "lat" double precision NOT NULL,
  "lon" double precision NOT NULL,
  "geofence_radius_km" double precision NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "vessels" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "name" text NOT NULL,
  "mmsi" text,
  "imo" text,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "ais_positions" (
  "id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  "vessel_id" uuid NOT NULL REFERENCES "vessels"("id") ON DELETE CASCADE,
  "timestamp_utc" timestamptz NOT NULL,
  "lat" double precision NOT NULL,
  "lon" double precision NOT NULL,
  "speed" double precision,
  "course" double precision,
  "heading" double precision,
  "nav_status" text,
  "source" text NOT NULL
);

CREATE TABLE IF NOT EXISTS "port_calls" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "vessel_id" uuid NOT NULL REFERENCES "vessels"("id") ON DELETE CASCADE,
  "port_id" uuid NOT NULL REFERENCES "ports"("id") ON DELETE CASCADE,
  "arrival_time_utc" timestamptz NOT NULL,
  "departure_time_utc" timestamptz,
  "first_position_id" bigint REFERENCES "ais_positions"("id"),
  "last_position_id" bigint REFERENCES "ais_positions"("id")
);

CREATE TABLE IF NOT EXISTS "sessions" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "user_id" uuid NOT NULL REFERENCES "users"("id") ON DELETE CASCADE,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS "ais_positions_timestamp_utc_desc" ON "ais_positions" ("timestamp_utc" DESC);
CREATE INDEX IF NOT EXISTS "ais_positions_vessel_timestamp_desc" ON "ais_positions" ("vessel_id", "timestamp_utc" DESC);
CREATE INDEX IF NOT EXISTS "port_calls_port_arrival_desc" ON "port_calls" ("port_id", "arrival_time_utc" DESC);
CREATE INDEX IF NOT EXISTS "port_calls_open_calls" ON "port_calls" ("port_id", "arrival_time_utc" DESC)
  WHERE "departure_time_utc" IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS "port_calls_open_unique" ON "port_calls" ("vessel_id", "port_id")
  WHERE "departure_time_utc" IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS "vessels_mmsi_unique" ON "vessels" ("mmsi");
CREATE UNIQUE INDEX IF NOT EXISTS "vessels_imo_unique" ON "vessels" ("imo");
CREATE UNIQUE INDEX IF NOT EXISTS "ports_name_country_unique" ON "ports" ("name", "country_code");
