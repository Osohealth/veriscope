import { sql } from "drizzle-orm";
import { db } from "../../server/db";

const timingEnabled = process.env.TEST_TIMING === "1";

const execSchema = async (label: string, text: string) => {
  const start = timingEnabled ? process.hrtime.bigint() : null;
  await db.execute(sql.raw(text));
  if (start) {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
    console.log(`TEST_TIMING: schema ${label} ${elapsedMs.toFixed(1)}ms`);
  }
};

const execSchemaBundle = async (label: string, blocks: string[]) => {
  const start = timingEnabled ? process.hrtime.bigint() : null;
  const body = blocks.join("\n");
  await db.execute(sql.raw(`BEGIN;\n${body}\nCOMMIT;`));
  if (start) {
    const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
    console.log(`TEST_TIMING: schema ${label} ${elapsedMs.toFixed(1)}ms`);
  }
};

const SIGNALS_SCHEMA_SQL = `
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
  "confidence_score" double precision,
  "confidence_band" text,
  "method" text,
  "cluster_id" text,
  "cluster_type" text,
  "cluster_summary" text,
  "explanation" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS "signals_unique"
  ON "signals" ("signal_type", "entity_type", "entity_id", "day");
`;

const ALERTING_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS "alert_subscriptions" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "user_id" uuid NOT NULL,
  "scope" text NOT NULL DEFAULT 'PORT',
  "entity_type" text NOT NULL DEFAULT 'port',
  "entity_id" uuid NOT NULL,
  "severity_min" text NOT NULL DEFAULT 'HIGH',
  "confidence_min" text,
  "channel" text NOT NULL DEFAULT 'WEBHOOK',
  "endpoint" text NOT NULL,
  "secret" text,
  "signature_version" text NOT NULL DEFAULT 'v1',
  "is_enabled" boolean NOT NULL DEFAULT true,
  "last_test_at" timestamptz,
  "last_test_status" text,
  "last_test_error" text,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "secret" text;
ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "signature_version" text NOT NULL DEFAULT 'v1';
ALTER TABLE "alert_subscriptions"
  ADD COLUMN IF NOT EXISTS "min_quality_band" text,
  ADD COLUMN IF NOT EXISTS "min_quality_score" integer;

CREATE TABLE IF NOT EXISTS "alert_dedupe" (
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "cluster_id" text NOT NULL,
  "channel" text NOT NULL,
  "endpoint" text NOT NULL,
  "last_sent_at" timestamptz NOT NULL,
  "ttl_hours" int NOT NULL DEFAULT 24
);
CREATE UNIQUE INDEX IF NOT EXISTS "alert_dedupe_unique"
  ON "alert_dedupe" ("tenant_id", "cluster_id", "channel", "endpoint");

CREATE TABLE IF NOT EXISTS "alert_runs" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "day" date,
  "started_at" timestamptz NOT NULL DEFAULT now(),
  "finished_at" timestamptz,
  "status" text NOT NULL,
  "summary" jsonb,
  "error" jsonb
);

CREATE TABLE IF NOT EXISTS "alert_deliveries" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "run_id" uuid NOT NULL REFERENCES "alert_runs" ("id"),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "user_id" uuid NOT NULL,
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
  "is_test" boolean NOT NULL DEFAULT false,
  "created_at" timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "destination_key" text;
CREATE OR REPLACE FUNCTION compute_destination_key(dest_type text, endpoint text)
RETURNS text AS $$
DECLARE
  normalized text;
  base text;
BEGIN
  IF dest_type IS NULL OR endpoint IS NULL THEN
    RETURN NULL;
  END IF;

  IF upper(dest_type) = 'EMAIL' THEN
    normalized := lower(btrim(endpoint));
  ELSE
    base := lower(btrim(endpoint));
    base := split_part(base, '?', 1);
    base := split_part(base, '#', 1);
    IF base ~ '^https?://[^/]+/$' THEN
      normalized := base;
    ELSE
      normalized := regexp_replace(base, '/+$', '');
    END IF;
  END IF;

  RETURN substring(encode(digest(convert_to(normalized, 'utf8'), 'sha256'), 'hex') from 1 for 16);
END;
$$ LANGUAGE plpgsql IMMUTABLE;
CREATE OR REPLACE FUNCTION set_alert_deliveries_destination_key()
RETURNS trigger AS $$
BEGIN
  IF NEW.destination_key IS NULL THEN
    NEW.destination_key := compute_destination_key(NEW.destination_type, NEW.endpoint);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS alert_deliveries_destination_key_trigger ON alert_deliveries;
CREATE TRIGGER alert_deliveries_destination_key_trigger
BEFORE INSERT ON alert_deliveries
FOR EACH ROW
EXECUTE FUNCTION set_alert_deliveries_destination_key();
UPDATE alert_deliveries
SET destination_key = compute_destination_key(destination_type, endpoint)
WHERE destination_key IS NULL;
ALTER TABLE "alert_deliveries"
  ALTER COLUMN "destination_key" SET NOT NULL;

CREATE TABLE IF NOT EXISTS "alert_dlq" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "delivery_id" uuid NOT NULL REFERENCES "alert_deliveries" ("id") ON DELETE CASCADE,
  "next_attempt_at" timestamptz NOT NULL,
  "attempt_count" int NOT NULL DEFAULT 0,
  "max_attempts" int NOT NULL DEFAULT 10,
  "last_error" text,
  "created_at" timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS "alert_dlq_unique"
  ON "alert_dlq" ("delivery_id");

CREATE TABLE IF NOT EXISTS "alert_delivery_attempts" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "delivery_id" uuid NOT NULL REFERENCES "alert_deliveries" ("id") ON DELETE CASCADE,
  "attempt_no" int NOT NULL,
  "status" text NOT NULL,
  "latency_ms" int,
  "http_status" int,
  "error" text,
  "sent_at" timestamptz,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "alert_endpoint_health" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "window" text NOT NULL,
  "destination_type" text NOT NULL,
  "destination" text NOT NULL,
  "status" text NOT NULL,
  "attempts_total" integer NOT NULL DEFAULT 0,
  "attempts_success" integer NOT NULL DEFAULT 0,
  "success_rate" double precision NOT NULL DEFAULT 1,
  "p50_ms" integer NULL,
  "p95_ms" integer NULL,
  "consecutive_failures" integer NOT NULL DEFAULT 0,
  "last_success_at" timestamptz NULL,
  "last_failure_at" timestamptz NULL,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  UNIQUE ("tenant_id", "window", "destination_type", "destination")
);

CREATE TABLE IF NOT EXISTS "alert_destination_states" (
  "tenant_id" uuid NOT NULL,
  "destination_type" text NOT NULL,
  "destination_key" text NOT NULL,
  "state" text NOT NULL,
  "reason" text NULL,
  "paused_by_user_id" uuid NULL,
  "paused_at" timestamptz NULL,
  "auto_paused_at" timestamptz NULL,
  "resume_ready_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY ("tenant_id", "destination_type", "destination_key")
);

CREATE TABLE IF NOT EXISTS "alert_noise_budgets" (
  "tenant_id" uuid NOT NULL,
  "destination_type" text NOT NULL,
  "window" text NOT NULL,
  "max_deliveries" integer NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, destination_type, "window")
);

ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "skip_reason" text;
ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "is_bundle" boolean NOT NULL DEFAULT true,
  ADD COLUMN IF NOT EXISTS "bundle_size" int NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS "bundle_overflow" int NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS "bundle_payload" jsonb,
  ADD COLUMN IF NOT EXISTS "decision" jsonb;

CREATE TABLE IF NOT EXISTS "alert_noise_budget_breaches" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "destination_type" text NOT NULL,
  "window" text NOT NULL,
  "bucket_minute" timestamptz NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "alert_noise_budget_breaches_unique"
  ON "alert_noise_budget_breaches" ("tenant_id", "destination_type", "window", "bucket_minute");

CREATE TABLE IF NOT EXISTS "alert_quality_gate_breaches" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "subscription_id" uuid NOT NULL,
  "day" date NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS "alert_quality_gate_breaches_unique"
  ON "alert_quality_gate_breaches" ("tenant_id", "subscription_id", "day");

ALTER TABLE "alert_deliveries"
  ADD COLUMN IF NOT EXISTS "quality_score" integer,
  ADD COLUMN IF NOT EXISTS "quality_band" text,
  ADD COLUMN IF NOT EXISTS "quality_reasons" jsonb,
  ADD COLUMN IF NOT EXISTS "quality_version" text;
`;

const CORE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS "api_keys" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "user_id" uuid NOT NULL,
  "key_hash" text NOT NULL UNIQUE,
  "role" text NOT NULL DEFAULT 'OWNER',
  "name" text,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "revoked_at" timestamptz
);
ALTER TABLE "api_keys"
ADD COLUMN IF NOT EXISTS "role" text NOT NULL DEFAULT 'OWNER';

CREATE TABLE IF NOT EXISTS "tenant_users" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "user_id" uuid NOT NULL,
  "email" text NOT NULL,
  "display_name" text,
  "role" text NOT NULL DEFAULT 'VIEWER',
  "status" text NOT NULL DEFAULT 'INVITED',
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "created_by" uuid,
  "revoked_at" timestamptz,
  "revoked_by" uuid
);

CREATE TABLE IF NOT EXISTS "tenant_invites" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001',
  "email" text NOT NULL,
  "role" text NOT NULL DEFAULT 'VIEWER',
  "token_hash" text NOT NULL UNIQUE,
  "expires_at" timestamptz NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "created_by" uuid NOT NULL,
  "accepted_at" timestamptz,
  "accepted_by_user_id" uuid,
  "revoked_at" timestamptz,
  "revoked_by" uuid
);

CREATE TABLE IF NOT EXISTS "audit_events" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "actor_type" text NOT NULL,
  "actor_user_id" uuid,
  "actor_api_key_id" uuid,
  "actor_label" text,
  "action" text NOT NULL,
  "resource_type" text NOT NULL,
  "resource_id" text,
  "severity" text NOT NULL,
  "status" text NOT NULL,
  "message" text NOT NULL,
  "metadata" jsonb NOT NULL DEFAULT '{}'::jsonb,
  "ip" text,
  "user_agent" text,
  "request_id" text,
  "created_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "tenant_settings" (
  "tenant_id" uuid PRIMARY KEY,
  "audit_retention_days" integer NOT NULL DEFAULT 90,
  "allowed_email_domains" text[] NOT NULL DEFAULT ARRAY[]::text[],
  "allowed_webhook_hosts" text[] NOT NULL DEFAULT ARRAY[]::text[],
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "user_contact_methods" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "user_id" uuid NOT NULL,
  "type" text NOT NULL,
  "value" text NOT NULL,
  "label" text NULL,
  "is_primary" boolean NOT NULL DEFAULT false,
  "is_verified" boolean NOT NULL DEFAULT true,
  "is_active" boolean NOT NULL DEFAULT true,
  "created_at" timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS "user_contact_methods_unique"
  ON "user_contact_methods" ("tenant_id", "user_id", "type", "value");
CREATE UNIQUE INDEX IF NOT EXISTS "user_contact_methods_primary"
  ON "user_contact_methods" ("tenant_id", "user_id", "type")
  WHERE "is_primary" = true;
CREATE INDEX IF NOT EXISTS "user_contact_methods_tenant_user_type_active"
  ON "user_contact_methods" ("tenant_id", "user_id", "type", "is_active");
CREATE INDEX IF NOT EXISTS "user_contact_methods_tenant_user_primary_created"
  ON "user_contact_methods" ("tenant_id", "user_id", "is_primary" DESC, "created_at" DESC);

CREATE TABLE IF NOT EXISTS "audit_exports" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "requested_by_user_id" uuid NOT NULL,
  "format" text NOT NULL,
  "filters" jsonb NOT NULL,
  "row_count" integer NOT NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "expires_at" timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS "rate_limit_buckets" (
  "tenant_id" uuid NOT NULL,
  "key_hash" text NOT NULL,
  "scope" text NOT NULL,
  "window_start" timestamptz NOT NULL,
  "count" integer NOT NULL DEFAULT 0,
  "updated_at" timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, key_hash, scope, window_start)
);
`;

const INCIDENTS_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS "incidents" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "type" text NOT NULL,
  "destination_key" text NULL,
  "status" text NOT NULL DEFAULT 'OPEN',
  "severity" text NOT NULL,
  "title" text NOT NULL,
  "summary" text NOT NULL,
  "opened_at" timestamptz NOT NULL DEFAULT now(),
  "acked_at" timestamptz NULL,
  "resolved_at" timestamptz NULL,
  "opened_by_actor_type" text NOT NULL,
  "opened_by_actor_id" uuid NULL,
  "acked_by_actor_type" text NULL,
  "acked_by_actor_id" uuid NULL,
  "resolved_by_actor_type" text NULL,
  "resolved_by_actor_id" uuid NULL
);
CREATE INDEX IF NOT EXISTS "incidents_tenant_status_opened"
  ON "incidents" ("tenant_id", "status", "opened_at" DESC);
CREATE INDEX IF NOT EXISTS "incidents_tenant_destination"
  ON "incidents" ("tenant_id", "destination_key", "opened_at" DESC);
CREATE UNIQUE INDEX IF NOT EXISTS "incidents_open_unique"
  ON "incidents" ("tenant_id", "type", coalesce("destination_key", ''))
  WHERE "status" = 'OPEN';
`;

const ESCALATIONS_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS "incident_escalation_policies" (
  "id" uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  "tenant_id" uuid NOT NULL,
  "incident_type" text NOT NULL,
  "severity_min" text NOT NULL,
  "level" integer NOT NULL,
  "after_minutes" integer NOT NULL,
  "target_type" text NOT NULL,
  "target_ref" text NOT NULL,
  "target_name" text NULL,
  "enabled" boolean NOT NULL DEFAULT true,
  "last_validated_at" timestamptz NULL,
  "last_routing_health" jsonb,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);
DROP INDEX IF EXISTS "incident_escalation_policies_unique";
CREATE UNIQUE INDEX IF NOT EXISTS "incident_escalation_policies_unique"
  ON "incident_escalation_policies" ("tenant_id", "incident_type", "severity_min", "level", "target_type", "target_ref");
CREATE INDEX IF NOT EXISTS "incident_escalation_policies_lookup"
  ON "incident_escalation_policies" ("tenant_id", "incident_type", "enabled", "severity_min", "after_minutes");
CREATE INDEX IF NOT EXISTS "incident_escalation_policies_enabled_idx"
  ON "incident_escalation_policies" ("tenant_id", "enabled", "incident_type", "level");
CREATE TABLE IF NOT EXISTS "incident_escalations" (
  "incident_id" uuid PRIMARY KEY,
  "tenant_id" uuid NOT NULL,
  "current_level" integer NOT NULL DEFAULT 0,
  "last_escalated_at" timestamptz NULL,
  "created_at" timestamptz NOT NULL DEFAULT now(),
  "updated_at" timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "incident_escalations_tenant"
  ON "incident_escalations" ("tenant_id");
`;

export async function ensureSignalsSchema() {
  if (process.env.TEST_SCHEMA_PROFILE === "escalations") {
    throw new Error("Signals schema must not be initialized in escalations profile");
  }
  await execSchema("signals", SIGNALS_SCHEMA_SQL);
}

export async function ensureAlertingSchema() {
  await execSchema("alerting", ALERTING_SCHEMA_SQL);
}

export async function ensureAlertingOnlySchema() {
  await execSchemaBundle("alerts-only", [
    CORE_SCHEMA_SQL,
    SIGNALS_SCHEMA_SQL,
    ALERTING_SCHEMA_SQL,
    INCIDENTS_SCHEMA_SQL,
  ]);
}

export async function ensureCoreSchema() {
  await execSchema("core", CORE_SCHEMA_SQL);
}

export async function ensureIncidentsSchema() {
  await execSchema("incidents", INCIDENTS_SCHEMA_SQL);
}

export async function ensureEscalationsSchema() {
  await execSchema("escalations", ESCALATIONS_SCHEMA_SQL);
}

export async function ensureTestSchema() {
  if (process.env.TEST_SCHEMA_PROFILE === "escalations") {
    throw new Error("Full schema must not be initialized in escalations profile");
  }
  await execSchemaBundle("full", [
    SIGNALS_SCHEMA_SQL,
    ALERTING_SCHEMA_SQL,
    CORE_SCHEMA_SQL,
    INCIDENTS_SCHEMA_SQL,
    ESCALATIONS_SCHEMA_SQL,
  ]);
}

export async function ensureEscalationsOnlySchema() {
  await execSchemaBundle("escalations-only", [
    CORE_SCHEMA_SQL,
    ALERTING_SCHEMA_SQL,
    INCIDENTS_SCHEMA_SQL,
    ESCALATIONS_SCHEMA_SQL,
  ]);
}
