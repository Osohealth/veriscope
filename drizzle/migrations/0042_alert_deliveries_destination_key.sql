ALTER TABLE alert_deliveries
  ADD COLUMN IF NOT EXISTS destination_key text;

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

ALTER TABLE alert_deliveries
  ALTER COLUMN destination_key SET NOT NULL;

CREATE INDEX IF NOT EXISTS alert_deliveries_tenant_destination_key_created
  ON alert_deliveries (tenant_id, destination_key, created_at DESC, id DESC);
