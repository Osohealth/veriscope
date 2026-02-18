ALTER TABLE audit_events
  ALTER COLUMN resource_id TYPE text USING resource_id::text;
