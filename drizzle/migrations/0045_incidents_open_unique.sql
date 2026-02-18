CREATE UNIQUE INDEX IF NOT EXISTS incidents_open_unique
  ON incidents (tenant_id, type, coalesce(destination_key, ''))
  WHERE status = 'OPEN';
