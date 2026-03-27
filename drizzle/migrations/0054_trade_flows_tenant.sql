ALTER TABLE trade_flows ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(255) NOT NULL DEFAULT '00000000-0000-0000-0000-000000000001';
CREATE INDEX IF NOT EXISTS trade_flows_tenant_created ON trade_flows (tenant_id, created_at DESC);
