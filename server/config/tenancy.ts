export const TENANT_DEMO_ID = "00000000-0000-0000-0000-000000000001";

export const resolveTenantId = () => {
  return process.env.ALERTS_TENANT_ID ?? TENANT_DEMO_ID;
};
