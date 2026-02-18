import { db } from "../db";
import { alertQualityGateBreaches } from "@shared/schema";

export async function recordQualityGateSuppressOnce(params: {
  tenantId: string;
  subscriptionId: string;
  day: Date;
}) {
  const [row] = await db
    .insert(alertQualityGateBreaches)
    .values({
      tenantId: params.tenantId,
      subscriptionId: params.subscriptionId,
      day: params.day,
    })
    .onConflictDoNothing()
    .returning({ id: alertQualityGateBreaches.id });

  return Boolean(row?.id);
}
