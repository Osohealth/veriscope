import { db } from "../db";
import { alertQualityGateBreaches } from "@shared/schema";

export async function recordQualityGateSuppressOnce(params: {
  tenantId: string;
  subscriptionId: string;
  day: Date | string;
}) {
  const dayValue = params.day instanceof Date
    ? params.day.toISOString().slice(0, 10)
    : String(params.day);
  const [row] = await db
    .insert(alertQualityGateBreaches)
    .values({
      tenantId: params.tenantId,
      subscriptionId: params.subscriptionId,
      day: dayValue,
    })
    .onConflictDoNothing()
    .returning({ id: alertQualityGateBreaches.id });

  return Boolean(row?.id);
}
