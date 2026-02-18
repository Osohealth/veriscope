import { and, eq } from "drizzle-orm";
import { db } from "../db";
import { alertDestinationStates } from "@shared/schema";

export type DestinationState = "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
export type DestinationBlockReason =
  | "DESTINATION_DISABLED"
  | "DESTINATION_PAUSED"
  | "DESTINATION_AUTO_PAUSED";

export type DestinationGateResult = {
  applied: boolean;
  allowed: boolean;
  state: DestinationState;
  reason?: DestinationBlockReason;
  readyToResume?: boolean;
  details?: Record<string, unknown>;
};

export async function getDestinationGate(args: {
  tenantId: string;
  destinationKey: string;
  now: Date;
}): Promise<DestinationGateResult> {
  const { tenantId, destinationKey, now } = args;

  const [row] = await db
    .select({
      state: alertDestinationStates.state,
      resumeReadyAt: alertDestinationStates.resumeReadyAt,
    })
    .from(alertDestinationStates)
    .where(and(
      eq(alertDestinationStates.tenantId, tenantId),
      eq(alertDestinationStates.destinationKey, destinationKey),
    ))
    .limit(1);

  if (!row) {
    return { applied: false, allowed: true, state: "ACTIVE" };
  }

  const state = String(row.state ?? "ACTIVE").toUpperCase() as DestinationState;

  if (state === "DISABLED") {
    return { applied: true, allowed: false, state, reason: "DESTINATION_DISABLED" };
  }

  if (state === "PAUSED") {
    return { applied: true, allowed: false, state, reason: "DESTINATION_PAUSED" };
  }

  if (state === "AUTO_PAUSED") {
    const resumeReadyAt = row.resumeReadyAt ? new Date(row.resumeReadyAt) : null;
    const readyToResume = Boolean(resumeReadyAt && resumeReadyAt.getTime() <= now.getTime());
    return {
      applied: true,
      allowed: readyToResume,
      state,
      reason: readyToResume ? undefined : "DESTINATION_AUTO_PAUSED",
      readyToResume,
      details: { resume_ready_at: resumeReadyAt?.toISOString() ?? null },
    };
  }

  return { applied: true, allowed: true, state };
}
