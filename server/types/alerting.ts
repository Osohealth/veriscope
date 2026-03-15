import type { alertDeliveries, alertDlq } from "@shared/schema";

export type DestinationType = "WEBHOOK" | "EMAIL";

export type DeliveryStatus = (typeof alertDeliveries.$inferSelect)["status"];
export type SkipReason = (typeof alertDeliveries.$inferSelect)["skipReason"] | null;

export type DestinationGateState = "ACTIVE" | "PAUSED" | "AUTO_PAUSED" | "DISABLED";
export type GateResult = {
  allowed: boolean;
  state: DestinationGateState;
  reason?: string;
  readyToResume?: boolean;
  details?: Record<string, unknown>;
};

export type DispatchContext = {
  tenantId: string;
  userId?: string | null;
  role?: string | null;
  now: Date;
  requestId?: string;
  actorApiKeyId?: string;
};

export type AlertDeliveryInsert = typeof alertDeliveries.$inferInsert;
export type AlertDlqRow = typeof alertDlq.$inferSelect;
export type DeliveryInsertShape = AlertDeliveryInsert;
