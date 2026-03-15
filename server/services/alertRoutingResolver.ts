import { and, desc, eq } from "drizzle-orm";
import { db } from "../db";
import { userContactMethods } from "@shared/schema";

export type ContactMethodRow = {
  id: string;
  userId: string;
  type: "EMAIL" | "WEBHOOK";
  value: string;
  isPrimary: boolean;
  createdAt: Date;
};

export const selectContactMethod = (methods: ContactMethodRow[]) => {
  const primaryWebhook = methods.find((m) => m.isPrimary && m.type === "WEBHOOK");
  const anyWebhook = methods.find((m) => m.type === "WEBHOOK");
  const primaryEmail = methods.find((m) => m.isPrimary && m.type === "EMAIL");
  const anyEmail = methods.find((m) => m.type === "EMAIL");
  return primaryWebhook ?? anyWebhook ?? primaryEmail ?? anyEmail ?? null;
};

export async function listActiveUserContactMethods(args: {
  tenantId: string;
  userId: string;
}) {
  const rows = await db
    .select()
    .from(userContactMethods)
    .where(and(
      eq(userContactMethods.tenantId, args.tenantId),
      eq(userContactMethods.userId, args.userId),
      eq(userContactMethods.isActive, true),
    ))
    .orderBy(desc(userContactMethods.isPrimary), desc(userContactMethods.createdAt));

  return rows.map((row) => ({
    id: row.id,
    userId: row.userId,
    type: row.type as "EMAIL" | "WEBHOOK",
    value: row.value,
    isPrimary: row.isPrimary,
    createdAt: row.createdAt,
  }));
}
