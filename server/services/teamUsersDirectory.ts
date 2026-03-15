import { and, desc, eq, or, sql } from "drizzle-orm";
import { db } from "../db";
import { tenantUsers } from "@shared/schema";

export type TeamUserDirectoryQuery = {
  tenantId: string;
  query?: string | null;
  limit?: number;
  cursorCreatedAt?: string | null;
  cursorId?: string | null;
};

export type TeamUserDirectoryItem = {
  user_id: string;
  name: string | null;
  email: string;
  role: string;
  status: string;
  created_at: Date;
};

export async function listTeamUsersDirectory(args: TeamUserDirectoryQuery) {
  const limit = Math.max(1, Math.min(Number(args.limit ?? 20) || 20, 50));
  const filters: any[] = [
    eq(tenantUsers.tenantId, args.tenantId),
    eq(tenantUsers.status, "ACTIVE"),
  ];

  const query = args.query ? String(args.query).trim().toLowerCase() : "";
  if (query) {
    const like = `%${query}%`;
    filters.push(or(
      sql`lower(${tenantUsers.email}) like ${like}`,
      sql`lower(${tenantUsers.displayName}) like ${like}`,
    ));
  }

  if (args.cursorCreatedAt && args.cursorId) {
    const cursorDate = new Date(args.cursorCreatedAt);
    filters.push(or(
      sql`${tenantUsers.createdAt} < ${cursorDate}`,
      sql`${tenantUsers.createdAt} = ${cursorDate} AND ${tenantUsers.id} < ${args.cursorId}`,
    ));
  }

  const rows = await db
    .select()
    .from(tenantUsers)
    .where(and(...filters))
    .orderBy(desc(tenantUsers.createdAt), desc(tenantUsers.id))
    .limit(limit + 1);

  const hasMore = rows.length > limit;
  const pageRows = rows.slice(0, limit);
  const items = pageRows.map((row) => ({
    user_id: row.userId,
    name: row.displayName ?? null,
    email: row.email,
    role: row.role,
    status: row.status,
    created_at: row.createdAt,
  }));

  let nextCursor: string | null = null;
  if (hasMore && pageRows.length > 0) {
    const last = pageRows[pageRows.length - 1];
    nextCursor = Buffer.from(`${last.createdAt.toISOString()}|${last.id}`).toString("base64");
  }

  return { items, nextCursor };
}
