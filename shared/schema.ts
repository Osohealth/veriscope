import { pgTable, varchar, text, integer, decimal, doublePrecision, timestamp, boolean, jsonb, serial, date, index, uniqueIndex, uuid, primaryKey } from "drizzle-orm/pg-core";
import { sql } from "drizzle-orm";
import { createInsertSchema } from "drizzle-zod";
import { z } from "zod";

// ===== CORE ENTITIES =====

// Commodities (Oil, Gas, LNG, Refined Products, Chemicals, etc.)
export const commodities = pgTable("commodities", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  code: varchar("code", { length: 20 }).notNull().unique(),
  category: varchar("category", { length: 50 }).notNull(), // oil, gas, lng, refined_products, chemicals, dry_bulk
  subcategory: varchar("subcategory", { length: 50 }), // crude_oil, gasoline, diesel, etc.
  unit: varchar("unit", { length: 20 }).notNull(), // barrel, ton, mmbtu
  description: text("description"),
  specifications: jsonb("specifications"), // quality specs, API gravity, sulfur content, etc.
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Markets (Physical, Power, Financial, Shipping)
export const markets = pgTable("markets", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  code: varchar("code", { length: 20 }).notNull().unique(),
  type: varchar("type", { length: 30 }).notNull(), // physical, power, financial, shipping
  region: varchar("region", { length: 50 }), // global, europe, asia, americas
  description: text("description"),
  currency: varchar("currency", { length: 3 }).default("USD"),
  timezone: varchar("timezone", { length: 50 }).default("UTC"),
  tradingHours: jsonb("trading_hours"),
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Global Ports and Terminals
export const ports = pgTable("ports", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  code: varchar("code", { length: 10 }).notNull().unique(),
  unlocode: varchar("unlocode", { length: 10 }), // UN/LOCODE e.g. NLRTM
  country: varchar("country", { length: 50 }).notNull(),
  countryCode: varchar("country_code", { length: 10 }), // ISO 2-letter country code
  region: varchar("region", { length: 50 }).notNull().default("Unknown"),
  latitude: decimal("latitude", { precision: 10, scale: 7 }).notNull(),
  longitude: decimal("longitude", { precision: 10, scale: 7 }).notNull(),
  timezone: varchar("timezone", { length: 50 }), // e.g. Europe/Amsterdam
  type: varchar("type", { length: 30 }).notNull(), // oil_terminal, lng_terminal, container_port, dry_bulk
  capacity: integer("capacity"), // in relevant units
  depth: decimal("depth", { precision: 5, scale: 2 }), // max vessel draft in meters
  geofenceRadiusKm: decimal("geofence_radius_km", { precision: 6, scale: 2 }).default(sql`3.0`), // port area radius
  facilities: jsonb("facilities"), // storage tanks, loading arms, etc.
  operationalStatus: varchar("operational_status", { length: 20 }).default("active"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
});

// Vessels Database
export const vessels = pgTable("vessels", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  mmsi: varchar("mmsi", { length: 15 }).notNull().unique(),
  name: varchar("name", { length: 100 }).notNull(),
  imo: varchar("imo", { length: 15 }),
  vesselType: varchar("vessel_type", { length: 30 }).notNull(), // vlcc, suezmax, aframax, lng_carrier, container
  flag: varchar("flag", { length: 3 }),
  owner: varchar("owner", { length: 100 }),
  operator: varchar("operator", { length: 100 }),
  buildYear: integer("build_year"),
  deadweight: integer("deadweight"), // in tons
  length: decimal("length", { precision: 7, scale: 2 }),
  beam: decimal("beam", { precision: 6, scale: 2 }),
  draft: decimal("draft", { precision: 5, scale: 2 }),
  capacity: integer("capacity"), // cargo capacity
  specifications: jsonb("specifications"), // engine, speed, etc.
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Real-time Vessel Positions
export const vesselPositions = pgTable("vessel_positions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  mmsi: varchar("mmsi", { length: 15 }), // denormalized for faster queries
  latitude: decimal("latitude", { precision: 10, scale: 7 }).notNull(),
  longitude: decimal("longitude", { precision: 10, scale: 7 }).notNull(),
  sogKnots: decimal("sog_knots", { precision: 5, scale: 2 }), // speed over ground
  cogDeg: decimal("cog_deg", { precision: 5, scale: 2 }), // course over ground
  course: decimal("course", { precision: 5, scale: 2 }), // kept for backwards compatibility
  speed: decimal("speed", { precision: 5, scale: 2 }), // kept for backwards compatibility
  heading: decimal("heading", { precision: 5, scale: 2 }),
  navStatus: varchar("nav_status", { length: 50 }), // Under way, At anchor, Moored, etc.
  status: varchar("status", { length: 30 }), // underway, anchored, moored (legacy)
  destination: varchar("destination", { length: 100 }),
  eta: timestamp("eta", { withTimezone: true }),
  source: varchar("source", { length: 50 }), // AIS provider/feed identifier
  timestampUtc: timestamp("timestamp_utc", { withTimezone: true }), // renamed for clarity
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Storage Facilities
export const storageFacilities = pgTable("storage_facilities", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  portId: varchar("port_id").references(() => ports.id),
  type: varchar("type", { length: 30 }).notNull(), // crude_oil, refined_products, lng, chemicals
  totalCapacity: integer("total_capacity").notNull(), // in relevant units
  currentLevel: integer("current_level").default(0),
  utilizationRate: decimal("utilization_rate", { precision: 5, scale: 2 }).default(sql`0`),
  operator: varchar("operator", { length: 100 }),
  specifications: jsonb("specifications"),
  lastUpdated: timestamp("last_updated", { withTimezone: true }).defaultNow(),
  isActive: boolean("is_active").default(true),
});

// ===== TRADING & MARKET DATA =====

// Real-time Commodity Prices
export const commodityPrices = pgTable("commodity_prices", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  marketId: varchar("market_id").references(() => markets.id).notNull(),
  price: decimal("price", { precision: 12, scale: 4 }).notNull(),
  currency: varchar("currency", { length: 3 }).default("USD"),
  unit: varchar("unit", { length: 20 }).notNull(),
  priceType: varchar("price_type", { length: 20 }).notNull(), // spot, future, index
  contractMonth: varchar("contract_month", { length: 10 }), // for futures
  volume: integer("volume"),
  change: decimal("change", { precision: 10, scale: 4 }),
  changePercent: decimal("change_percent", { precision: 6, scale: 3 }),
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
});

// Trade Flows and Cargo Tracking
export const tradeFlows = pgTable("trade_flows", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: varchar("tenant_id", { length: 255 }).notNull().default("00000000-0000-0000-0000-000000000001"),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  originPortId: varchar("origin_port_id").references(() => ports.id),
  destinationPortId: varchar("destination_port_id").references(() => ports.id),
  cargoVolume: integer("cargo_volume").notNull(),
  cargoValue: decimal("cargo_value", { precision: 15, scale: 2 }),
  loadingDate: timestamp("loading_date", { withTimezone: true }),
  departureDate: timestamp("departure_date", { withTimezone: true }),
  expectedArrival: timestamp("expected_arrival", { withTimezone: true }),
  actualArrival: timestamp("actual_arrival", { withTimezone: true }),
  status: varchar("status", { length: 30 }).notNull(), // loading, in_transit, discharging, completed
  charterer: varchar("charterer", { length: 100 }),
  trader: varchar("trader", { length: 100 }),
  freight: decimal("freight", { precision: 12, scale: 2 }),
  grade: varchar("grade", { length: 50 }), // Brent, WTI, Dubai, etc.
  hasSTS: boolean("has_sts").default(false), // indicates if cargo involves STS transfer
  isSplit: boolean("is_split").default(false), // indicates if cargo is split into multiple products
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Cargo Legs (individual stops in a cargo chain)
export const cargoLegs = pgTable("cargo_legs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tradeFlowId: varchar("trade_flow_id").references(() => tradeFlows.id).notNull(),
  sequence: integer("sequence").notNull(), // order in the cargo chain (1, 2, 3...)
  portId: varchar("port_id").references(() => ports.id).notNull(),
  legType: varchar("leg_type", { length: 20 }).notNull(), // origin, waypoint, sts_point, destination
  arrivalDate: timestamp("arrival_date", { withTimezone: true }),
  departureDate: timestamp("departure_date", { withTimezone: true }),
  volumeLoaded: integer("volume_loaded").default(0),
  volumeDischargedRounded: integer("volume_discharged").default(0),
  activity: varchar("activity", { length: 50 }), // loading, discharging, bunkering, sts_transfer
  waitTimeHours: decimal("wait_time_hours", { precision: 6, scale: 2 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// STS Events (Ship-to-Ship Transshipment)
export const stsEvents = pgTable("sts_events", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  motherVesselId: varchar("mother_vessel_id").references(() => vessels.id).notNull(),
  daughterVesselId: varchar("daughter_vessel_id").references(() => vessels.id).notNull(),
  tradeFlowId: varchar("trade_flow_id").references(() => tradeFlows.id),
  locationPortId: varchar("location_port_id").references(() => ports.id), // nearest port
  latitude: decimal("latitude", { precision: 10, scale: 7 }),
  longitude: decimal("longitude", { precision: 10, scale: 7 }),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  volumeTransferred: integer("volume_transferred").notNull(),
  grade: varchar("grade", { length: 50 }),
  startTime: timestamp("start_time", { withTimezone: true }).notNull(),
  endTime: timestamp("end_time", { withTimezone: true }),
  status: varchar("status", { length: 30 }).default("in_progress"), // scheduled, in_progress, completed, cancelled
  reason: varchar("reason", { length: 100 }), // arbitrage, blending, storage_optimization
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Cargo Splits (when cargo is divided into multiple products/grades)
export const cargoSplits = pgTable("cargo_splits", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tradeFlowId: varchar("trade_flow_id").references(() => tradeFlows.id).notNull(),
  splitSequence: integer("split_sequence").notNull(), // 1, 2, 3... for multiple splits
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  grade: varchar("grade", { length: 50 }).notNull(),
  volume: integer("volume").notNull(),
  percentage: decimal("percentage", { precision: 5, scale: 2 }).notNull(), // % of total cargo
  destinationPortId: varchar("destination_port_id").references(() => ports.id),
  destinationVesselId: varchar("destination_vessel_id").references(() => vessels.id), // for STS splits
  buyer: varchar("buyer", { length: 100 }),
  price: decimal("price", { precision: 12, scale: 4 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Flow Forecasts (ML-based short-term trade flow predictions)
export const flowForecasts = pgTable("flow_forecasts", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  originPortId: varchar("origin_port_id").references(() => ports.id).notNull(),
  destinationPortId: varchar("destination_port_id").references(() => ports.id).notNull(),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  timeframe: varchar("timeframe", { length: 20 }).notNull(), // 7d, 14d, 30d
  forecastedVolume: integer("forecasted_volume").notNull(), // tons
  forecastedVesselCount: integer("forecasted_vessel_count").notNull(),
  confidence: decimal("confidence", { precision: 5, scale: 4 }).notNull(),
  trend: varchar("trend", { length: 20 }).notNull(), // increasing, decreasing, stable
  historicalAverage: integer("historical_average"), // for comparison
  factors: jsonb("factors"), // weather, seasonality, price spreads, congestion
  modelVersion: varchar("model_version", { length: 20 }),
  validFrom: timestamp("valid_from", { withTimezone: true }).notNull(),
  validUntil: timestamp("valid_until", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Market Analytics and Balances
export const marketAnalytics = pgTable("market_analytics", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  marketId: varchar("market_id").references(() => markets.id).notNull(),
  region: varchar("region", { length: 50 }).notNull(),
  supplyData: jsonb("supply_data"), // production, imports, exports
  demandData: jsonb("demand_data"), // consumption, refinery runs
  inventoryData: jsonb("inventory_data"), // storage levels, days of cover
  balanceData: jsonb("balance_data"), // supply/demand balance
  period: varchar("period", { length: 20 }).notNull(), // daily, weekly, monthly
  periodStart: timestamp("period_start", { withTimezone: true }).notNull(),
  periodEnd: timestamp("period_end", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== USER MANAGEMENT =====

// Organizations (Phase One)
export const organizations = pgTable("organizations", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 200 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Users with different roles
export const users = pgTable("users", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  email: varchar("email", { length: 255 }).notNull().unique(),
  passwordHash: varchar("password_hash", { length: 255 }).notNull(),
  fullName: varchar("full_name", { length: 100 }),
  name: varchar("name", { length: 100 }), // kept for backwards compatibility
  organizationId: varchar("organization_id").references(() => organizations.id),
  role: varchar("role", { length: 30 }).notNull().default("analyst"), // admin, analyst, viewer
  company: varchar("company", { length: 100 }),
  department: varchar("department", { length: 50 }),
  permissions: jsonb("permissions"),
  preferences: jsonb("preferences"), // dashboard config, favorite markets, etc.
  isActive: boolean("is_active").default(true),
  lastLogin: timestamp("last_login", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
});

// Failed login attempts — used for per-account lockout (OWASP A07)
export const failedLoginAttempts = pgTable("failed_login_attempts", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id", { length: 255 }),
  emailAttempted: varchar("email_attempted", { length: 255 }).notNull(),
  ipAddress: varchar("ip_address", { length: 45 }),
  attemptedAt: timestamp("attempted_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  userWindowIdx: index("failed_login_attempts_user_window").on(table.userId, table.attemptedAt),
  emailWindowIdx: index("failed_login_attempts_email_window").on(table.emailAttempted, table.attemptedAt),
}));

// Revoked refresh tokens — used for JTI-based token rotation (OWASP A07)
export const revokedRefreshTokens = pgTable("revoked_refresh_tokens", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  jti: varchar("jti", { length: 255 }).notNull(),
  userId: varchar("user_id", { length: 255 }).notNull(),
  revokedAt: timestamp("revoked_at", { withTimezone: true }).defaultNow().notNull(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
}, (table) => ({
  jtiIdx: uniqueIndex("revoked_refresh_tokens_jti").on(table.jti),
  expiresIdx: index("revoked_refresh_tokens_expires").on(table.expiresAt),
}));

// API Keys (Alerts Auth)
export const apiKeys = pgTable("api_keys", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: varchar("user_id"),
  organizationId: varchar("organization_id"),
  keyHash: varchar("key_hash", { length: 255 }).notNull(),
  name: varchar("name", { length: 100 }),
  label: varchar("label", { length: 100 }),
  role: varchar("role", { length: 20 }).notNull().default("OWNER"),
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  revokedAt: timestamp("revoked_at", { withTimezone: true }),
  expiresAt: timestamp("expires_at", { withTimezone: true }),
}, (table) => ({
  keyHashUnique: uniqueIndex("api_keys_key_hash_unique").on(table.keyHash),
  tenantKeyHashIdx: index("api_keys_tenant_key_hash").on(table.tenantId, table.keyHash),
}));

export const tenantUsers = pgTable("tenant_users", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: uuid("user_id").notNull(),
  email: varchar("email", { length: 255 }).notNull(),
  displayName: varchar("display_name", { length: 200 }),
  role: varchar("role", { length: 20 }).notNull().default("VIEWER"),
  status: varchar("status", { length: 20 }).notNull().default("INVITED"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  createdBy: uuid("created_by"),
  revokedAt: timestamp("revoked_at", { withTimezone: true }),
  revokedBy: uuid("revoked_by"),
}, (table) => ({
  tenantEmailIdx: uniqueIndex("tenant_users_tenant_email").on(table.tenantId, table.email),
  tenantUserIdx: uniqueIndex("tenant_users_tenant_user").on(table.tenantId, table.userId),
  tenantStatusIdx: index("tenant_users_tenant_status_created").on(table.tenantId, table.status, table.createdAt),
}));

export const tenantInvites = pgTable("tenant_invites", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  email: varchar("email", { length: 255 }).notNull(),
  role: varchar("role", { length: 20 }).notNull().default("VIEWER"),
  tokenHash: varchar("token_hash", { length: 255 }).notNull(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  createdBy: uuid("created_by").notNull(),
  acceptedAt: timestamp("accepted_at", { withTimezone: true }),
  acceptedByUserId: uuid("accepted_by_user_id"),
  revokedAt: timestamp("revoked_at", { withTimezone: true }),
  revokedBy: uuid("revoked_by"),
}, (table) => ({
  tokenUnique: uniqueIndex("tenant_invites_token_unique").on(table.tokenHash),
  tenantEmailIdx: index("tenant_invites_tenant_email").on(table.tenantId, table.email),
  tenantStatusIdx: index("tenant_invites_tenant_status").on(table.tenantId, table.acceptedAt, table.revokedAt),
}));

export const tenantSettings = pgTable("tenant_settings", {
  tenantId: uuid("tenant_id").primaryKey(),
  auditRetentionDays: integer("audit_retention_days").notNull().default(90),
  allowedEmailDomains: text("allowed_email_domains").array().notNull().default(sql`ARRAY[]::text[]`),
  allowedWebhookHosts: text("allowed_webhook_hosts").array().notNull().default(sql`ARRAY[]::text[]`),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
});

export const auditExports = pgTable("audit_exports", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  requestedByUserId: uuid("requested_by_user_id").notNull(),
  format: text("format").notNull(),
  filters: jsonb("filters").notNull(),
  rowCount: integer("row_count").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
}, (table) => ({
  tenantCreatedIdx: index("audit_exports_tenant_created").on(table.tenantId, table.createdAt, table.id),
}));

export const auditEvents = pgTable("audit_events", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  actorType: text("actor_type").notNull(),
  actorUserId: uuid("actor_user_id"),
  actorApiKeyId: uuid("actor_api_key_id"),
  actorLabel: text("actor_label"),
  action: text("action").notNull(),
  resourceType: text("resource_type").notNull(),
  resourceId: text("resource_id"),
  severity: text("severity").notNull(),
  status: text("status").notNull(),
  message: text("message").notNull(),
  metadata: jsonb("metadata").notNull().default({}),
  ip: text("ip"),
  userAgent: text("user_agent"),
  requestId: text("request_id"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => ({
  tenantCreatedIdx: index("audit_events_tenant_created_id").on(table.tenantId, table.createdAt, table.id),
  tenantActionIdx: index("audit_events_tenant_action_created").on(table.tenantId, table.action, table.createdAt, table.id),
  tenantResourceIdx: index("audit_events_tenant_resource_created").on(table.tenantId, table.resourceType, table.resourceId, table.createdAt, table.id),
  tenantActorIdx: index("audit_events_tenant_actor_created").on(table.tenantId, table.actorType, table.actorUserId, table.actorApiKeyId, table.createdAt, table.id),
}));

export const incidents = pgTable("incidents", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  type: text("type").notNull(),
  destinationKey: text("destination_key"),
  status: text("status").notNull().default("OPEN"),
  severity: text("severity").notNull(),
  title: text("title").notNull(),
  summary: text("summary").notNull(),
  openedAt: timestamp("opened_at", { withTimezone: true }).notNull().defaultNow(),
  ackedAt: timestamp("acked_at", { withTimezone: true }),
  resolvedAt: timestamp("resolved_at", { withTimezone: true }),
  openedByActorType: text("opened_by_actor_type").notNull(),
  openedByActorId: uuid("opened_by_actor_id"),
  ackedByActorType: text("acked_by_actor_type"),
  ackedByActorId: uuid("acked_by_actor_id"),
  resolvedByActorType: text("resolved_by_actor_type"),
  resolvedByActorId: uuid("resolved_by_actor_id"),
}, (table) => ({
  tenantStatusOpenedIdx: index("incidents_tenant_status_opened").on(table.tenantId, table.status, table.openedAt.desc()),
  tenantDestinationIdx: index("incidents_tenant_destination").on(table.tenantId, table.destinationKey, table.openedAt.desc()),
}));

export const incidentEscalationPolicies = pgTable("incident_escalation_policies", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  incidentType: text("incident_type").notNull(),
  severityMin: text("severity_min").notNull(),
  level: integer("level").notNull(),
  afterMinutes: integer("after_minutes").notNull(),
  targetType: text("target_type").notNull(),
  targetRef: text("target_ref").notNull(),
  targetName: text("target_name"),
  enabled: boolean("enabled").notNull().default(true),
  lastValidatedAt: timestamp("last_validated_at", { withTimezone: true }),
  lastRoutingHealth: jsonb("last_routing_health"),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => ({
  uniquePolicy: uniqueIndex("incident_escalation_policies_unique").on(
    table.tenantId,
    table.incidentType,
    table.severityMin,
    table.level,
    table.targetType,
    table.targetRef,
  ),
  lookupIdx: index("incident_escalation_policies_lookup").on(
    table.tenantId,
    table.incidentType,
    table.enabled,
    table.severityMin,
    table.afterMinutes,
  ),
  enabledIdx: index("incident_escalation_policies_enabled_idx").on(
    table.tenantId,
    table.enabled,
    table.incidentType,
    table.level,
  ),
}));

export const incidentEscalations = pgTable("incident_escalations", {
  incidentId: uuid("incident_id").primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  currentLevel: integer("current_level").notNull().default(0),
  lastEscalatedAt: timestamp("last_escalated_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => ({
  tenantIdx: index("incident_escalations_tenant").on(table.tenantId),
}));

export const userContactMethods = pgTable("user_contact_methods", {
  id: uuid("id").default(sql`gen_random_uuid()`).primaryKey(),
  tenantId: uuid("tenant_id").notNull(),
  userId: uuid("user_id").notNull(),
  type: text("type").notNull(),
  value: text("value").notNull(),
  label: text("label"),
  isPrimary: boolean("is_primary").notNull().default(false),
  isVerified: boolean("is_verified").notNull().default(true),
  isActive: boolean("is_active").notNull().default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => ({
  uniqueMethod: uniqueIndex("user_contact_methods_unique").on(table.tenantId, table.userId, table.type, table.value),
  primaryUnique: uniqueIndex("user_contact_methods_primary").on(table.tenantId, table.userId, table.type).where(sql`${table.isPrimary} = true`),
  tenantUserTypeActive: index("user_contact_methods_tenant_user_type_active").on(table.tenantId, table.userId, table.type, table.isActive),
  tenantUserPrimaryCreated: index("user_contact_methods_tenant_user_primary_created").on(table.tenantId, table.userId, table.isPrimary.desc(), table.createdAt.desc()),
}));

export const rateLimitBuckets = pgTable("rate_limit_buckets", {
  tenantId: uuid("tenant_id").notNull(),
  keyHash: text("key_hash").notNull(),
  scope: text("scope").notNull(),
  windowStart: timestamp("window_start", { withTimezone: true }).notNull(),
  count: integer("count").notNull().default(0),
  updatedAt: timestamp("updated_at", { withTimezone: true }).notNull().defaultNow(),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.keyHash, table.scope, table.windowStart] }),
  tenantScopeWindowIdx: index("rate_limit_buckets_tenant_scope_window").on(table.tenantId, table.scope, table.windowStart.desc()),
}));

// User Alerts and Notifications
export const alerts = pgTable("alerts", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").references(() => users.id).notNull(),
  type: varchar("type", { length: 30 }).notNull(), // price_move, trade_flow, vessel_arrival, storage_level
  title: varchar("title", { length: 200 }).notNull(),
  description: text("description"),
  conditions: jsonb("conditions"), // trigger conditions
  frequency: varchar("frequency", { length: 20 }).notNull(), // real_time, hourly, daily
  isActive: boolean("is_active").default(true),
  lastTriggered: timestamp("last_triggered", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Alert Notifications
export const notifications = pgTable("notifications", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").references(() => users.id).notNull(),
  alertId: varchar("alert_id").references(() => alerts.id),
  type: varchar("type", { length: 30 }).notNull(),
  title: varchar("title", { length: 200 }).notNull(),
  message: text("message"),
  severity: varchar("severity", { length: 20 }).notNull(), // info, warning, critical
  data: jsonb("data"), // related data for the notification
  isRead: boolean("is_read").default(false),
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
});

// Port Statistics (for congestion analysis and predictions)
export const portStats = pgTable("port_stats", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  date: timestamp("date", { withTimezone: true }).notNull(),
  arrivals: integer("arrivals").notNull(),
  departures: integer("departures").notNull(),
  queueLength: integer("queue_length").notNull(),
  averageWaitHours: decimal("average_wait_hours", { precision: 5, scale: 2 }).notNull(),
  totalVessels: integer("total_vessels").notNull(),
  throughputMT: decimal("throughput_mt", { precision: 10, scale: 2 }).notNull(),
  byClass: jsonb("by_class").notNull(), // VLCC, Suezmax, Aframax counts
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Port Daily Baselines (daily port activity baselines + rolling 30d stats)
export const portDailyBaselines = pgTable("port_daily_baselines", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  day: date("day").notNull(),
  arrivals: integer("arrivals").notNull().default(0),
  departures: integer("departures").notNull().default(0),
  uniqueVessels: integer("unique_vessels").notNull().default(0),
  avgDwellHours: doublePrecision("avg_dwell_hours"),
  openCalls: integer("open_calls").notNull().default(0),
  arrivals30dAvg: doublePrecision("arrivals_30d_avg"),
  arrivals30dStd: doublePrecision("arrivals_30d_std"),
  dwell30dAvg: doublePrecision("dwell_30d_avg"),
  dwell30dStd: doublePrecision("dwell_30d_std"),
  openCalls30dAvg: doublePrecision("open_calls_30d_avg"),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
}, (table) => ({
  portDayUnique: uniqueIndex("port_daily_baselines_port_day").on(table.portId, table.day),
  dayDescIdx: index("port_daily_baselines_day_desc").on(table.day.desc()),
  portDayDescIdx: index("port_daily_baselines_port_day_desc").on(table.portId, table.day.desc()),
}));

// Signals (persisted port-level anomaly signals)
export const signals = pgTable("signals", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  signalType: text("signal_type").notNull(),
  entityType: text("entity_type").notNull(),
  entityId: varchar("entity_id").notNull(),
  day: date("day").notNull(),
  severity: text("severity").notNull(),
  value: doublePrecision("value").notNull(),
  baseline: doublePrecision("baseline"),
  stddev: doublePrecision("stddev"),
  zscore: doublePrecision("zscore"),
  deltaPct: doublePrecision("delta_pct"),
  confidenceScore: doublePrecision("confidence_score"),
  confidenceBand: text("confidence_band"),
  method: text("method"),
  clusterId: text("cluster_id"),
  clusterKey: text("cluster_key"),
  clusterType: text("cluster_type"),
  clusterSeverity: text("cluster_severity"),
  clusterSummary: text("cluster_summary"),
  explanation: text("explanation").notNull(),
  metadata: jsonb("metadata").notNull().default(sql`'{}'::jsonb`),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
}, (table) => ({
  uniqueSignal: uniqueIndex("signals_unique").on(
    table.signalType,
    table.entityType,
    table.entityId,
    table.day,
  ),
  dayIdx: index("signals_day").on(table.day),
  entityDayIdx: index("signals_entity_day").on(table.entityId, table.day),
  dayClusterIdx: index("signals_day_cluster").on(table.day, table.clusterId),
  severityIdx: index("signals_severity").on(table.severity),
  typeIdx: index("signals_signal_type").on(table.signalType),
}));

// Market Predictions (AI-powered price forecasts)
export const predictions = pgTable("predictions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  marketId: varchar("market_id").references(() => markets.id).notNull(),
  timeframe: varchar("timeframe", { length: 10 }).notNull(), // 1D, 1W, 1M
  currentPrice: decimal("current_price", { precision: 12, scale: 4 }).notNull(),
  predictedPrice: decimal("predicted_price", { precision: 12, scale: 4 }).notNull(),
  confidence: decimal("confidence", { precision: 5, scale: 4 }).notNull(),
  direction: varchar("direction", { length: 10 }).notNull(), // up, down, stable
  features: jsonb("features"), // input features used for prediction
  metadata: jsonb("metadata"),
  validUntil: timestamp("valid_until", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Storage Fill Data (satellite-derived tank levels)
export const storageFillData = pgTable("storage_fill_data", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  siteId: varchar("site_id").references(() => storageFacilities.id).notNull(),
  timestamp: timestamp("timestamp", { withTimezone: true }).notNull(),
  fillIndex: decimal("fill_index", { precision: 5, scale: 4 }).notNull(), // 0.0 to 1.0
  confidence: decimal("confidence", { precision: 5, scale: 4 }).notNull(),
  source: varchar("source", { length: 20 }).notNull(), // SAR, optical, manual
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Floating Storage (vessels acting as storage facilities)
export const floatingStorage = pgTable("floating_storage", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id),
  vesselName: varchar("vessel_name", { length: 100 }).notNull(),
  vesselType: varchar("vessel_type", { length: 30 }).notNull(), // VLCC, Suezmax, Aframax, LNG
  imo: varchar("imo", { length: 10 }),
  cargoType: varchar("cargo_type", { length: 50 }).notNull(), // crude_oil, refined_products, lng
  cargoGrade: varchar("cargo_grade", { length: 50 }), // Brent, Oman, ULSD, etc.
  cargoVolume: integer("cargo_volume").notNull(), // MT or barrels
  cargoUnit: varchar("cargo_unit", { length: 10 }).default("MT"),
  locationLat: decimal("location_lat", { precision: 10, scale: 7 }),
  locationLng: decimal("location_lng", { precision: 10, scale: 7 }),
  region: varchar("region", { length: 50 }), // North Sea, Persian Gulf, Singapore, etc.
  durationDays: integer("duration_days").notNull(), // how long in storage mode
  startDate: timestamp("start_date", { withTimezone: true }).notNull(),
  estimatedValue: decimal("estimated_value", { precision: 15, scale: 2 }), // USD
  charterer: varchar("charterer", { length: 100 }),
  status: varchar("status", { length: 30 }).default("active"), // active, releasing, completed
  lastUpdated: timestamp("last_updated", { withTimezone: true }).defaultNow(),
});

// Strategic Petroleum Reserve (SPR) Data
export const sprReserves = pgTable("spr_reserves", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  country: varchar("country", { length: 50 }).notNull(),
  countryCode: varchar("country_code", { length: 3 }).notNull(),
  region: varchar("region", { length: 50 }), // facility location
  gradeType: varchar("grade_type", { length: 50 }).notNull(), // sweet_crude, sour_crude, middle_east, etc.
  volumeBarrels: decimal("volume_barrels", { precision: 14, scale: 2 }).notNull(),
  percentOfTotal: decimal("percent_of_total", { precision: 5, scale: 2 }).notNull(),
  capacityBarrels: decimal("capacity_barrels", { precision: 14, scale: 2 }),
  utilizationRate: decimal("utilization_rate", { precision: 5, scale: 2 }),
  daysOfCover: integer("days_of_cover"), // days of import cover
  lastReleaseDate: timestamp("last_release_date", { withTimezone: true }),
  lastReleaseVolume: decimal("last_release_volume", { precision: 12, scale: 2 }),
  reportDate: timestamp("report_date", { withTimezone: true }).notNull(),
  source: varchar("source", { length: 50 }), // DOE, IEA, national_agency
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Storage Time Series (aggregated historical data for charts)
export const storageTimeSeries = pgTable("storage_time_series", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  recordDate: timestamp("record_date", { withTimezone: true }).notNull(),
  metricType: varchar("metric_type", { length: 50 }).notNull(), // tank_level, floating_storage, spr_total
  region: varchar("region", { length: 50 }), // global, north_america, europe, asia
  storageType: varchar("storage_type", { length: 50 }), // crude_oil, refined_products, lng
  totalCapacity: decimal("total_capacity", { precision: 14, scale: 2 }),
  currentLevel: decimal("current_level", { precision: 14, scale: 2 }).notNull(),
  utilizationRate: decimal("utilization_rate", { precision: 5, scale: 2 }),
  weekOverWeekChange: decimal("week_over_week_change", { precision: 14, scale: 2 }),
  yearOverYearChange: decimal("year_over_year_change", { precision: 14, scale: 2 }),
  fiveYearAverage: decimal("five_year_average", { precision: 14, scale: 2 }),
  confidence: decimal("confidence", { precision: 5, scale: 4 }),
  source: varchar("source", { length: 50 }), // satellite, eia, iea, industry
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== PORT DELAYS & MARKET IMPACT =====

// Port Delay Events (tracking individual vessel delays at ports)
export const portDelayEvents = pgTable("port_delay_events", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  expectedArrival: timestamp("expected_arrival", { withTimezone: true }).notNull(),
  actualArrival: timestamp("actual_arrival", { withTimezone: true }),
  delayHours: decimal("delay_hours", { precision: 6, scale: 2 }).notNull(),
  delayReason: varchar("delay_reason", { length: 50 }), // congestion, weather, maintenance, customs
  cargoVolume: integer("cargo_volume"), // tons
  cargoType: varchar("cargo_type", { length: 50 }), // crude_oil, refined_products, lng
  queuePosition: integer("queue_position"),
  status: varchar("status", { length: 30 }).default("pending"), // pending, in_queue, berthing, discharged
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Vessel Delay Snapshots (current delay status for active vessels)
export const vesselDelaySnapshots = pgTable("vessel_delay_snapshots", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  currentPortId: varchar("current_port_id").references(() => ports.id),
  destinationPortId: varchar("destination_port_id").references(() => ports.id),
  scheduledETA: timestamp("scheduled_eta", { withTimezone: true }),
  currentETA: timestamp("current_eta", { withTimezone: true }),
  delayHours: decimal("delay_hours", { precision: 6, scale: 2 }).notNull(),
  cargoVolume: integer("cargo_volume"),
  cargoValue: decimal("cargo_value", { precision: 15, scale: 2 }),
  commodityId: varchar("commodity_id").references(() => commodities.id),
  impactSeverity: varchar("impact_severity", { length: 20 }).default("low"), // low, medium, high, critical
  lastUpdated: timestamp("last_updated", { withTimezone: true }).defaultNow(),
});

// Market Delay Impacts (aggregated delay impacts on market supply/demand)
export const marketDelayImpacts = pgTable("market_delay_impacts", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  commodityId: varchar("commodity_id").references(() => commodities.id).notNull(),
  marketId: varchar("market_id").references(() => markets.id),
  timeframe: varchar("timeframe", { length: 10 }).notNull(), // 24h, 48h, 7d
  totalDelayedVolume: integer("total_delayed_volume").notNull(), // tons delayed
  totalDelayedValue: decimal("total_delayed_value", { precision: 15, scale: 2 }),
  averageDelayHours: decimal("average_delay_hours", { precision: 6, scale: 2 }).notNull(),
  vesselCount: integer("vessel_count").notNull(), // number of delayed vessels
  supplyImpact: decimal("supply_impact", { precision: 8, scale: 2 }), // estimated % supply reduction
  priceImpact: decimal("price_impact", { precision: 8, scale: 4 }), // estimated price change
  confidence: decimal("confidence", { precision: 5, scale: 4 }).notNull(),
  metadata: jsonb("metadata"),
  validUntil: timestamp("valid_until", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== MARITIME INTELLIGENCE =====

// Port Calls/Events (tracking vessel port visits)
export const portCalls = pgTable("port_calls", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  callType: varchar("call_type", { length: 30 }).notNull(), // arrival, departure, anchorage, berth
  status: varchar("status", { length: 30 }).notNull(), // scheduled, in_progress, completed
  arrivalTime: timestamp("arrival_time", { withTimezone: true }),
  departureTime: timestamp("departure_time", { withTimezone: true }),
  berthNumber: varchar("berth_number", { length: 20 }),
  anchorageZone: varchar("anchorage_zone", { length: 50 }),
  purpose: varchar("purpose", { length: 50 }), // loading, discharging, bunkering, crew_change, repair
  cargoOperation: jsonb("cargo_operation"), // type, volume, grade
  waitTimeHours: decimal("wait_time_hours", { precision: 6, scale: 2 }),
  berthTimeHours: decimal("berth_time_hours", { precision: 6, scale: 2 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Container Operations (TEU tracking and container intelligence)
export const containerOperations = pgTable("container_operations", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  portId: varchar("port_id").references(() => ports.id).notNull(),
  operationType: varchar("operation_type", { length: 30 }).notNull(), // load, discharge, transshipment
  containerType: varchar("container_type", { length: 30 }).notNull(), // 20ft, 40ft, 40ft_hc, reefer
  teuCount: integer("teu_count").notNull(), // twenty-foot equivalent units
  feuCount: integer("feu_count"), // forty-foot equivalent units
  commodityType: varchar("commodity_type", { length: 50 }), // general_cargo, refrigerated, hazmat
  origin: varchar("origin", { length: 100 }),
  destination: varchar("destination", { length: 100 }),
  shippingLine: varchar("shipping_line", { length: 100 }),
  bookingReference: varchar("booking_reference", { length: 50 }),
  operationDate: timestamp("operation_date", { withTimezone: true }).notNull(),
  handlingTime: decimal("handling_time", { precision: 6, scale: 2 }), // hours
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Bunkering Events (fuel stops and consumption tracking)
export const bunkeringEvents = pgTable("bunkering_events", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  vesselId: varchar("vessel_id").references(() => vessels.id).notNull(),
  portId: varchar("port_id").references(() => ports.id),
  eventType: varchar("event_type", { length: 30 }).notNull(), // port_bunkering, sts_bunkering, scheduled, emergency
  fuelType: varchar("fuel_type", { length: 50 }).notNull(), // vlsfo, hsfo, mgo, lng, methanol
  volumeMT: decimal("volume_mt", { precision: 10, scale: 2 }).notNull(), // metric tons
  pricePerMT: decimal("price_per_mt", { precision: 10, scale: 2 }),
  totalCost: decimal("total_cost", { precision: 12, scale: 2 }),
  supplier: varchar("supplier", { length: 100 }),
  startTime: timestamp("start_time", { withTimezone: true }).notNull(),
  endTime: timestamp("end_time", { withTimezone: true }),
  location: jsonb("location"), // lat/lon for at-sea bunkering
  grade: varchar("grade", { length: 50 }), // sulfur content, quality specs
  consumptionRate: decimal("consumption_rate", { precision: 8, scale: 2 }), // MT per day
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Communications/Messages (inbox and alert system)
export const communications = pgTable("communications", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").references(() => users.id),
  messageType: varchar("message_type", { length: 30 }).notNull(), // alert, notification, system, user_message
  category: varchar("category", { length: 50 }), // vessel_update, port_event, price_alert, delay_warning
  subject: varchar("subject", { length: 200 }).notNull(),
  body: text("body"),
  priority: varchar("priority", { length: 20 }).default("normal"), // low, normal, high, critical
  relatedEntityType: varchar("related_entity_type", { length: 30 }), // vessel, port, commodity, trade_flow
  relatedEntityId: varchar("related_entity_id", { length: 100 }),
  attachments: jsonb("attachments"),
  isRead: boolean("is_read").default(false),
  isArchived: boolean("is_archived").default(false),
  readAt: timestamp("read_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== COMMODITY PACK TABLES =====

// Crude & Products Grades (oil quality specifications and pricing)
export const crudeGrades = pgTable("crude_grades", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  gradeCode: varchar("grade_code", { length: 20 }).notNull().unique(),
  category: varchar("category", { length: 30 }).notNull(), // crude, gasoline, diesel, jet_fuel, naphtha
  origin: varchar("origin", { length: 100 }), // Saudi Arabia, US, Russia
  apiGravity: decimal("api_gravity", { precision: 5, scale: 2 }), // API gravity
  sulfurContent: decimal("sulfur_content", { precision: 5, scale: 3 }), // percentage
  viscosity: decimal("viscosity", { precision: 8, scale: 2 }),
  pourPoint: decimal("pour_point", { precision: 5, scale: 2 }), // celsius
  yieldProfile: jsonb("yield_profile"), // refinery yield percentages
  priceBenchmark: varchar("price_benchmark", { length: 50 }), // Brent, WTI, Dubai
  currentPrice: decimal("current_price", { precision: 10, scale: 2 }),
  priceUnit: varchar("price_unit", { length: 20 }).default("USD/bbl"),
  specifications: jsonb("specifications"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// LNG/LPG Cargoes (liquefied gas shipments and terminals)
export const lngCargoes = pgTable("lng_cargoes", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  cargoId: varchar("cargo_id", { length: 50 }).notNull().unique(),
  cargoType: varchar("cargo_type", { length: 20 }).notNull(), // lng, lpg, propane, butane
  vesselId: varchar("vessel_id").references(() => vessels.id),
  loadPortId: varchar("load_port_id").references(() => ports.id),
  dischargePortId: varchar("discharge_port_id").references(() => ports.id),
  volume: decimal("volume", { precision: 12, scale: 2 }).notNull(), // cubic meters or metric tons
  volumeUnit: varchar("volume_unit", { length: 20 }).default("m3"),
  isDiversion: boolean("is_diversion").default(false),
  originalDestination: varchar("original_destination", { length: 100 }),
  loadDate: timestamp("load_date", { withTimezone: true }),
  dischargeDate: timestamp("discharge_date", { withTimezone: true }),
  price: decimal("price", { precision: 10, scale: 2 }),
  priceUnit: varchar("price_unit", { length: 20 }).default("USD/mmbtu"),
  buyer: varchar("buyer", { length: 100 }),
  seller: varchar("seller", { length: 100 }),
  contractType: varchar("contract_type", { length: 30 }), // spot, term, fob, dap
  terminalCapacity: decimal("terminal_capacity", { precision: 10, scale: 2 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Dry Bulk Fixtures (coal, iron ore, grain vessel charters)
export const dryBulkFixtures = pgTable("dry_bulk_fixtures", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  fixtureId: varchar("fixture_id", { length: 50 }).notNull().unique(),
  commodityType: varchar("commodity_type", { length: 30 }).notNull(), // coal, iron_ore, grain, bauxite
  subtype: varchar("subtype", { length: 50 }), // thermal_coal, coking_coal, wheat, corn
  vesselId: varchar("vessel_id").references(() => vessels.id),
  vesselSize: varchar("vessel_size", { length: 30 }), // capesize, panamax, handymax
  loadPortId: varchar("load_port_id").references(() => ports.id),
  dischargePortId: varchar("discharge_port_id").references(() => ports.id),
  quantity: decimal("quantity", { precision: 12, scale: 2 }).notNull(), // metric tons
  freightRate: decimal("freight_rate", { precision: 10, scale: 2 }), // USD per ton
  charterer: varchar("charterer", { length: 100 }),
  shipper: varchar("shipper", { length: 100 }),
  laycanStart: timestamp("laycan_start", { withTimezone: true }), // laycan window start
  laycanEnd: timestamp("laycan_end", { withTimezone: true }), // laycan window end
  loadDate: timestamp("load_date", { withTimezone: true }),
  eta: timestamp("eta", { withTimezone: true }), // estimated time of arrival
  fixtureDate: timestamp("fixture_date", { withTimezone: true }).notNull(),
  marketIndex: varchar("market_index", { length: 30 }), // BCI, BPI, BSI
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Petrochemical Products (chemical products, yields, margins)
export const petrochemProducts = pgTable("petrochem_products", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  productName: varchar("product_name", { length: 100 }).notNull(),
  productCode: varchar("product_code", { length: 30 }).notNull().unique(),
  category: varchar("category", { length: 50 }).notNull(), // olefins, aromatics, polymers, intermediates
  subcategory: varchar("subcategory", { length: 50 }), // ethylene, propylene, benzene, polyethylene
  feedstock: varchar("feedstock", { length: 100 }), // naphtha, ethane, propane
  productionVolume: decimal("production_volume", { precision: 12, scale: 2 }), // metric tons
  yieldRate: decimal("yield_rate", { precision: 5, scale: 2 }), // percentage from feedstock
  marginSpread: decimal("margin_spread", { precision: 10, scale: 2 }), // USD per ton
  currentPrice: decimal("current_price", { precision: 10, scale: 2 }),
  priceUnit: varchar("price_unit", { length: 20 }).default("USD/ton"),
  facility: varchar("facility", { length: 100 }),
  region: varchar("region", { length: 50 }),
  capacity: decimal("capacity", { precision: 12, scale: 2 }), // annual capacity in tons
  utilizationRate: decimal("utilization_rate", { precision: 5, scale: 2 }), // percentage
  specifications: jsonb("specifications"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Agri & Biofuel Flows (oilseeds, biofuel production, sustainability)
export const agriBiofuelFlows = pgTable("agri_biofuel_flows", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  flowId: varchar("flow_id", { length: 50 }).notNull().unique(),
  commodityType: varchar("commodity_type", { length: 50 }).notNull(), // soybean, palm_oil, rapeseed, corn, ethanol, biodiesel
  flowType: varchar("flow_type", { length: 30 }).notNull(), // import, export, production, consumption
  originCountry: varchar("origin_country", { length: 50 }),
  destinationCountry: varchar("destination_country", { length: 50 }),
  volume: decimal("volume", { precision: 12, scale: 2 }).notNull(), // metric tons or liters
  volumeUnit: varchar("volume_unit", { length: 20 }).default("MT"),
  biofuelType: varchar("biofuel_type", { length: 30 }), // ethanol, biodiesel, saf, hvo
  feedstock: varchar("feedstock", { length: 100 }), // corn, sugarcane, used_cooking_oil
  sustainabilityCert: varchar("sustainability_cert", { length: 50 }), // ISCC, RSB, 2BSvs
  carbonIntensity: decimal("carbon_intensity", { precision: 8, scale: 2 }), // gCO2e/MJ
  price: decimal("price", { precision: 10, scale: 2 }),
  priceUnit: varchar("price_unit", { length: 20 }).default("USD/ton"),
  flowDate: timestamp("flow_date", { withTimezone: true }).notNull(),
  trader: varchar("trader", { length: 100 }),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Refinery/Plant Intelligence (capacity, utilization, yields, maintenance)
export const refineries = pgTable("refineries", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  refineryCode: varchar("refinery_code", { length: 30 }).notNull().unique(),
  country: varchar("country", { length: 50 }).notNull(),
  region: varchar("region", { length: 50 }).notNull(),
  operator: varchar("operator", { length: 100 }),
  capacity: decimal("capacity", { precision: 12, scale: 2 }).notNull(), // barrels per day
  currentThroughput: decimal("current_throughput", { precision: 12, scale: 2 }), // current crude runs
  utilizationRate: decimal("utilization_rate", { precision: 5, scale: 2 }), // percentage
  complexityIndex: decimal("complexity_index", { precision: 5, scale: 2 }), // Nelson Complexity Index
  yieldGasoline: decimal("yield_gasoline", { precision: 5, scale: 2 }), // percentage
  yieldDiesel: decimal("yield_diesel", { precision: 5, scale: 2 }), // percentage
  yieldJetFuel: decimal("yield_jet_fuel", { precision: 5, scale: 2 }), // percentage
  yieldOther: decimal("yield_other", { precision: 5, scale: 2 }), // percentage
  maintenanceStatus: varchar("maintenance_status", { length: 30 }).default("operational"), // operational, planned_maintenance, unplanned_outage
  maintenanceStart: timestamp("maintenance_start", { withTimezone: true }),
  maintenanceEnd: timestamp("maintenance_end", { withTimezone: true }),
  marginPerBarrel: decimal("margin_per_barrel", { precision: 8, scale: 2 }), // USD per barrel
  specifications: jsonb("specifications"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Supply & Demand Balances (production, consumption, trade flows)
export const supplyDemandBalances = pgTable("supply_demand_balances", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  balanceId: varchar("balance_id", { length: 50 }).notNull().unique(),
  commodity: varchar("commodity", { length: 50 }).notNull(), // crude_oil, gasoline, diesel, lng
  region: varchar("region", { length: 50 }).notNull(), // global, north_america, europe, asia, middle_east
  period: varchar("period", { length: 20 }).notNull(), // Q1_2024, 2024_M01
  production: decimal("production", { precision: 14, scale: 2 }), // thousand barrels per day or MT
  consumption: decimal("consumption", { precision: 14, scale: 2 }), // thousand barrels per day or MT
  imports: decimal("imports", { precision: 14, scale: 2 }), // thousand barrels per day or MT
  exports: decimal("exports", { precision: 14, scale: 2 }), // thousand barrels per day or MT
  inventoryChange: decimal("inventory_change", { precision: 12, scale: 2 }), // million barrels or MT
  closingInventory: decimal("closing_inventory", { precision: 14, scale: 2 }), // million barrels or MT
  balanceValue: decimal("balance_value", { precision: 12, scale: 2 }), // calculated surplus/deficit
  unit: varchar("unit", { length: 20 }).default("kbd"), // kbd = thousand barrels per day, MT
  forecastType: varchar("forecast_type", { length: 30 }), // actual, estimate, forecast
  dataSource: varchar("data_source", { length: 100 }), // IEA, EIA, OPEC
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Research & Insight Layer (market reports, analysis, forecasts)
export const researchReports = pgTable("research_reports", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  reportId: varchar("report_id", { length: 50 }).notNull().unique(),
  title: varchar("title", { length: 200 }).notNull(),
  category: varchar("category", { length: 50 }).notNull(), // market_analysis, price_forecast, trade_flow, supply_demand
  subcategory: varchar("subcategory", { length: 50 }), // crude_oil, lng, refining, shipping
  summary: text("summary").notNull(),
  keyInsights: jsonb("key_insights"), // array of key takeaways
  priceOutlook: varchar("price_outlook", { length: 30 }), // bullish, bearish, neutral
  shortTermForecast: text("short_term_forecast"), // 1-3 months
  mediumTermForecast: text("medium_term_forecast"), // 3-12 months
  longTermForecast: text("long_term_forecast"), // 1-3 years
  analyst: varchar("analyst", { length: 100 }),
  confidenceLevel: varchar("confidence_level", { length: 20 }), // high, medium, low
  dataPoints: jsonb("data_points"), // supporting data/charts
  tags: text("tags").array(), // searchable tags
  publishDate: timestamp("publish_date", { withTimezone: true }).notNull(),
  lastUpdated: timestamp("last_updated", { withTimezone: true }).defaultNow(),
  isPublished: boolean("is_published").default(true),
  metadata: jsonb("metadata"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== CSV-BASED DATA TABLES =====

// Refinery Units (plant operational units and capacities)
export const refineryUnits = pgTable("refinery_units", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  plant: varchar("plant", { length: 50 }).notNull(),
  unit: varchar("unit", { length: 30 }).notNull(), // CDU, VDU, FCC, HCU
  nameplateBpd: integer("nameplate_bpd").notNull(), // barrels per day capacity
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Refinery Utilization Daily (daily plant utilization data)
export const refineryUtilizationDaily = pgTable("refinery_utilization_daily", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  date: date("date").notNull(),
  plant: varchar("plant", { length: 50 }).notNull(),
  utilizationPct: decimal("utilization_pct", { precision: 5, scale: 2 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Refinery Crack Spreads Daily (daily pricing and spreads)
export const refineryCrackSpreadsDaily = pgTable("refinery_crack_spreads_daily", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  date: date("date").notNull(),
  spread321Usd: decimal("spread_321_usd", { precision: 10, scale: 2 }).notNull(), // 3-2-1 crack spread
  gasolineUsd: decimal("gasoline_usd", { precision: 10, scale: 2 }).notNull(),
  dieselUsd: decimal("diesel_usd", { precision: 10, scale: 2 }).notNull(),
  crudeUsd: decimal("crude_usd", { precision: 10, scale: 2 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Supply & Demand Models Daily (daily supply/demand data by region)
export const sdModelsDaily = pgTable("sd_models_daily", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  date: date("date").notNull(),
  region: varchar("region", { length: 50 }).notNull(), // EU, US, ASIA
  supplyMt: integer("supply_mt").notNull(), // metric tons
  demandMt: integer("demand_mt").notNull(), // metric tons
  balanceMt: integer("balance_mt").notNull(), // metric tons (supply - demand)
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Supply & Demand Forecasts Weekly (weekly balance forecasts by region)
export const sdForecastsWeekly = pgTable("sd_forecasts_weekly", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  weekEnd: date("week_end").notNull(),
  region: varchar("region", { length: 50 }).notNull(),
  balanceForecastMt: integer("balance_forecast_mt").notNull(), // metric tons
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Research Insights Daily (daily automated insights and analysis)
export const researchInsightsDaily = pgTable("research_insights_daily", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  date: date("date").notNull(),
  title: varchar("title", { length: 200 }).notNull(),
  summary: text("summary").notNull(),
  impactScore: decimal("impact_score", { precision: 3, scale: 2 }).notNull(), // 0.00 to 1.00
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// ===== SCHEMAS FOR VALIDATION =====

// Insert schemas
export const insertCommoditySchema = createInsertSchema(commodities).omit({
  id: true,
  createdAt: true,
});

export const insertMarketSchema = createInsertSchema(markets).omit({
  id: true,
  createdAt: true,
});

export const insertPortSchema = createInsertSchema(ports).omit({
  id: true,
  createdAt: true,
});

export const insertVesselSchema = createInsertSchema(vessels).omit({
  id: true,
  createdAt: true,
});

export const insertTradeFlowSchema = createInsertSchema(tradeFlows).omit({
  id: true,
  createdAt: true,
});

export const insertOrganizationSchema = createInsertSchema(organizations).omit({
  id: true,
  createdAt: true,
});

export const insertUserSchema = createInsertSchema(users).omit({
  id: true,
  createdAt: true,
  updatedAt: true,
});

export const insertApiKeySchema = createInsertSchema(apiKeys).omit({
  id: true,
  createdAt: true,
});

export const insertAlertSchema = createInsertSchema(alerts).omit({
  id: true,
  createdAt: true,
});

export const insertPortStatsSchema = createInsertSchema(portStats).omit({
  id: true,
  createdAt: true,
});

export const alertSubscriptions = pgTable("alert_subscriptions", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: uuid("user_id").notNull(),
  scope: varchar("scope", { length: 20 }).notNull().default("PORT"),
  entityType: varchar("entity_type", { length: 30 }).notNull().default("port"),
  entityId: uuid("entity_id").notNull(),
  severityMin: varchar("severity_min", { length: 20 }).notNull().default("HIGH"),
  confidenceMin: varchar("confidence_min", { length: 20 }),
  minQualityBand: varchar("min_quality_band", { length: 20 }),
  minQualityScore: integer("min_quality_score"),
  channel: varchar("channel", { length: 20 }).notNull().default("WEBHOOK"),
  endpoint: text("endpoint").notNull(),
  secret: text("secret").notNull(),
  signatureVersion: varchar("signature_version", { length: 10 }).notNull().default("v1"),
  isEnabled: boolean("is_enabled").notNull().default(true),
  lastTestAt: timestamp("last_test_at", { withTimezone: true }),
  lastTestStatus: text("last_test_status"),
  lastTestError: text("last_test_error"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  tenantUserEnabledIdx: index("idx_alert_subs_tenant_user_enabled").on(table.tenantId, table.userId, table.isEnabled),
  tenantEntityIdx: index("idx_alert_subs_tenant_entity").on(table.tenantId, table.entityType, table.entityId),
  tenantUserCreatedIdx: index("alert_subscriptions_tenant_user_created_id").on(table.tenantId, table.userId, table.createdAt.desc(), table.id.desc()),
  uniqueIdx: uniqueIndex("alert_subscriptions_unique").on(table.tenantId, table.userId, table.channel, table.endpoint, table.entityId),
}));

export const alertRuns = pgTable("alert_runs", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  day: date("day"),
  startedAt: timestamp("started_at", { withTimezone: true }).defaultNow().notNull(),
  finishedAt: timestamp("finished_at", { withTimezone: true }),
  status: varchar("status", { length: 20 }).notNull(),
  summary: jsonb("summary"),
  error: jsonb("error"),
}, (table) => ({
  startedAtIdx: index("idx_alert_runs_started_at").on(table.startedAt),
  tenantStartedAtIdx: index("idx_alert_runs_tenant_started_at").on(table.tenantId, table.startedAt),
}));

export const alertDedupe = pgTable("alert_dedupe", {
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  clusterId: text("cluster_id").notNull(),
  channel: text("channel").notNull(),
  endpoint: text("endpoint").notNull(),
  lastSentAt: timestamp("last_sent_at", { withTimezone: true }).notNull(),
  ttlHours: integer("ttl_hours").notNull().default(24),
}, (table) => ({
  uniqueIdx: uniqueIndex("alert_dedupe_unique").on(table.tenantId, table.clusterId, table.channel, table.endpoint),
}));

export const alertDeliveries = pgTable("alert_deliveries", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  runId: uuid("run_id").notNull(),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: uuid("user_id").notNull(),
  subscriptionId: uuid("subscription_id").notNull(),
  clusterId: text("cluster_id").notNull(),
  entityType: text("entity_type").notNull(),
  entityId: text("entity_id").notNull(),
  day: date("day").notNull(),
  destinationType: text("destination_type").notNull(),
  endpoint: text("endpoint").notNull(),
  destinationKey: text("destination_key").notNull(),
  status: text("status").notNull(),
  skipReason: text("skip_reason"),
  isBundle: boolean("is_bundle").notNull().default(true),
  bundleSize: integer("bundle_size").notNull().default(1),
  bundleOverflow: integer("bundle_overflow").notNull().default(0),
  bundlePayload: jsonb("bundle_payload"),
  decision: jsonb("decision"),
  qualityScore: integer("quality_score"),
  qualityBand: text("quality_band"),
  qualityReasons: jsonb("quality_reasons"),
  qualityVersion: text("quality_version"),
  attempts: integer("attempts").notNull().default(0),
  lastHttpStatus: integer("last_http_status"),
  latencyMs: integer("latency_ms"),
  error: text("error"),
  sentAt: timestamp("sent_at", { withTimezone: true }),
  isTest: boolean("is_test").notNull().default(false),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  runIdx: index("alert_deliveries_run_id").on(table.runId),
  tenantUserCreatedIdx: index("alert_deliveries_tenant_user_created_id").on(table.tenantId, table.userId, table.createdAt.desc(), table.id.desc()),
  tenantDestinationCreatedIdx: index("alert_deliveries_tenant_destination_created").on(table.tenantId, table.destinationType, table.createdAt.desc()),
  tenantDestinationKeyCreatedIdx: index("alert_deliveries_tenant_destination_key_created").on(table.tenantId, table.destinationKey, table.createdAt.desc(), table.id.desc()),
  subTimeIdx: index("alert_deliveries_sub_time").on(table.subscriptionId, table.createdAt.desc()),
  dayEntityIdx: index("alert_deliveries_day_entity").on(table.day, table.entityId),
  clusterIdx: index("alert_deliveries_cluster_id").on(table.clusterId),
}));

export const alertNoiseBudgets = pgTable("alert_noise_budgets", {
  tenantId: uuid("tenant_id").notNull(),
  destinationType: text("destination_type").notNull(),
  window: text("window").notNull(),
  maxDeliveries: integer("max_deliveries").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.destinationType, table.window] }),
  tenantWindowIdx: index("alert_noise_budgets_tenant_window").on(table.tenantId, table.window),
}));

export const alertNoiseBudgetBreaches = pgTable("alert_noise_budget_breaches", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull(),
  destinationType: text("destination_type").notNull(),
  window: text("window").notNull(),
  bucketMinute: timestamp("bucket_minute", { withTimezone: true }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  uniqueIdx: uniqueIndex("alert_noise_budget_breaches_unique").on(table.tenantId, table.destinationType, table.window, table.bucketMinute),
  tenantBucketIdx: index("alert_noise_budget_breaches_tenant_bucket").on(table.tenantId, table.bucketMinute.desc()),
}));

export const alertQualityGateBreaches = pgTable("alert_quality_gate_breaches", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull(),
  subscriptionId: uuid("subscription_id").notNull(),
  day: date("day").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  uniqueIdx: uniqueIndex("alert_quality_gate_breaches_unique").on(table.tenantId, table.subscriptionId, table.day),
  tenantDayIdx: index("alert_quality_gate_breaches_tenant_day").on(table.tenantId, table.day.desc()),
}));

export const alertDeliveryAttempts = pgTable("alert_delivery_attempts", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  deliveryId: uuid("delivery_id").notNull(),
  attemptNo: integer("attempt_no").notNull(),
  status: text("status").notNull(),
  latencyMs: integer("latency_ms"),
  httpStatus: integer("http_status"),
  error: text("error"),
  sentAt: timestamp("sent_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  tenantDeliveryIdx: index("alert_delivery_attempts_tenant_delivery_id").on(table.tenantId, table.deliveryId),
  createdIdx: index("alert_delivery_attempts_created_at").on(table.createdAt),
}));

export const alertDeliverySlaWindows = pgTable("alert_delivery_sla_windows", {
  tenantId: uuid("tenant_id").notNull(),
  destinationType: text("destination_type").notNull(),
  destinationKey: text("destination_key").notNull(),
  window: text("window").notNull(),
  windowStart: timestamp("window_start", { withTimezone: true }).notNull(),
  attemptsTotal: integer("attempts_total").notNull(),
  attemptsSuccess: integer("attempts_success").notNull(),
  attemptsFailed: integer("attempts_failed").notNull(),
  latencyP50Ms: integer("latency_p50_ms").notNull(),
  latencyP95Ms: integer("latency_p95_ms").notNull(),
  successRate: decimal("success_rate", { precision: 5, scale: 2 }).notNull(),
  status: text("status").notNull(),
  computedAt: timestamp("computed_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.window, table.destinationType, table.destinationKey, table.windowStart] }),
  tenantWindowIdx: index("alert_delivery_sla_windows_tenant_window").on(table.tenantId, table.window, table.windowStart.desc()),
  tenantWindowDestKeyUpdatedIdx: index("alert_sla_windows_tenant_window_destkey_updated").on(table.tenantId, table.window, table.destinationType, table.destinationKey, table.computedAt.desc()),
  tenantWindowUpdatedIdx: index("alert_sla_windows_tenant_window_updated").on(table.tenantId, table.window, table.computedAt.desc()),
}));

export const alertEndpointHealth = pgTable("alert_endpoint_health", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull(),
  window: text("window").notNull(),
  destinationType: text("destination_type").notNull(),
  destination: text("destination").notNull(),
  status: text("status").notNull(),
  attemptsTotal: integer("attempts_total").notNull().default(0),
  attemptsSuccess: integer("attempts_success").notNull().default(0),
  successRate: doublePrecision("success_rate").notNull().default(1),
  p50Ms: integer("p50_ms"),
  p95Ms: integer("p95_ms"),
  consecutiveFailures: integer("consecutive_failures").notNull().default(0),
  lastSuccessAt: timestamp("last_success_at", { withTimezone: true }),
  lastFailureAt: timestamp("last_failure_at", { withTimezone: true }),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  uniqueIdx: uniqueIndex("alert_endpoint_health_unique").on(table.tenantId, table.window, table.destinationType, table.destination),
  tenantWindowStatusIdx: index("alert_endpoint_health_tenant_window_status").on(table.tenantId, table.window, table.status),
  tenantWindowUpdatedIdx: index("alert_endpoint_health_tenant_window_updated").on(table.tenantId, table.window, table.updatedAt.desc()),
}));

export const alertDestinationStates = pgTable("alert_destination_states", {
  tenantId: uuid("tenant_id").notNull(),
  destinationType: text("destination_type").notNull(),
  destinationKey: text("destination_key").notNull(),
  state: text("state").notNull(),
  reason: text("reason"),
  pausedByUserId: uuid("paused_by_user_id"),
  pausedAt: timestamp("paused_at", { withTimezone: true }),
  autoPausedAt: timestamp("auto_paused_at", { withTimezone: true }),
  resumeReadyAt: timestamp("resume_ready_at", { withTimezone: true }),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.destinationType, table.destinationKey] }),
  tenantStateIdx: index("alert_destination_states_tenant_state").on(table.tenantId, table.state),
  tenantDestinationStateIdx: index("alert_destination_states_tenant_dest_state").on(table.tenantId, table.destinationType, table.destinationKey, table.state),
  tenantDestinationIdx: index("alert_destination_states_tenant_destination").on(table.tenantId, table.destinationKey),
  tenantStateUpdatedIdx: index("alert_destination_states_tenant_state_updated").on(table.tenantId, table.state, table.updatedAt.desc()),
}));

export const alertDestinationOverrides = pgTable("alert_destination_overrides", {
  tenantId: uuid("tenant_id").notNull(),
  destinationKey: text("destination_key").notNull(),
  destinationType: text("destination_type").notNull(),
  noiseBudgetEnabled: boolean("noise_budget_enabled").notNull().default(true),
  noiseBudgetWindowMinutes: integer("noise_budget_window_minutes"),
  noiseBudgetMaxDeliveries: integer("noise_budget_max_deliveries"),
  slaEnabled: boolean("sla_enabled").notNull().default(true),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  updatedByUserId: uuid("updated_by_user_id"),
  updatedByKeyId: uuid("updated_by_key_id"),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.destinationKey] }),
  tenantTypeIdx: index("alert_destination_overrides_tenant_type").on(table.tenantId, table.destinationType),
}));

export const alertDestinationSlaOverrides = pgTable("alert_destination_sla_overrides", {
  tenantId: uuid("tenant_id").notNull(),
  destinationKey: text("destination_key").notNull(),
  window: text("window").notNull(),
  p95Ms: integer("p95_ms"),
  successRateMinPct: integer("success_rate_min_pct"),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
  updatedByUserId: uuid("updated_by_user_id"),
  updatedByKeyId: uuid("updated_by_key_id"),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.destinationKey, table.window] }),
  tenantWindowIdx: index("alert_destination_sla_overrides_tenant_window").on(table.tenantId, table.window),
}));

export const alertSlaThresholds = pgTable("alert_sla_thresholds", {
  tenantId: uuid("tenant_id").notNull(),
  window: text("window").notNull(),
  destinationType: text("destination_type").notNull(),
  p95MsThreshold: integer("p95_ms_threshold").notNull(),
  successRateThreshold: decimal("success_rate_threshold", { precision: 5, scale: 4 }).notNull(),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  pk: primaryKey({ columns: [table.tenantId, table.window, table.destinationType] }),
  tenantWindowIdx: index("alert_sla_thresholds_tenant_window").on(table.tenantId, table.window),
}));

export const alertDlq = pgTable("alert_dlq", {
  id: uuid("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  deliveryId: uuid("delivery_id").notNull(),
  nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true }).notNull(),
  attemptCount: integer("attempt_count").notNull().default(0),
  maxAttempts: integer("max_attempts").notNull().default(10),
  lastError: text("last_error"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow().notNull(),
}, (table) => ({
  uniqueIdx: uniqueIndex("alert_dlq_unique").on(table.deliveryId),
  nextAttemptIdx: index("alert_dlq_next_attempt").on(table.nextAttemptAt),
  tenantNextAttemptIdx: index("alert_dlq_tenant_next_attempt").on(table.tenantId, table.nextAttemptAt),
  tenantAttemptIdx: index("alert_dlq_tenant_attempt_count").on(table.tenantId, table.attemptCount),
}));

export const insertPortDailyBaselineSchema = createInsertSchema(portDailyBaselines).omit({
  id: true,
  updatedAt: true,
});

export const insertSignalSchema = createInsertSchema(signals).omit({
  id: true,
  createdAt: true,
});

export const insertPredictionSchema = createInsertSchema(predictions).omit({
  id: true,
  createdAt: true,
});

export const insertStorageFillDataSchema = createInsertSchema(storageFillData).omit({
  id: true,
  createdAt: true,
});

export const insertFloatingStorageSchema = createInsertSchema(floatingStorage).omit({
  id: true,
  lastUpdated: true,
});

export const insertSprReservesSchema = createInsertSchema(sprReserves).omit({
  id: true,
  createdAt: true,
});

export const insertStorageTimeSeriesSchema = createInsertSchema(storageTimeSeries).omit({
  id: true,
  createdAt: true,
});

export const insertPortDelayEventSchema = createInsertSchema(portDelayEvents).omit({
  id: true,
  createdAt: true,
});

export const insertVesselDelaySnapshotSchema = createInsertSchema(vesselDelaySnapshots).omit({
  id: true,
  lastUpdated: true,
});

export const insertMarketDelayImpactSchema = createInsertSchema(marketDelayImpacts).omit({
  id: true,
  createdAt: true,
});

export const insertCargoLegSchema = createInsertSchema(cargoLegs).omit({
  id: true,
  createdAt: true,
});

export const insertSTSEventSchema = createInsertSchema(stsEvents).omit({
  id: true,
  createdAt: true,
});

export const insertCargoSplitSchema = createInsertSchema(cargoSplits).omit({
  id: true,
  createdAt: true,
});

export const insertFlowForecastSchema = createInsertSchema(flowForecasts).omit({
  id: true,
  createdAt: true,
});

export const insertPortCallSchema = createInsertSchema(portCalls).omit({
  id: true,
  createdAt: true,
});

export const insertContainerOperationSchema = createInsertSchema(containerOperations).omit({
  id: true,
  createdAt: true,
});

export const insertBunkeringEventSchema = createInsertSchema(bunkeringEvents).omit({
  id: true,
  createdAt: true,
});

export const insertCommunicationSchema = createInsertSchema(communications).omit({
  id: true,
  createdAt: true,
});

export const insertCrudeGradeSchema = createInsertSchema(crudeGrades).omit({
  id: true,
  createdAt: true,
});

export const insertLngCargoSchema = createInsertSchema(lngCargoes).omit({
  id: true,
  createdAt: true,
});

export const insertDryBulkFixtureSchema = createInsertSchema(dryBulkFixtures).omit({
  id: true,
  createdAt: true,
});

export const insertPetrochemProductSchema = createInsertSchema(petrochemProducts).omit({
  id: true,
  createdAt: true,
});

export const insertAgriBiofuelFlowSchema = createInsertSchema(agriBiofuelFlows).omit({
  id: true,
  createdAt: true,
});

export const insertRefinerySchema = createInsertSchema(refineries).omit({
  id: true,
  createdAt: true,
});

export const insertSupplyDemandBalanceSchema = createInsertSchema(supplyDemandBalances).omit({
  id: true,
  createdAt: true,
});

export const insertResearchReportSchema = createInsertSchema(researchReports).omit({
  id: true,
  createdAt: true,
  lastUpdated: true,
});

// CSV-based insert schemas
export const insertRefineryUnitSchema = createInsertSchema(refineryUnits).omit({
  id: true,
  createdAt: true,
});

export const insertRefineryUtilizationDailySchema = createInsertSchema(refineryUtilizationDaily).omit({
  id: true,
  createdAt: true,
});

export const insertRefineryCrackSpreadsDailySchema = createInsertSchema(refineryCrackSpreadsDaily).omit({
  id: true,
  createdAt: true,
});

export const insertSdModelsDailySchema = createInsertSchema(sdModelsDaily).omit({
  id: true,
  createdAt: true,
});

export const insertSdForecastsWeeklySchema = createInsertSchema(sdForecastsWeekly).omit({
  id: true,
  createdAt: true,
});

export const insertResearchInsightsDailySchema = createInsertSchema(researchInsightsDaily).omit({
  id: true,
  createdAt: true,
});

// Types
export type Commodity = typeof commodities.$inferSelect;
export type InsertCommodity = z.infer<typeof insertCommoditySchema>;

export type Market = typeof markets.$inferSelect;
export type InsertMarket = z.infer<typeof insertMarketSchema>;

export type Port = typeof ports.$inferSelect;
export type InsertPort = z.infer<typeof insertPortSchema>;

export type Vessel = typeof vessels.$inferSelect;
export type InsertVessel = z.infer<typeof insertVesselSchema>;

export type VesselPosition = typeof vesselPositions.$inferSelect;
export type StorageFacility = typeof storageFacilities.$inferSelect;
export type CommodityPrice = typeof commodityPrices.$inferSelect;
export type TradeFlow = typeof tradeFlows.$inferSelect;
export type InsertTradeFlow = z.infer<typeof insertTradeFlowSchema>;

export type MarketAnalytics = typeof marketAnalytics.$inferSelect;

export type Organization = typeof organizations.$inferSelect;
export type InsertOrganization = z.infer<typeof insertOrganizationSchema>;

export type User = typeof users.$inferSelect;
export type InsertUser = z.infer<typeof insertUserSchema>;

export type ApiKey = typeof apiKeys.$inferSelect;
export type InsertApiKey = z.infer<typeof insertApiKeySchema>;

export type Alert = typeof alerts.$inferSelect;
export type InsertAlert = z.infer<typeof insertAlertSchema>;

export type Notification = typeof notifications.$inferSelect;

export type PortStats = typeof portStats.$inferSelect;
export type InsertPortStats = z.infer<typeof insertPortStatsSchema>;

export type PortDailyBaseline = typeof portDailyBaselines.$inferSelect;
export type InsertPortDailyBaseline = z.infer<typeof insertPortDailyBaselineSchema>;

export type Signal = typeof signals.$inferSelect;
export type InsertSignal = z.infer<typeof insertSignalSchema>;

export type Prediction = typeof predictions.$inferSelect;
export type InsertPrediction = z.infer<typeof insertPredictionSchema>;

export type StorageFillData = typeof storageFillData.$inferSelect;
export type InsertStorageFillData = z.infer<typeof insertStorageFillDataSchema>;

export type FloatingStorage = typeof floatingStorage.$inferSelect;
export type InsertFloatingStorage = z.infer<typeof insertFloatingStorageSchema>;

export type SprReserves = typeof sprReserves.$inferSelect;
export type InsertSprReserves = z.infer<typeof insertSprReservesSchema>;

export type StorageTimeSeries = typeof storageTimeSeries.$inferSelect;
export type InsertStorageTimeSeries = z.infer<typeof insertStorageTimeSeriesSchema>;

export type PortDelayEvent = typeof portDelayEvents.$inferSelect;
export type InsertPortDelayEvent = z.infer<typeof insertPortDelayEventSchema>;

export type VesselDelaySnapshot = typeof vesselDelaySnapshots.$inferSelect;
export type InsertVesselDelaySnapshot = z.infer<typeof insertVesselDelaySnapshotSchema>;

export type MarketDelayImpact = typeof marketDelayImpacts.$inferSelect;
export type InsertMarketDelayImpact = z.infer<typeof insertMarketDelayImpactSchema>;

// ===== ML PRICE PREDICTIONS =====

// ML-based commodity price predictions using vessel/port data
export const mlPricePredictions = pgTable("ml_price_predictions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  commodityType: varchar("commodity_type", { length: 50 }).notNull(), // crude_oil, refined_products, lng, etc.
  predictionDate: date("prediction_date").notNull(),
  targetDate: date("target_date").notNull(), // next day prediction
  predictedPrice: decimal("predicted_price", { precision: 12, scale: 4 }).notNull(),
  priceChange: decimal("price_change", { precision: 10, scale: 4 }).notNull(), // predicted change
  priceChangePercent: decimal("price_change_percent", { precision: 6, scale: 3 }).notNull(),
  confidence: decimal("confidence", { precision: 5, scale: 4 }).notNull(), // 0-1 confidence score

  // ML Features used for prediction
  features: jsonb("features").notNull(), // {vesselArrivals, avgWaitTime, cargoMix, congestionIndex, etc.}

  // Contributing factors
  vesselArrivals: integer("vessel_arrivals").notNull(), // number of vessels arriving
  avgWaitTimeHours: decimal("avg_wait_time_hours", { precision: 8, scale: 2 }).notNull(),
  bulkCarrierCount: integer("bulk_carrier_count").default(0),
  oilCarrierCount: integer("oil_carrier_count").default(0),
  lngCarrierCount: integer("lng_carrier_count").default(0),
  portCongestionIndex: decimal("port_congestion_index", { precision: 5, scale: 2 }).notNull(), // 0-100

  // Model metadata
  modelVersion: varchar("model_version", { length: 20 }).default("v1.0"),
  modelType: varchar("model_type", { length: 30 }).default("regression"), // regression, time_series, ensemble

  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

export const insertMlPricePredictionSchema = createInsertSchema(mlPricePredictions).omit({
  id: true,
  createdAt: true,
});

export type CargoLeg = typeof cargoLegs.$inferSelect;
export type InsertCargoLeg = z.infer<typeof insertCargoLegSchema>;

export type STSEvent = typeof stsEvents.$inferSelect;
export type InsertSTSEvent = z.infer<typeof insertSTSEventSchema>;

export type CargoSplit = typeof cargoSplits.$inferSelect;
export type InsertCargoSplit = z.infer<typeof insertCargoSplitSchema>;

export type FlowForecast = typeof flowForecasts.$inferSelect;
export type InsertFlowForecast = z.infer<typeof insertFlowForecastSchema>;

export type PortCall = typeof portCalls.$inferSelect;
export type InsertPortCall = z.infer<typeof insertPortCallSchema>;

export type ContainerOperation = typeof containerOperations.$inferSelect;
export type InsertContainerOperation = z.infer<typeof insertContainerOperationSchema>;

export type BunkeringEvent = typeof bunkeringEvents.$inferSelect;
export type InsertBunkeringEvent = z.infer<typeof insertBunkeringEventSchema>;

export type Communication = typeof communications.$inferSelect;
export type InsertCommunication = z.infer<typeof insertCommunicationSchema>;

export type CrudeGrade = typeof crudeGrades.$inferSelect;
export type InsertCrudeGrade = z.infer<typeof insertCrudeGradeSchema>;

export type LngCargo = typeof lngCargoes.$inferSelect;
export type InsertLngCargo = z.infer<typeof insertLngCargoSchema>;

export type DryBulkFixture = typeof dryBulkFixtures.$inferSelect;
export type InsertDryBulkFixture = z.infer<typeof insertDryBulkFixtureSchema>;

export type PetrochemProduct = typeof petrochemProducts.$inferSelect;
export type InsertPetrochemProduct = z.infer<typeof insertPetrochemProductSchema>;

export type AgriBiofuelFlow = typeof agriBiofuelFlows.$inferSelect;
export type InsertAgriBiofuelFlow = z.infer<typeof insertAgriBiofuelFlowSchema>;

export type Refinery = typeof refineries.$inferSelect;
export type InsertRefinery = z.infer<typeof insertRefinerySchema>;

export type SupplyDemandBalance = typeof supplyDemandBalances.$inferSelect;
export type InsertSupplyDemandBalance = z.infer<typeof insertSupplyDemandBalanceSchema>;

export type ResearchReport = typeof researchReports.$inferSelect;
export type InsertResearchReport = z.infer<typeof insertResearchReportSchema>;

// CSV-based data types
export type RefineryUnit = typeof refineryUnits.$inferSelect;
export type InsertRefineryUnit = z.infer<typeof insertRefineryUnitSchema>;

export type RefineryUtilizationDaily = typeof refineryUtilizationDaily.$inferSelect;
export type InsertRefineryUtilizationDaily = z.infer<typeof insertRefineryUtilizationDailySchema>;

export type RefineryCrackSpreadsDaily = typeof refineryCrackSpreadsDaily.$inferSelect;
export type InsertRefineryCrackSpreadsDaily = z.infer<typeof insertRefineryCrackSpreadsDailySchema>;

export type SdModelsDaily = typeof sdModelsDaily.$inferSelect;
export type InsertSdModelsDaily = z.infer<typeof insertSdModelsDailySchema>;

export type SdForecastsWeekly = typeof sdForecastsWeekly.$inferSelect;
export type InsertSdForecastsWeekly = z.infer<typeof insertSdForecastsWeeklySchema>;

export type ResearchInsightsDaily = typeof researchInsightsDaily.$inferSelect;
export type InsertResearchInsightsDaily = z.infer<typeof insertResearchInsightsDailySchema>;

export type MlPricePrediction = typeof mlPricePredictions.$inferSelect;
export type InsertMlPricePrediction = z.infer<typeof insertMlPricePredictionSchema>;

// ===== PRODUCTION INFRASTRUCTURE =====

// Audit Logs (security and compliance tracking)
export const auditLogs = pgTable("audit_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").references(() => users.id),
  action: varchar("action", { length: 50 }).notNull(), // login, logout, register, failed_login, password_change, data_access, data_modify
  resource: varchar("resource", { length: 100 }), // resource being accessed/modified
  resourceId: varchar("resource_id"), // ID of specific resource
  ipAddress: varchar("ip_address", { length: 50 }),
  userAgent: text("user_agent"),
  status: varchar("status", { length: 20 }).notNull(), // success, failure
  errorMessage: text("error_message"),
  metadata: jsonb("metadata"), // additional context
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
});

// Event Log (AIS message replayability and deduplication)
export const eventLogs = pgTable("event_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  eventType: varchar("event_type", { length: 50 }).notNull(), // ais_position, port_arrival, sts_detection, congestion_alert
  sourceId: varchar("source_id", { length: 100 }).notNull(), // e.g., MMSI for AIS, port code for port events
  eventHash: varchar("event_hash", { length: 64 }).notNull(), // SHA256 hash for deduplication
  sequenceNumber: integer("sequence_number"), // for ordering
  payload: jsonb("payload").notNull(), // original event data
  processedAt: timestamp("processed_at", { withTimezone: true }),
  status: varchar("status", { length: 20 }).default("pending"), // pending, processed, failed, duplicate
  retryCount: integer("retry_count").default(0),
  errorMessage: text("error_message"),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Ingestion Checkpoints (for AIS stream recovery)
export const ingestionCheckpoints = pgTable("ingestion_checkpoints", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  streamName: varchar("stream_name", { length: 50 }).notNull().unique(), // aisstream, rotterdam_api, etc
  lastOffset: varchar("last_offset", { length: 100 }), // last processed message ID/offset
  lastTimestamp: timestamp("last_timestamp", { withTimezone: true }),
  messageCount: integer("message_count").default(0),
  errorCount: integer("error_count").default(0),
  lastError: text("last_error"),
  status: varchar("status", { length: 20 }).default("active"), // active, paused, error
  metadata: jsonb("metadata"),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
});

// Watchlists (user-specific tracking)
export const watchlists = pgTable("watchlists", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: varchar("user_id").references(() => users.id).notNull(),
  name: varchar("name", { length: 100 }).notNull(),
  type: varchar("type", { length: 30 }).notNull(), // vessels, ports, commodities, routes
  isDefault: boolean("is_default").default(false),
  items: jsonb("items").notNull(), // array of IDs being watched
  alertSettings: jsonb("alert_settings"), // notification preferences
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
  updatedAt: timestamp("updated_at", { withTimezone: true }).defaultNow(),
});

// Alert Rules (user-configurable thresholds)
export const alertRules = pgTable("alert_rules", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  tenantId: uuid("tenant_id").notNull().default(sql`'00000000-0000-0000-0000-000000000001'`),
  userId: varchar("user_id").references(() => users.id).notNull(),
  name: varchar("name", { length: 100 }).notNull(),
  type: varchar("rule_type", { length: 50 }).notNull(), // price_threshold, congestion, vessel_arrival, storage_level
  isActive: boolean("is_active").default(true),
  isMuted: boolean("is_muted").default(false), // temporarily mute without disabling
  severity: varchar("severity", { length: 20 }).default("medium"), // critical, high, medium, low
  snoozedUntil: timestamp("snoozed_until", { withTimezone: true }), // snooze alerts until this time
  conditions: jsonb("conditions").notNull(), // threshold values, comparison operators
  watchlistId: varchar("watchlist_id").references(() => watchlists.id),
  channels: jsonb("channels"), // email, webhook, in_app - nullable for existing data
  cooldownMinutes: integer("cooldown_minutes").default(60), // don't re-trigger within this window
  lastTriggered: timestamp("last_triggered_at", { withTimezone: true }),
  triggerCount: integer("trigger_count").default(0),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Model Registry (ML model versioning)
export const modelRegistry = pgTable("model_registry", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  modelName: varchar("model_name", { length: 100 }).notNull(),
  version: varchar("version", { length: 20 }).notNull(),
  modelType: varchar("model_type", { length: 50 }), // regression, classification, ensemble
  features: text("features").array(), // feature names
  hyperparameters: jsonb("hyperparameters"),
  trainingMetrics: jsonb("training_metrics"), // accuracy, MAE, RMSE during training
  validationMetrics: jsonb("validation_metrics"), // metrics on validation set
  status: varchar("status", { length: 20 }).default("active"), // active, deprecated, testing
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Model Predictions with Confidence (enhanced predictions)
export const modelPredictions = pgTable("model_predictions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  modelId: varchar("model_id").references(() => modelRegistry.id).notNull(),
  target: varchar("target", { length: 100 }).notNull(), // what is being predicted
  predictionDate: timestamp("prediction_date", { withTimezone: true }).notNull(),
  horizon: varchar("horizon", { length: 20 }), // 1d, 7d, 30d
  predictedValue: decimal("predicted_value", { precision: 12, scale: 4 }).notNull(),
  confidenceLower: decimal("confidence_lower", { precision: 12, scale: 4 }),
  confidenceUpper: decimal("confidence_upper", { precision: 12, scale: 4 }),
  confidenceLevel: decimal("confidence_level", { precision: 5, scale: 4 }).default(sql`0.95`),
  actualValue: decimal("actual_value", { precision: 12, scale: 4 }), // for backtesting
  featuresUsed: jsonb("features_used"), // snapshot of input feature values
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Data Quality Scores (per-metric confidence)
export const dataQualityScores = pgTable("data_quality_scores", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  metricType: varchar("metric_type", { length: 50 }).notNull(), // congestion, storage_fill, vessel_count
  entityId: varchar("entity_id").notNull(), // port_id, vessel_id, etc.
  value: decimal("value", { precision: 12, scale: 4 }).notNull(),
  confidenceScore: decimal("confidence_score", { precision: 5, scale: 4 }).notNull(), // 0 to 1
  dataCompleteness: decimal("data_completeness", { precision: 5, scale: 4 }).notNull(), // % of expected data received
  dataFreshness: integer("data_freshness").notNull(), // seconds since last update
  outlierScore: decimal("outlier_score", { precision: 5, scale: 4 }), // how unusual is this value
  contributingSources: jsonb("contributing_sources"), // which AIS messages, API calls contributed
  methodology: text("methodology"), // how was this calculated
  timestamp: timestamp("timestamp", { withTimezone: true }).defaultNow(),
});

// ===== REFINERY SATELLITE MONITORING =====

// Refinery AOIs (Areas of Interest for satellite monitoring)
export const refineryAois = pgTable("refinery_aois", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  name: varchar("name", { length: 100 }).notNull(),
  code: varchar("code", { length: 20 }).notNull().unique(), // pernis, botlek, europoort, maasvlakte
  region: varchar("region", { length: 50 }).notNull(), // rotterdam_cluster
  description: text("description"),
  boundingBox: jsonb("bounding_box").notNull(), // { minLat, maxLat, minLon, maxLon }
  polygon: jsonb("polygon"), // array of [lat, lon] pairs for precise AOI
  facilities: jsonb("facilities"), // key facilities in this AOI
  isActive: boolean("is_active").default(true),
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Satellite Observations (individual scene metadata)
export const satelliteObservations = pgTable("satellite_observations", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  aoiId: varchar("aoi_id").references(() => refineryAois.id).notNull(),
  sceneId: varchar("scene_id", { length: 100 }).notNull(), // Sentinel-2 scene identifier
  satellite: varchar("satellite", { length: 20 }).notNull(), // sentinel-2a, sentinel-2b
  observationDate: timestamp("observation_date", { withTimezone: true }).notNull(),
  cloudCoverPercent: decimal("cloud_cover_percent", { precision: 5, scale: 2 }).notNull(),
  cloudFreeAoiPercent: decimal("cloud_free_aoi_percent", { precision: 5, scale: 2 }).notNull(),
  isUsable: boolean("is_usable").default(false), // true if cloud-free enough for analysis
  processingLevel: varchar("processing_level", { length: 10 }).notNull(), // L2A
  tileId: varchar("tile_id", { length: 20 }), // Sentinel-2 tile ID
  sunAzimuth: decimal("sun_azimuth", { precision: 6, scale: 2 }),
  sunElevation: decimal("sun_elevation", { precision: 5, scale: 2 }),
  metadata: jsonb("metadata"), // additional scene metadata
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Refinery Activity Indices (weekly computed metrics)
export const refineryActivityIndices = pgTable("refinery_activity_indices", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  aoiId: varchar("aoi_id").references(() => refineryAois.id).notNull(),
  weekStart: date("week_start").notNull(),
  weekEnd: date("week_end").notNull(),
  sceneId: varchar("scene_id", { length: 100 }), // best scene used for this week
  observationDate: timestamp("observation_date", { withTimezone: true }), // actual observation timestamp

  // Main composite index
  activityIndex: decimal("activity_index", { precision: 5, scale: 2 }).notNull(), // 0-100 scale
  confidence: decimal("confidence", { precision: 5, scale: 2 }).notNull(), // 0-100 scale

  // Individual signal indices
  swirAnomalyIndex: decimal("swir_anomaly_index", { precision: 5, scale: 2 }), // flaring/combustion proxy
  plumeIndex: decimal("plume_index", { precision: 5, scale: 2 }), // steam/plume visibility
  surfaceChangeIndex: decimal("surface_change_index", { precision: 5, scale: 2 }), // industrial surface changes

  // Cloud coverage for this observation
  cloudFreePercent: decimal("cloud_free_percent", { precision: 5, scale: 2 }),

  // Baseline comparisons
  baselineActivityIndex: decimal("baseline_activity_index", { precision: 5, scale: 2 }), // 4-week rolling avg
  activityTrend: varchar("activity_trend", { length: 20 }), // increasing, decreasing, stable, anomaly

  // Quality metadata
  dataSource: varchar("data_source", { length: 50 }).default("sentinel-2"), // sentinel-2, sentinel-1, combined
  methodology: varchar("methodology", { length: 50 }).default("optical"), // optical, sar, combined

  metadata: jsonb("metadata"), // additional analysis details
  createdAt: timestamp("created_at", { withTimezone: true }).defaultNow(),
});

// Insert schemas for new tables
export const insertAuditLogSchema = createInsertSchema(auditLogs).omit({ id: true, timestamp: true });
export const insertEventLogSchema = createInsertSchema(eventLogs).omit({ id: true, createdAt: true });
export const insertIngestionCheckpointSchema = createInsertSchema(ingestionCheckpoints).omit({ id: true, updatedAt: true });
export const insertWatchlistSchema = createInsertSchema(watchlists).omit({ id: true, tenantId: true, createdAt: true, updatedAt: true });
export const insertAlertRuleSchema = createInsertSchema(alertRules).omit({ id: true, tenantId: true, createdAt: true });
export const insertModelRegistrySchema = createInsertSchema(modelRegistry).omit({ id: true, createdAt: true });
export const insertModelPredictionSchema = createInsertSchema(modelPredictions).omit({ id: true, createdAt: true });
export const insertDataQualityScoreSchema = createInsertSchema(dataQualityScores).omit({ id: true, timestamp: true });

// Types for new tables
export type AuditLog = typeof auditLogs.$inferSelect;
export type InsertAuditLog = z.infer<typeof insertAuditLogSchema>;

export type EventLog = typeof eventLogs.$inferSelect;
export type InsertEventLog = z.infer<typeof insertEventLogSchema>;

export type IngestionCheckpoint = typeof ingestionCheckpoints.$inferSelect;
export type InsertIngestionCheckpoint = z.infer<typeof insertIngestionCheckpointSchema>;

export type Watchlist = typeof watchlists.$inferSelect;
export type InsertWatchlist = z.infer<typeof insertWatchlistSchema>;

export type AlertRule = typeof alertRules.$inferSelect;
export type InsertAlertRule = z.infer<typeof insertAlertRuleSchema>;

export type ModelRegistryEntry = typeof modelRegistry.$inferSelect;
export type InsertModelRegistryEntry = z.infer<typeof insertModelRegistrySchema>;

export type ModelPrediction = typeof modelPredictions.$inferSelect;
export type InsertModelPrediction = z.infer<typeof insertModelPredictionSchema>;

export type DataQualityScore = typeof dataQualityScores.$inferSelect;
export type InsertDataQualityScore = z.infer<typeof insertDataQualityScoreSchema>;

// Refinery Satellite Monitoring schemas and types
export const insertRefineryAoiSchema = createInsertSchema(refineryAois).omit({ id: true, createdAt: true });
export const insertSatelliteObservationSchema = createInsertSchema(satelliteObservations).omit({ id: true, createdAt: true });
export const insertRefineryActivityIndexSchema = createInsertSchema(refineryActivityIndices).omit({ id: true, createdAt: true });

export type RefineryAoi = typeof refineryAois.$inferSelect;
export type InsertRefineryAoi = z.infer<typeof insertRefineryAoiSchema>;

export type SatelliteObservation = typeof satelliteObservations.$inferSelect;
export type InsertSatelliteObservation = z.infer<typeof insertSatelliteObservationSchema>;

export type RefineryActivityIndex = typeof refineryActivityIndices.$inferSelect;
export type InsertRefineryActivityIndex = z.infer<typeof insertRefineryActivityIndexSchema>;
