import {
  bigint,
  boolean,
  doublePrecision,
  index,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from "drizzle-orm/pg-core";

export const users = pgTable("users", {
  id: uuid("id").defaultRandom().primaryKey(),
  email: text("email").notNull().unique(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
});

export const ports = pgTable("ports", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  countryCode: text("country_code").notNull(),
  lat: doublePrecision("lat").notNull(),
  lon: doublePrecision("lon").notNull(),
  geofenceRadiusKm: doublePrecision("geofence_radius_km").notNull(),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
}, (table) => ({
  nameCountryUniqueIdx: uniqueIndex("ports_name_country_unique").on(
    table.name,
    table.countryCode,
  ),
}));

export const vessels = pgTable("vessels", {
  id: uuid("id").defaultRandom().primaryKey(),
  name: text("name").notNull(),
  mmsi: text("mmsi"),
  imo: text("imo"),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
}, (table) => ({
  mmsiUniqueIdx: uniqueIndex("vessels_mmsi_unique").on(table.mmsi),
  imoUniqueIdx: uniqueIndex("vessels_imo_unique").on(table.imo),
}));

export const aisPositions = pgTable(
  "ais_positions",
  {
    id: bigint("id", { mode: "number" }).primaryKey().generatedAlwaysAsIdentity(),
    vesselId: uuid("vessel_id")
      .notNull()
      .references(() => vessels.id, { onDelete: "cascade" }),
    timestampUtc: timestamp("timestamp_utc", { withTimezone: true }).notNull(),
    lat: doublePrecision("lat").notNull(),
    lon: doublePrecision("lon").notNull(),
    speed: doublePrecision("speed"),
    course: doublePrecision("course"),
    heading: doublePrecision("heading"),
    navStatus: text("nav_status"),
    source: text("source").notNull(),
  },
  (table) => ({
    timestampIdx: index("ais_positions_timestamp_utc_desc").on(
      table.timestampUtc.desc(),
    ),
    vesselTimestampIdx: index("ais_positions_vessel_timestamp_desc").on(
      table.vesselId,
      table.timestampUtc.desc(),
    ),
  }),
);

export const portCalls = pgTable(
  "port_calls",
  {
    id: uuid("id").defaultRandom().primaryKey(),
    vesselId: uuid("vessel_id")
      .notNull()
      .references(() => vessels.id, { onDelete: "cascade" }),
    portId: uuid("port_id")
      .notNull()
      .references(() => ports.id, { onDelete: "cascade" }),
    arrivalTimeUtc: timestamp("arrival_time_utc", { withTimezone: true }).notNull(),
    departureTimeUtc: timestamp("departure_time_utc", { withTimezone: true }),
    firstPositionId: bigint("first_position_id", { mode: "number" }).references(
      () => aisPositions.id,
    ),
    lastPositionId: bigint("last_position_id", { mode: "number" }).references(
      () => aisPositions.id,
    ),
  },
  (table) => ({
    portArrivalIdx: index("port_calls_port_arrival_desc").on(
      table.portId,
      table.arrivalTimeUtc.desc(),
    ),
  }),
);

export const sessions = pgTable("sessions", {
  id: uuid("id").defaultRandom().primaryKey(),
  userId: uuid("user_id")
    .notNull()
    .references(() => users.id, { onDelete: "cascade" }),
  createdAt: timestamp("created_at", { withTimezone: true })
    .notNull()
    .defaultNow(),
  expiresAt: timestamp("expires_at", { withTimezone: true }).notNull(),
});

export const vesselPortState = pgTable(
  "vessel_port_state",
  {
    vesselId: uuid("vessel_id")
      .notNull()
      .references(() => vessels.id, { onDelete: "cascade" })
      .primaryKey(),
    inPort: boolean("in_port").notNull().default(false),
    currentPortId: uuid("current_port_id").references(() => ports.id),
    currentPortCallId: uuid("current_port_call_id").references(() => portCalls.id),
    lastPositionTimeUtc: timestamp("last_position_time_utc", { withTimezone: true }),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => ({
    currentPortIdx: index("vessel_port_state_current_port_idx").on(
      table.currentPortId,
    ),
    inPortIdx: index("vessel_port_state_in_port_idx").on(table.inPort),
  }),
);
