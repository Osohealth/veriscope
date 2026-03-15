import { randomUUID } from "node:crypto";
import {
  applyTestEnv,
  cleanDatabase,
  closePool,
  deterministicNow,
  enablePgCrypto,
  ensureAlertingOnlySchema,
  ensureEscalationsOnlySchema,
  ensureTestSchema,
} from "./bootstrap";

type CleanupMode = "tenant" | "full" | "none";
type SchemaMode = "full" | "escalations" | "alerts";

type HarnessOptions = {
  cleanup?: CleanupMode;
  label?: string;
  schema?: SchemaMode;
};

type Harness = {
  beforeAll: () => Promise<void>;
  afterAll: () => Promise<void>;
  beforeEach: (testContext?: { name?: string }) => Promise<void>;
  afterEach: () => Promise<void>;
  registerTenant: (tenantId: string) => void;
  registerTenants: (tenantIds: string[]) => void;
  registerPortIds: (portIds: string[]) => void;
  registerMissingPortId: (portId: string) => void;
  newTenantId: () => string;
  now: (offsetMinutes?: number, baseIso?: string) => Date;
};

export function createTestHarness(options: HarnessOptions = {}): Harness {
  const cleanupMode = options.cleanup ?? "tenant";
  const label = options.label ?? "suite";
  const schemaMode = options.schema ?? "full";
  const tenantIds = new Set<string>();
  const portIds = new Set<string>();
  let missingPortId: string | null = null;
  const baseEnv = { ...process.env };
  const baseDateNow = Date.now;
  const baseFetch = globalThis.fetch;
  const timingEnabled = process.env.TEST_TIMING === "1";
  let testStart: bigint | null = null;
  let testName = "";

  const log = (message: string) => {
    if (process.env.TEST_DEBUG === "1") {
      console.log(`TEST_STEP: ${label} ${message}`);
    }
  };
  const logTiming = (message: string) => {
    if (timingEnabled) {
      console.log(`TEST_TIMING: ${label} ${message}`);
    }
  };

  const restoreEnv = () => {
    for (const key of Object.keys(process.env)) {
      if (!(key in baseEnv)) {
        delete process.env[key];
      }
    }
    for (const [key, value] of Object.entries(baseEnv)) {
      if (value === undefined) continue;
      process.env[key] = value;
    }
  };

  return {
    registerTenant: (tenantId: string) => {
      tenantIds.add(tenantId);
    },
    registerTenants: (ids: string[]) => {
      for (const id of ids) tenantIds.add(id);
    },
    registerPortIds: (ids: string[]) => {
      for (const id of ids) portIds.add(id);
    },
    registerMissingPortId: (id: string) => {
      missingPortId = id;
    },
    newTenantId: () => {
      const id = randomUUID();
      tenantIds.add(id);
      return id;
    },
    now: (offsetMinutes = 0, baseIso = "2026-01-01T00:00:00Z") =>
      deterministicNow(offsetMinutes, baseIso),
    async beforeAll() {
      log("beforeAll");
      const start = timingEnabled ? process.hrtime.bigint() : null;
      applyTestEnv();
      if (schemaMode === "escalations") {
        process.env.TEST_SCHEMA_PROFILE = "escalations";
      } else if (schemaMode === "alerts") {
        process.env.TEST_SCHEMA_PROFILE = "alerts";
      } else {
        delete process.env.TEST_SCHEMA_PROFILE;
      }
      await enablePgCrypto();
      if (schemaMode === "escalations") {
        await ensureEscalationsOnlySchema();
      } else if (schemaMode === "alerts") {
        await ensureAlertingOnlySchema();
      } else {
        await ensureTestSchema();
      }
      if (start) {
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
        logTiming(`beforeAll ${elapsedMs.toFixed(1)}ms`);
      }
    },
    async beforeEach(testContext) {
      log("beforeEach");
      const start = timingEnabled ? process.hrtime.bigint() : null;
      tenantIds.clear();
      portIds.clear();
      missingPortId = null;
      restoreEnv();
      applyTestEnv();
      Date.now = baseDateNow;
      if (globalThis.fetch !== baseFetch) {
        globalThis.fetch = baseFetch;
      }
      if (timingEnabled) {
        testStart = process.hrtime.bigint();
        testName = testContext?.name ?? "";
      }
      if (start) {
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
        logTiming(`beforeEach ${elapsedMs.toFixed(1)}ms`);
      }
    },
    async afterEach() {
      log("afterEach");
      const start = timingEnabled ? process.hrtime.bigint() : null;
      if (timingEnabled && testStart) {
        const elapsedMs = Number(process.hrtime.bigint() - testStart) / 1_000_000;
        logTiming(`test ${testName || "<unnamed>"} ${elapsedMs.toFixed(1)}ms`);
      }
      try {
        if (cleanupMode === "full") {
          await cleanDatabase();
        } else if (cleanupMode === "tenant") {
          const ids = Array.from(tenantIds);
          const portList = Array.from(portIds);
          const ctx: { tenantIds?: string[]; portIds?: string[]; missingPortId?: string } = {};
          if (ids.length > 0) ctx.tenantIds = ids;
          if (portList.length > 0) ctx.portIds = portList;
          if (missingPortId) ctx.missingPortId = missingPortId;
          if (Object.keys(ctx).length > 0) {
            await cleanDatabase(ctx);
          } else if (ids.length === 0) {
            await cleanDatabase();
          }
        }
      } finally {
        tenantIds.clear();
        portIds.clear();
        missingPortId = null;
        Date.now = baseDateNow;
        if (globalThis.fetch !== baseFetch) {
          globalThis.fetch = baseFetch;
        }
        restoreEnv();
        if (start) {
          const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
          logTiming(`afterEach ${elapsedMs.toFixed(1)}ms`);
        }
        testStart = null;
        testName = "";
      }
    },
    async afterAll() {
      log("afterAll");
      const start = timingEnabled ? process.hrtime.bigint() : null;
      await closePool();
      if (start) {
        const elapsedMs = Number(process.hrtime.bigint() - start) / 1_000_000;
        logTiming(`afterAll ${elapsedMs.toFixed(1)}ms`);
      }
    },
  };
}
