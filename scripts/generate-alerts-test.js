const fs = require('fs');
const srcPath = 'scripts/signal-engine.test.ts';
const outPath = 'scripts/tests/alerts.test.ts';
const lines = fs.readFileSync(srcPath, 'utf8').split(/\r?\n/);

const helperStart = lines.findIndex((line) => line.startsWith('const stableStringify'));
const helperEnd = lines.findIndex((line) => line.startsWith('async function main'));
if (helperStart === -1 || helperEnd === -1) {
  throw new Error('Could not locate helper block in signal-engine.test.ts');
}
const helpers = lines.slice(helperStart, helperEnd).join('\n');

const bodyStart = lines.findIndex((line) => line.includes('const debug = process.env.TEST_DEBUG'));
const bodyEnd = lines.findIndex((line) => line.includes('tenant isolation for endpoint health'));
if (bodyStart === -1 || bodyEnd === -1) {
  throw new Error('Could not locate alerts block range in signal-engine.test.ts');
}
const bodyLines = lines.slice(bodyStart, bodyEnd + 1).map((line) => line.replace(/^  /, ''));
const body = bodyLines.map((line) => `    ${line}`).join('\n');

const imports = `import test from "node:test";
import { strict as assert } from "node:assert";
import { randomUUID, createHmac } from "node:crypto";
import { and, eq, inArray, sql } from "drizzle-orm";
import { db } from "../../server/db";
import {
  alertDedupe,
  alertDeliveries,
  alertDeliveryAttempts,
  alertDeliverySlaWindows,
  alertDestinationStates,
  alertDlq,
  alertEndpointHealth,
  alertNoiseBudgetBreaches,
  alertNoiseBudgets,
  alertQualityGateBreaches,
  alertRuns,
  alertSlaThresholds,
  alertSubscriptions,
  apiKeys,
  auditEvents,
  auditExports,
  incidentEscalationPolicies,
  incidentEscalations,
  incidents,
  signals,
  tenantSettings,
  tenantUsers,
  userContactMethods,
} from "@shared/schema";
import { validateAlertSubscriptionInput } from "../../server/services/alertSubscriptionService";
import { markAlertSent, shouldSendAlert } from "../../server/services/alertDedupe";
import { getAlertCandidates } from "../../server/services/alertQuery";
import { renderAlertEmail } from "../../server/services/emailSender";
import { getDeliveryHealthByDay, getDeliveryLatency, getEndpointHealth, getDlqHealth, getDlqOverdue } from "../../server/services/alertMetrics";
import { getAlertDeliveriesSummary, listAlertDeliveries } from "../../server/services/alertDeliveries";
import { getAlertSubscriptionsSummary, listAlertSubscriptionsPage } from "../../server/services/alertSubscriptions";
import { getAuditEventsSummary, listAuditEvents } from "../../server/services/auditEvents";
import { auditEventsToCsv, auditEventsToJsonl, createAuditExport, fetchAuditEventsForExport, getAuditExport, signExportToken, verifyExportToken } from "../../server/services/auditExport";
import { purgeAuditEventsForTenant } from "../../server/services/auditRetention";
import { hashApiKey } from "../../server/services/apiKeyService";
import { retryDeliveryById } from "../../server/services/alertDlqQueue";
import { authenticateApiKey } from "../../server/middleware/apiKeyAuth";
import { requireRole as requireAlertRole } from "../../server/auth/requireRole";
import { writeAuditEvent } from "../../server/services/auditLog";
import { applyRateLimit } from "../../server/middleware/rateLimitMiddleware";
import { getTenantSettings, upsertTenantSettings } from "../../server/services/tenantSettings";
import { backfillAlertDeliverySlaWindows, computeAlertDeliverySlaWindows, getAlertSlaSummary, getSlaThresholds, listAlertSlaWindows } from "../../server/services/alertSlaService";
import { listAlertNoiseBudgets, recordNoiseBudgetBreachOnce } from "../../server/services/alertNoiseBudgetService";
import { resolveNoiseBudget, resolveSlaThresholds, upsertDestinationOverrides } from "../../server/services/alertDestinationOverridesService";
import { computeAlertQuality } from "../../server/services/alertQualityService";
import { computeEndpointHealth, listEndpointHealth } from "../../server/services/alertEndpointHealthService";
import { getIncidentMetricsV1 } from "../../server/services/incidentMetrics";
import { applyAutoPauseFromEndpointHealth, bulkUpdateDestinationStates, canTransitionDestinationState, getDestinationDetail, listDestinations } from "../../server/services/alertDestinationStateService";
import { getAlertPlaybook } from "@shared/alertPlaybook";
import { getIncidentById, listIncidents } from "../../server/services/incidentService";
import { autoAckIncidents, autoResolveIncidents, runIncidentAutomation } from "../../server/services/incidentAutomationService";
import { getIncidentEscalationSnapshot, listIncidentEscalationPolicies, runIncidentEscalations, upsertIncidentEscalationPolicy, validateEscalationPolicyTarget } from "../../server/services/incidentEscalationService";
import { validateRoutingPolicyDraft } from "../../server/services/alertRoutingValidationService";
import { getRoutingHealthForPolicy } from "../../server/services/alertRoutingHealthService";
import { makeDestinationKey } from "../../server/services/destinationKey";
import { createUserContactMethod } from "../../server/services/userContactMethodsService";
import { GLOBAL_SCOPE_ENTITY_ID } from "../../server/services/alertScope";
import { listTeamUsersDirectory } from "../../server/services/teamUsersDirectory";
import {
  applyTestEnv,
  cleanDatabase,
  enablePgCrypto,
  ensureTestSchema,
  insertApiKeys,
  insertTenantUsers,
  releaseAdvisoryLock,
  tryAdvisoryLock,
  withTransientRetry,
  createTestContext,
} from "../bootstrap";`;

const content = `${imports}\n\n${helpers}\n\ntest.describe("alerts", () => {\n  const ctx = createTestContext();\n\n  test.before(async () => {\n    await ctx.start();\n    await ctx.reset();\n  });\n\n  test.after(async () => {\n    await ctx.stop();\n  });\n\n  test("alerts pipeline", async () => {\n${body}\n  });\n});\n`;

fs.writeFileSync(outPath, content);
console.log(`Wrote ${outPath}`);
