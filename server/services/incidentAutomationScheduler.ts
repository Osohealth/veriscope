import { INCIDENT_AUTOMATION_ENABLED, INCIDENT_AUTOMATION_INTERVAL_MS } from "../config/alerting";
import { runIncidentAutomation } from "./incidentAutomationService";

let running = false;
let lastRunAt: Date | null = null;
let lastSuccessAt: Date | null = null;
let lastErrorAt: Date | null = null;
let lastErrorMessage: string | null = null;

export function getIncidentAutomationHealth() {
  return {
    enabled: INCIDENT_AUTOMATION_ENABLED,
    interval_ms: INCIDENT_AUTOMATION_INTERVAL_MS,
    running,
    last_run_at: lastRunAt,
    last_success_at: lastSuccessAt,
    last_error_at: lastErrorAt,
    last_error_message: lastErrorMessage,
  };
}

export function startIncidentAutomationScheduler() {
  if (!INCIDENT_AUTOMATION_ENABLED) return;

  setInterval(async () => {
    if (running) return;
    running = true;
    lastRunAt = new Date();

    try {
      await runIncidentAutomation();
      lastSuccessAt = new Date();
      lastErrorMessage = null;
    } catch (err: any) {
      lastErrorAt = new Date();
      lastErrorMessage = err?.message ?? "Unknown error";
      console.error("Incident automation error:", err);
    } finally {
      running = false;
    }
  }, INCIDENT_AUTOMATION_INTERVAL_MS);
}
