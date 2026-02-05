import { buildSignalClusterAlertPayload } from "./signalAlertService";
import { createHmac, createHash } from "node:crypto";
import { WEBHOOK_RETRY_ATTEMPTS, WEBHOOK_TIMEOUT_MS } from "../config/alerting";

type WebhookSendArgs = {
  endpoint: string;
  body: string;
  headers?: Record<string, string>;
  timeoutMs?: number;
  attempts?: number;
};

type WebhookPayloadOptions = {
  version?: string;
  sentAt?: Date;
};

export class WebhookSendError extends Error {
  attempts: number;
  lastStatus?: number;

  constructor(message: string, attempts: number, lastStatus?: number) {
    super(message);
    this.name = "WebhookSendError";
    this.attempts = attempts;
    this.lastStatus = lastStatus;
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export const computeIdempotencyKey = (subscriptionId: string, clusterId: string, day: string) => {
  const raw = `${subscriptionId}|${clusterId}|${day}`;
  return createHash("sha1").update(raw).digest("hex");
};

export const buildWebhookPayload = (signalDto: any, options: WebhookPayloadOptions = {}) => {
  const base = buildSignalClusterAlertPayload(signalDto);
  return {
    ...base,
    payload_version: options.version ?? "1.1",
    sent_at: (options.sentAt ?? new Date()).toISOString(),
  };
};

export const buildWebhookRequest = (args: {
  payload: Record<string, any>;
  secret?: string | null;
  subscriptionId: string;
  clusterId: string;
  day: string;
  now: Date;
}) => {
  const idempotencyKey = computeIdempotencyKey(args.subscriptionId, args.clusterId, args.day);
  const payload = {
    ...args.payload,
    idempotency_key: idempotencyKey,
  };
  const body = JSON.stringify(payload);
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    "Idempotency-Key": idempotencyKey,
  };
  if (args.secret) {
    const timestamp = Math.floor(args.now.getTime() / 1000).toString();
    const signatureInput = `v1:${timestamp}:${body}`;
    const signature = createHmac("sha256", args.secret).update(signatureInput).digest("hex");
    headers["X-Veriscope-Timestamp"] = timestamp;
    headers["X-Veriscope-Signature"] = `v1=${signature}`;
  }
  return { body, headers, idempotencyKey };
};

export async function sendWebhook(args: WebhookSendArgs) {
  const timeoutMs = args.timeoutMs ?? WEBHOOK_TIMEOUT_MS;
  const attempts = args.attempts ?? WEBHOOK_RETRY_ATTEMPTS;
  const backoffs = [0, 250, 1000];
  let lastStatus: number | undefined;
  let lastError: Error | undefined;
  const attemptLogs: { attempt: number; status: string; latency_ms: number; http_status?: number }[] = [];

  for (let i = 0; i < attempts; i += 1) {
    if (i > 0) {
      await sleep(backoffs[Math.min(i, backoffs.length - 1)]);
    }
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    const start = Date.now();
    try {
      const response = await fetch(args.endpoint, {
        method: "POST",
        headers: args.headers ?? { "Content-Type": "application/json" },
        body: args.body,
        signal: controller.signal,
      });
      clearTimeout(timeout);
      lastStatus = response.status;
      if (response.ok) {
        attemptLogs.push({
          attempt: i + 1,
          status: "SUCCESS",
          latency_ms: Date.now() - start,
          http_status: response.status,
        });
        return { ok: true, status: response.status, attemptLogs };
      }
      attemptLogs.push({
        attempt: i + 1,
        status: "FAILED",
        latency_ms: Date.now() - start,
        http_status: response.status,
      });
      lastError = new Error(`Webhook response ${response.status}`);
    } catch (error: any) {
      clearTimeout(timeout);
      attemptLogs.push({
        attempt: i + 1,
        status: "FAILED",
        latency_ms: Date.now() - start,
      });
      lastError = error;
    }
  }

  const err = new WebhookSendError(
    lastError?.message ?? "Webhook delivery failed",
    attempts,
    lastStatus,
  );
  (err as any).attemptLogs = attemptLogs;
  throw err;
}
