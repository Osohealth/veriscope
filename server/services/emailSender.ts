import { METRIC_LABELS } from "@shared/metrics";

type EmailDriver = {
  metric: string;
  value?: number;
  baseline?: number | null;
  zscore?: number | null;
  delta_pct?: number | null;
};

type EmailEntity = {
  name?: string | null;
};

type EmailRenderArgs = {
  signal: any;
  entity?: EmailEntity | null;
  link?: string | null;
};

export const renderAlertEmail = ({ signal, entity, link }: EmailRenderArgs) => {
  const clusterSeverity = signal.clusterSeverity ?? signal.severity ?? "UNKNOWN";
  const clusterType = signal.clusterType ?? "ALERT";
  const day = signal.day instanceof Date ? signal.day.toISOString().slice(0, 10) : String(signal.day ?? "");
  const entityName = entity?.name ?? "Unknown entity";

  const subject = `[Veriscope] ${clusterSeverity} ${clusterType} - ${entityName} - ${day}`;

  const summary = signal.clusterSummary ?? signal.explanation ?? "";
  const drivers: EmailDriver[] = signal.metadata?.drivers ?? [];
  const impact: string[] = signal.metadata?.impact ?? [];
  const followups: string[] = signal.metadata?.recommended_followups ?? [];

  const driverLines = drivers.map((driver) => {
    const label = METRIC_LABELS[driver.metric] ?? driver.metric;
    const delta = driver.delta_pct !== undefined && driver.delta_pct !== null ? ` Δ ${driver.delta_pct.toFixed(1)}%` : "";
    const z = driver.zscore !== undefined && driver.zscore !== null ? ` z=${driver.zscore.toFixed(2)}` : "";
    return `- ${label}${delta}${z}`;
  });

  const bodyLines = [
    summary,
    "",
    "Top drivers:",
    ...driverLines,
    "",
    "Impact:",
    ...impact.map((line) => `- ${line}`),
    "",
    "Follow-ups:",
    ...followups.map((line) => `- ${line}`),
  ];

  if (link) {
    bodyLines.push("", `View: ${link}`);
  }

  return {
    subject,
    text: bodyLines.join("\n"),
  };
};



export const renderAlertBundleEmail = ({ payload }: { payload: any }) => {
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const primary = items[0] ?? {};
  const overflow = Number(payload?.summary?.overflow ?? 0);
  const day = primary.day ?? payload?.summary?.day ?? "";
  const severity = primary.severity ?? "ALERT";
  const type = primary.cluster_type ?? "Alert";
  const entityName =
    primary.entity?.name ??
    primary.entity?.code ??
    primary.entity?.unlocode ??
    payload?.subscription?.entity?.name ??
    "All ports";
  const overflowLabel = overflow > 0 ? ` +${overflow} more` : "";
  const dayLabel = day ? ` (${day})` : "";
  const subject = `[Veriscope] ${severity} ${type}${overflowLabel}${dayLabel}`;

  const summary = payload?.summary ?? {};
  const lines = [
    `Destination: ${payload?.subscription?.destination_type ?? ""} ${payload?.subscription?.destination ?? ""}`.trim(),
    `Scope: ${payload?.subscription?.scope ?? ""}`.trim(),
    `Entity: ${entityName}`.trim(),
    `Matched: ${summary.matched_total ?? items.length}`,
    `Sent items: ${summary.sent_items ?? items.length}`,
    `Overflow: ${summary.overflow ?? overflow}`,
    "",
    "Top alerts:",
  ];

  items.forEach((item: any, index: number) => {
    const label = item.cluster_summary ?? item.cluster_type ?? "Alert";
    const entity = item.entity?.name ?? item.entity?.code ?? item.entity?.unlocode ?? "";
    const entitySuffix = entity ? ` - ${entity}` : "";
    lines.push(`${index + 1}. ${item.severity ?? "ALERT"} ${label}${entitySuffix}`);
  });

  if (overflow > 0) {
    lines.push(`+ ${overflow} more alerts not shown`);
  }

  return {
    subject,
    text: lines.join("\n"),
  };
};
export const sendEmail = async (args: { to: string; subject: string; text: string }) => {
  // MVP: stub sender for dev/test. Swap with SMTP provider later.
  return {
    ok: true,
    provider: "stub",
    to: args.to,
    subject: args.subject,
  };
};
