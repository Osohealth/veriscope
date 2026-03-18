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

  const subject = `[Veriscope] ${clusterSeverity} ${clusterType} — ${entityName} — ${day}`;

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

export const sendEmail = async (args: { to: string; subject: string; text: string }) => {
  // MVP: stub sender for dev/test. Swap with SMTP provider later.
  return {
    ok: true,
    provider: "stub",
    to: args.to,
    subject: args.subject,
  };
};
