import { useEffect, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Skeleton } from "@/components/ui/skeleton";
import { apiFetchJson, getApiKey } from "@/lib/apiFetch";
import { ArrowLeft } from "lucide-react";
import { Link } from "wouter";

type HealthResponse = { status: string; [key: string]: any };
type MetricsResponse = { version: string; [key: string]: any };

export default function AlertsHealthPage() {
  const [alertsHealth, setAlertsHealth] = useState<HealthResponse | null>(null);
  const [webhooksHealth, setWebhooksHealth] = useState<HealthResponse | null>(null);
  const [dlqHealth, setDlqHealth] = useState<MetricsResponse | null>(null);
  const [metrics, setMetrics] = useState<MetricsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [apiKey, setApiKey] = useState(() => (getApiKey() ?? ""));

  useEffect(() => {
    const controller = new AbortController();
    const load = async () => {
      setLoading(true);
      try {
        const headers = apiKey ? { Authorization: `Bearer ${apiKey}` } : undefined;
        const [alertsRes, webhooksRes, dlqRes, metricsRes] = await Promise.all([
          apiFetchJson("/health/alerts", { signal: controller.signal, headers }),
          apiFetchJson("/health/webhooks", { signal: controller.signal, headers }),
          apiFetchJson("/api/alerts/dlq-health", { signal: controller.signal, headers }),
          apiFetchJson("/api/alerts/metrics?days=30", { signal: controller.signal, headers }),
        ]);
        setAlertsHealth(alertsRes);
        setWebhooksHealth(webhooksRes);
        setDlqHealth(dlqRes);
        setMetrics(metricsRes);
      } catch {
        setAlertsHealth(null);
        setWebhooksHealth(null);
        setDlqHealth(null);
        setMetrics(null);
      } finally {
        setLoading(false);
      }
    };
    load();
    return () => controller.abort();
  }, [apiKey]);

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-6">
          <div className="flex items-center gap-3">
            <Link href="/platform">
              <Button variant="ghost" size="sm">
                <ArrowLeft className="mr-2 h-4 w-4" />
                Back to Menu
              </Button>
            </Link>
          </div>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Alert Ops Health</h1>
          <p className="text-sm text-muted-foreground">
            Live health and metrics for alerting reliability.
          </p>
        </div>
      </div>

      <div className="container mx-auto px-6 py-8 space-y-6">
        {import.meta.env.MODE !== "production" && (
          <Card className="border-border/60 bg-card/70">
            <CardContent className="p-5 space-y-3">
              <p className="text-sm font-semibold text-foreground">Dev API Key</p>
              <div className="flex flex-col gap-3 md:flex-row md:items-center">
                <Input
                  placeholder="Paste API key (vs_demo_...)"
                  value={apiKey}
                  onChange={(event) => setApiKey(event.target.value)}
                />
                <Button
                  onClick={() => {
                    if (typeof window !== "undefined") {
                      window.localStorage.setItem("api_key", apiKey);
                    }
                  }}
                >
                  Save key
                </Button>
                <Button
                  variant="outline"
                  onClick={() => {
                    if (typeof window !== "undefined") {
                      window.localStorage.removeItem("api_key");
                      setApiKey("");
                    }
                  }}
                >
                  Clear
                </Button>
              </div>
            </CardContent>
          </Card>
        )}
        {[{
          title: "/health/alerts",
          payload: alertsHealth,
        }, {
          title: "/health/webhooks",
          payload: webhooksHealth,
        }, {
          title: "/api/alerts/dlq-health",
          payload: dlqHealth,
        }, {
          title: "/api/alerts/metrics?days=30",
          payload: metrics,
        }].map((section) => (
          <Card key={section.title} className="border-border/60 bg-card/70">
            <CardContent className="p-5">
              <p className="text-sm font-semibold text-foreground mb-3">{section.title}</p>
              {loading ? (
                <Skeleton className="h-24 w-full" />
              ) : section.payload ? (
                <pre className="whitespace-pre-wrap text-xs text-muted-foreground">
                  {JSON.stringify(section.payload, null, 2)}
                </pre>
              ) : (
                <p className="text-sm text-muted-foreground">No data available.</p>
              )}
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
}
