import { useEffect, useMemo, useState } from "react";
import { useLocation, Link } from "wouter";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useToast } from "@/hooks/use-toast";
import { apiFetchJson } from "@/lib/apiFetch";

type AcceptResponse = {
  version: string;
  user_id: string;
  api_key: string;
  api_key_id: string;
};

const parseToken = (location: string) => {
  const search = location.split("?")[1] ?? "";
  const params = new URLSearchParams(search);
  return params.get("token") ?? "";
};

export default function InviteAcceptPage() {
  const { toast } = useToast();
  const [location] = useLocation();
  const tokenFromUrl = useMemo(() => parseToken(location), [location]);
  const [tokenInput, setTokenInput] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [loading, setLoading] = useState(false);
  const [accepted, setAccepted] = useState<AcceptResponse | null>(null);

  useEffect(() => {
    setAccepted(null);
    setTokenInput(tokenFromUrl);
  }, [tokenFromUrl]);

  const handleAccept = async () => {
    const token = tokenInput.trim();
    if (!token) {
      toast({ title: "Missing token", description: "Invite token was not found in the URL.", variant: "destructive" });
      return;
    }
    setLoading(true);
    try {
      const payload = await apiFetchJson("/v1/team/invites/accept", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          token,
          display_name: displayName || undefined,
        }),
      });
      setAccepted(payload as AcceptResponse);
      toast({ title: "Invite accepted", description: "API key generated. Copy it now." });
    } catch (error: any) {
      toast({ title: "Accept failed", description: error?.message ?? "Unable to accept invite.", variant: "destructive" });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="border-b border-border bg-card">
        <div className="container mx-auto px-6 py-6">
          <Link href="/platform">
            <Button variant="ghost" size="sm">
              Back to Menu
            </Button>
          </Link>
          <h1 className="mt-2 text-2xl font-semibold text-foreground">Accept Team Invite</h1>
          <p className="text-sm text-muted-foreground">Finish joining the team and generate your API key.</p>
        </div>
      </div>

      <div className="container mx-auto px-6 py-10">
        <Card className="border-border/60 bg-card/70 max-w-2xl">
          <CardContent className="p-6 space-y-4">
            <div>
              <label className="text-xs font-medium text-muted-foreground">Invite token</label>
              <Input
                className="mt-2"
                placeholder="Paste invite token"
                value={tokenInput}
                onChange={(event) => setTokenInput(event.target.value)}
              />
            </div>
            <div>
              <label className="text-xs font-medium text-muted-foreground">Display name (optional)</label>
              <Input
                className="mt-2"
                placeholder="Your name"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
              />
            </div>

            <Button onClick={handleAccept} disabled={loading || !tokenInput.trim()}>
              {loading ? "Accepting..." : "Accept invite"}
            </Button>

            {accepted && (
              <div className="mt-4 rounded-lg border border-emerald-500/30 bg-emerald-500/10 p-4">
                <p className="text-sm text-emerald-200 font-semibold">Invite accepted</p>
                <p className="text-xs text-emerald-200/80 mt-1">
                  This API key is shown once. Copy and store it securely.
                </p>
                <div className="mt-3 flex flex-col gap-2 md:flex-row md:items-center">
                  <Input value={accepted.api_key} readOnly />
                  <Button
                    variant="outline"
                    onClick={() => {
                      if (typeof navigator !== "undefined") {
                        navigator.clipboard?.writeText(accepted.api_key);
                        toast({ title: "Copied API key" });
                      }
                    }}
                  >
                    Copy key
                  </Button>
                </div>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
