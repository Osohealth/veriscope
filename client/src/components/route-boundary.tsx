import { Component, type ReactNode } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

type Props = {
  name?: string;
  children: ReactNode;
};

type State = {
  hasError: boolean;
};

export default class RouteBoundary extends Component<Props, State> {
  state: State = { hasError: false };

  static getDerivedStateFromError() {
    return { hasError: true };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen bg-background text-foreground">
          <div className="mx-auto max-w-3xl px-6 py-20">
            <Card className="border-border/60">
              <CardContent className="space-y-4 pt-6">
                <div className="text-lg font-semibold">Something went wrong</div>
                <div className="text-sm text-muted-foreground">
                  {this.props.name ? `The ${this.props.name} route failed to render.` : "This route failed to render."}
                  {" "}Try reloading or return to the terminal.
                </div>
                <div className="flex items-center gap-2">
                  <Button onClick={() => window.location.reload()}>Reload</Button>
                  <Button variant="outline" onClick={() => (window.location.href = "/terminal")}>
                    Go to Terminal
                  </Button>
                </div>
              </CardContent>
            </Card>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}
