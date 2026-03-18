import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Link } from "wouter";
import { ArrowLeft, Construction } from "lucide-react";

interface ModulePageProps {
  title: string;
  description: string;
  backLink: string;
  backText: string;
}

export default function ModulePage({ title, description, backLink, backText }: ModulePageProps) {
  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href={backLink}>
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                {backText}
              </Button>
            </Link>
            <div>
              <h1 className="text-2xl font-bold" data-testid="text-page-title">{title}</h1>
              <p className="text-sm text-muted-foreground">{description}</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6">
        <Card className="bg-accent/50">
          <CardContent className="p-12 text-center">
            <Construction className="w-16 h-16 mx-auto mb-4 text-muted-foreground" />
            <h2 className="text-xl font-semibold mb-2">Module Coming Soon</h2>
            <p className="text-muted-foreground mb-6">
              This module is currently under development. Please check back later or contact support for more information.
            </p>
            <div className="flex gap-3 justify-center">
              <Link href={backLink}>
                <Button variant="outline" data-testid="button-go-back">
                  Go Back
                </Button>
              </Link>
              <Link href="/">
                <Button data-testid="button-return-home">
                  Return to Home
                </Button>
              </Link>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

// Helper function to create module page components
export function createModulePage(title: string, description: string, backLink: string, backText: string) {
  return () => <ModulePage title={title} description={description} backLink={backLink} backText={backText} />;
}
