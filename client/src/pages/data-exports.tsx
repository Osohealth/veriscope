import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useToast } from "@/hooks/use-toast";
import { Download, Ship, Anchor, AlertTriangle, TrendingUp, FileSpreadsheet, CheckCircle } from "lucide-react";
import { getAuthHeaders } from "@/lib/queryClient";

const EXPORT_OPTIONS = [
  {
    id: "vessels",
    name: "Vessels",
    description: "Export all vessel data including MMSI, IMO, type, flag, and specifications",
    icon: Ship,
    endpoint: "/api/export/vessels",
    filename: "vessels.csv"
  },
  {
    id: "ports",
    name: "Ports",
    description: "Export port database with coordinates, type, capacity, and operational status",
    icon: Anchor,
    endpoint: "/api/export/ports",
    filename: "ports.csv"
  },
  {
    id: "signals",
    name: "Signals",
    description: "Export active market signals and alerts with timestamps",
    icon: AlertTriangle,
    endpoint: "/api/export/signals",
    filename: "signals.csv"
  },
  {
    id: "predictions",
    name: "Predictions",
    description: "Export price predictions with confidence scores and timeframes",
    icon: TrendingUp,
    endpoint: "/api/export/predictions",
    filename: "predictions.csv"
  }
];

export default function DataExportsPage() {
  const { toast } = useToast();
  const [downloading, setDownloading] = useState<string | null>(null);
  const [recentDownloads, setRecentDownloads] = useState<string[]>([]);

  const handleDownload = async (option: typeof EXPORT_OPTIONS[0]) => {
    setDownloading(option.id);
    try {
      const response = await fetch(option.endpoint, { headers: getAuthHeaders() });
      if (!response.ok) throw new Error("Export failed");
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = option.filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      window.URL.revokeObjectURL(url);
      
      setRecentDownloads((prev) => [option.id, ...prev.filter((id) => id !== option.id)]);
      toast({ title: `${option.name} exported successfully` });
    } catch (error) {
      toast({ title: `Failed to export ${option.name}`, variant: "destructive" });
    } finally {
      setDownloading(null);
    }
  };

  return (
    <div className="p-6 space-y-6 bg-slate-950 min-h-screen">
      <div>
        <h1 className="text-3xl font-bold text-white">Data Exports</h1>
        <p className="text-slate-400 mt-1">Download your data in CSV format for analysis</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {EXPORT_OPTIONS.map((option) => {
          const IconComponent = option.icon;
          const isDownloading = downloading === option.id;
          const wasDownloaded = recentDownloads.includes(option.id);
          
          return (
            <Card 
              key={option.id} 
              data-testid={`card-export-${option.id}`}
              className="bg-slate-900 border-slate-800 hover:border-slate-700 transition-colors"
            >
              <CardHeader className="pb-3">
                <div className="flex items-center gap-3">
                  <div className="p-2 bg-slate-800 rounded-lg text-blue-400">
                    <IconComponent className="h-5 w-5" />
                  </div>
                  <div className="flex-1">
                    <div className="flex items-center gap-2">
                      <CardTitle className="text-white text-lg">{option.name}</CardTitle>
                      {wasDownloaded && (
                        <CheckCircle className="h-4 w-4 text-green-500" />
                      )}
                    </div>
                    <CardDescription className="text-slate-400">{option.description}</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent>
                <div className="flex items-center justify-between">
                  <Badge variant="outline" className="border-slate-700 text-slate-400">
                    <FileSpreadsheet className="h-3 w-3 mr-1" />
                    CSV
                  </Badge>
                  <Button
                    data-testid={`button-download-${option.id}`}
                    onClick={() => handleDownload(option)}
                    disabled={isDownloading}
                    className="bg-blue-600 hover:bg-blue-700"
                  >
                    {isDownloading ? (
                      <>
                        <div className="animate-spin mr-2 h-4 w-4 border-2 border-white border-t-transparent rounded-full" />
                        Exporting...
                      </>
                    ) : (
                      <>
                        <Download className="h-4 w-4 mr-2" />
                        Download
                      </>
                    )}
                  </Button>
                </div>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <Card className="bg-slate-900 border-slate-800">
        <CardHeader>
          <CardTitle className="text-white">Export Tips</CardTitle>
        </CardHeader>
        <CardContent className="text-slate-400 space-y-2">
          <p>• CSV files can be opened in Excel, Google Sheets, or any spreadsheet application</p>
          <p>• Large datasets may take a moment to generate</p>
          <p>• Data is exported in real-time from the current database state</p>
          <p>• For programmatic access, use the API endpoints directly</p>
        </CardContent>
      </Card>
    </div>
  );
}
