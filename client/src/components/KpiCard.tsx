import { cn } from "@/lib/utils";

interface KpiCardProps {
  value: string;
  label: string;
  change: string;
  changeType: "positive" | "negative" | "warning" | "neutral";
  testId?: string;
}

export default function KpiCard({ value, label, change, changeType, testId }: KpiCardProps) {
  const changeColorMap = {
    positive: "text-emerald-400",
    negative: "text-destructive",
    warning: "text-amber-400",
    neutral: "text-muted-foreground"
  };

  return (
    <div className="bg-muted rounded-lg p-3" data-testid={testId}>
      <div className="text-lg font-bold text-foreground" data-testid={`${testId}-value`}>
        {value}
      </div>
      <div className="text-xs text-muted-foreground" data-testid={`${testId}-label`}>
        {label}
      </div>
      <div className={cn(
        "text-xs",
        changeColorMap[changeType]
      )} data-testid={`${testId}-change`}>
        {change}
      </div>
    </div>
  );
}
