import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Satellite, Cpu, Calculator, Eye, HelpCircle, AlertTriangle, CheckCircle } from "lucide-react";
import { cn } from "@/lib/utils";

export type DataMethodology = 'satellite' | 'sensor' | 'modeled' | 'inferred' | 'reported';
export type ConfidenceLevel = 'high' | 'medium' | 'low';

export function parseMethodology(source: string | undefined | null): DataMethodology {
  if (!source) return 'reported';
  const normalized = source.toLowerCase();
  if (normalized.includes('satellite') || normalized.includes('sar')) return 'satellite';
  if (normalized.includes('sensor') || normalized.includes('monitor')) return 'sensor';
  if (normalized.includes('model') || normalized.includes('ml') || normalized.includes('predict')) return 'modeled';
  if (normalized.includes('infer') || normalized.includes('calc') || normalized.includes('ais')) return 'inferred';
  return 'reported';
}

interface CredibilityIndicatorProps {
  methodology: DataMethodology;
  confidence?: number;
  uncertaintyLow?: number;
  uncertaintyHigh?: number;
  lastUpdate?: Date | string;
  showDetails?: boolean;
  size?: 'sm' | 'md';
  className?: string;
}

const methodologyConfig: Record<DataMethodology, {
  icon: typeof Satellite;
  label: string;
  color: string;
  bgColor: string;
  description: string;
}> = {
  satellite: {
    icon: Satellite,
    label: 'SAR',
    color: 'text-emerald-400',
    bgColor: 'bg-emerald-400/10 border-emerald-400/30',
    description: 'Satellite radar imagery (SAR) with validated tank shadow analysis'
  },
  sensor: {
    icon: Eye,
    label: 'Sensor',
    color: 'text-blue-400',
    bgColor: 'bg-blue-400/10 border-blue-400/30',
    description: 'Direct sensor readings from tank level monitors'
  },
  modeled: {
    icon: Cpu,
    label: 'Modeled',
    color: 'text-purple-400',
    bgColor: 'bg-purple-400/10 border-purple-400/30',
    description: 'ML model prediction based on historical patterns and correlations'
  },
  inferred: {
    icon: Calculator,
    label: 'Inferred',
    color: 'text-yellow-400',
    bgColor: 'bg-yellow-400/10 border-yellow-400/30',
    description: 'Calculated from vessel movements and port activity'
  },
  reported: {
    icon: HelpCircle,
    label: 'Reported',
    color: 'text-gray-400',
    bgColor: 'bg-gray-400/10 border-gray-400/30',
    description: 'Self-reported data from terminal operators (unverified)'
  }
};

function getConfidenceLevel(confidence: number): ConfidenceLevel {
  if (confidence >= 0.8) return 'high';
  if (confidence >= 0.5) return 'medium';
  return 'low';
}

const confidenceConfig: Record<ConfidenceLevel, {
  icon: typeof CheckCircle;
  label: string;
  color: string;
}> = {
  high: { icon: CheckCircle, label: 'High Confidence', color: 'text-emerald-400' },
  medium: { icon: AlertTriangle, label: 'Medium Confidence', color: 'text-yellow-400' },
  low: { icon: AlertTriangle, label: 'Low Confidence', color: 'text-red-400' }
};

export function MethodologyBadge({ 
  methodology, 
  size = 'sm',
  className 
}: { 
  methodology: DataMethodology; 
  size?: 'sm' | 'md';
  className?: string;
}) {
  const config = methodologyConfig[methodology];
  const Icon = config.icon;
  const iconSize = size === 'sm' ? 'w-3 h-3' : 'w-4 h-4';
  const textSize = size === 'sm' ? 'text-xs' : 'text-sm';

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge 
          variant="outline" 
          className={cn(
            "gap-1 cursor-default border",
            config.bgColor,
            config.color,
            textSize,
            className
          )}
          data-testid={`badge-methodology-${methodology}`}
        >
          <Icon className={iconSize} />
          {config.label}
        </Badge>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        <p className="font-semibold">{config.label} Data</p>
        <p className="text-xs text-muted-foreground">{config.description}</p>
      </TooltipContent>
    </Tooltip>
  );
}

export function ConfidenceBadge({ 
  confidence,
  size = 'sm',
  className 
}: { 
  confidence: number;
  size?: 'sm' | 'md';
  className?: string;
}) {
  const level = getConfidenceLevel(confidence);
  const config = confidenceConfig[level];
  const Icon = config.icon;
  const iconSize = size === 'sm' ? 'w-3 h-3' : 'w-4 h-4';
  const textSize = size === 'sm' ? 'text-xs' : 'text-sm';

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Badge 
          variant="outline" 
          className={cn(
            "gap-1 cursor-default",
            config.color,
            textSize,
            className
          )}
          data-testid={`badge-confidence-${level}`}
        >
          <Icon className={iconSize} />
          {(confidence * 100).toFixed(0)}%
        </Badge>
      </TooltipTrigger>
      <TooltipContent side="top">
        <p className="font-semibold">{config.label}</p>
        <p className="text-xs text-muted-foreground">
          {confidence >= 0.8 
            ? 'Data validated from multiple sources'
            : confidence >= 0.5
              ? 'Single source or partial validation'
              : 'Limited data quality, use with caution'
          }
        </p>
      </TooltipContent>
    </Tooltip>
  );
}

export function UncertaintyBand({
  value,
  low,
  high,
  unit = '%',
  className
}: {
  value: number;
  low: number;
  high: number;
  unit?: string;
  className?: string;
}) {
  const range = high - low;
  const uncertaintyPercent = ((range / value) * 100).toFixed(1);
  
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div 
          className={cn("flex items-center gap-1 text-xs text-muted-foreground cursor-default", className)}
          data-testid="uncertainty-band"
        >
          <span className="text-foreground font-medium">{value.toFixed(1)}{unit}</span>
          <span className="text-yellow-400/80">
            ±{((high - low) / 2).toFixed(1)}{unit}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent side="top" className="max-w-xs">
        <p className="font-semibold">Uncertainty Range</p>
        <div className="text-xs text-muted-foreground space-y-1 mt-1">
          <p>Best estimate: <span className="text-foreground">{value.toFixed(1)}{unit}</span></p>
          <p>Range: <span className="text-foreground">{low.toFixed(1)}{unit} - {high.toFixed(1)}{unit}</span></p>
          <p>Uncertainty: <span className="text-yellow-400">±{uncertaintyPercent}%</span></p>
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

export function CredibilityIndicator({
  methodology,
  confidence,
  uncertaintyLow,
  uncertaintyHigh,
  lastUpdate,
  showDetails = true,
  size = 'sm',
  className
}: CredibilityIndicatorProps) {
  const config = methodologyConfig[methodology];
  const Icon = config.icon;
  
  const textSize = size === 'sm' ? 'text-xs' : 'text-sm';
  const iconSize = size === 'sm' ? 'w-3 h-3' : 'w-4 h-4';
  
  const updateTime = lastUpdate ? new Date(lastUpdate) : null;
  const now = new Date();
  const hoursAgo = updateTime 
    ? Math.floor((now.getTime() - updateTime.getTime()) / (1000 * 60 * 60))
    : null;

  return (
    <div 
      className={cn(
        "flex flex-wrap items-center gap-2",
        className
      )}
      data-testid="credibility-indicator"
    >
      <MethodologyBadge methodology={methodology} size={size} />
      
      {confidence !== undefined && (
        <ConfidenceBadge confidence={confidence} size={size} />
      )}
      
      {showDetails && uncertaintyLow !== undefined && uncertaintyHigh !== undefined && (
        <UncertaintyBand 
          value={(uncertaintyLow + uncertaintyHigh) / 2}
          low={uncertaintyLow}
          high={uncertaintyHigh}
        />
      )}
      
      {showDetails && hoursAgo !== null && (
        <span className={cn(textSize, "text-muted-foreground")}>
          Updated {hoursAgo < 1 ? 'just now' : `${hoursAgo}h ago`}
        </span>
      )}
    </div>
  );
}

export function DataSourceLegend({ className }: { className?: string }) {
  return (
    <div className={cn("flex flex-wrap gap-3", className)} data-testid="data-source-legend">
      {Object.entries(methodologyConfig).map(([key, config]) => {
        const Icon = config.icon;
        return (
          <div key={key} className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <Icon className={cn("w-3 h-3", config.color)} />
            <span>{config.label}</span>
          </div>
        );
      })}
    </div>
  );
}
