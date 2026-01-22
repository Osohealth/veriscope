import { useState, useEffect } from "react";
import { Badge } from "@/components/ui/badge";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { Wifi, WifiOff, Clock, AlertTriangle } from "lucide-react";
import { cn } from "@/lib/utils";

interface DataFreshnessIndicatorProps {
  lastUpdate?: Date | string | null;
  streamName?: string;
  refreshInterval?: number;
  showLabel?: boolean;
  className?: string;
  size?: 'sm' | 'md';
}

type StreamStatus = 'live' | 'delayed' | 'degraded' | 'offline';

export function DataFreshnessIndicator({
  lastUpdate,
  streamName = 'Data Stream',
  refreshInterval = 30000,
  showLabel = true,
  className,
  size = 'sm'
}: DataFreshnessIndicatorProps) {
  const [status, setStatus] = useState<StreamStatus>('live');
  const [timeAgo, setTimeAgo] = useState<string>('');

  useEffect(() => {
    const updateStatus = () => {
      if (!lastUpdate) {
        setStatus('offline');
        setTimeAgo('No data');
        return;
      }

      const updateTime = new Date(lastUpdate);
      const now = new Date();
      const diffMs = now.getTime() - updateTime.getTime();
      const diffSeconds = Math.floor(diffMs / 1000);
      const diffMinutes = Math.floor(diffSeconds / 60);
      const diffHours = Math.floor(diffMinutes / 60);

      if (diffSeconds < 60) {
        setTimeAgo('Just now');
      } else if (diffMinutes < 60) {
        setTimeAgo(`${diffMinutes}m ago`);
      } else if (diffHours < 24) {
        setTimeAgo(`${diffHours}h ago`);
      } else {
        setTimeAgo(updateTime.toLocaleDateString());
      }

      if (diffMinutes < 2) {
        setStatus('live');
      } else if (diffMinutes < 10) {
        setStatus('delayed');
      } else if (diffMinutes < 30) {
        setStatus('degraded');
      } else {
        setStatus('offline');
      }
    };

    updateStatus();
    const interval = setInterval(updateStatus, refreshInterval);
    return () => clearInterval(interval);
  }, [lastUpdate, refreshInterval]);

  const statusConfig = {
    live: {
      color: 'text-emerald-400',
      bgColor: 'bg-emerald-400/10',
      borderColor: 'border-emerald-400/30',
      icon: Wifi,
      label: 'Live',
      pulse: true
    },
    delayed: {
      color: 'text-yellow-400',
      bgColor: 'bg-yellow-400/10',
      borderColor: 'border-yellow-400/30',
      icon: Clock,
      label: 'Delayed',
      pulse: false
    },
    degraded: {
      color: 'text-orange-400',
      bgColor: 'bg-orange-400/10',
      borderColor: 'border-orange-400/30',
      icon: AlertTriangle,
      label: 'Degraded',
      pulse: false
    },
    offline: {
      color: 'text-red-400',
      bgColor: 'bg-red-400/10',
      borderColor: 'border-red-400/30',
      icon: WifiOff,
      label: 'Offline',
      pulse: false
    }
  };

  const config = statusConfig[status];
  const Icon = config.icon;

  const iconSize = size === 'sm' ? 'w-3 h-3' : 'w-4 h-4';
  const textSize = size === 'sm' ? 'text-xs' : 'text-sm';
  const dotSize = size === 'sm' ? 'w-1.5 h-1.5' : 'w-2 h-2';

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div 
          className={cn(
            "flex items-center gap-1.5 px-2 py-1 rounded-md border cursor-default",
            config.bgColor,
            config.borderColor,
            className
          )}
          data-testid={`indicator-freshness-${streamName.toLowerCase().replace(/\s+/g, '-')}`}
        >
          <div className="flex items-center gap-1">
            <div className={cn(dotSize, "rounded-full", config.color.replace('text-', 'bg-'), config.pulse && "pulse-dot")} />
            {showLabel && (
              <span className={cn(textSize, config.color, "font-medium")}>{config.label}</span>
            )}
          </div>
          <span className={cn(textSize, "text-muted-foreground")}>
            {timeAgo}
          </span>
        </div>
      </TooltipTrigger>
      <TooltipContent side="bottom" className="max-w-xs">
        <div className="space-y-1">
          <p className="font-semibold">{streamName}</p>
          <p className="text-xs text-muted-foreground">
            Status: <span className={config.color}>{config.label}</span>
          </p>
          {lastUpdate && (
            <p className="text-xs text-muted-foreground">
              Last update: {new Date(lastUpdate).toLocaleString()}
            </p>
          )}
        </div>
      </TooltipContent>
    </Tooltip>
  );
}

interface StreamStatusBadgeProps {
  status: 'live' | 'delayed' | 'degraded' | 'offline';
  className?: string;
}

export function StreamStatusBadge({ status, className }: StreamStatusBadgeProps) {
  const statusConfig = {
    live: { color: 'bg-emerald-500', label: 'Live', pulse: true },
    delayed: { color: 'bg-yellow-500', label: 'Delayed', pulse: false },
    degraded: { color: 'bg-orange-500', label: 'Degraded', pulse: false },
    offline: { color: 'bg-red-500', label: 'Offline', pulse: false }
  };

  const config = statusConfig[status];

  return (
    <Badge 
      variant="outline" 
      className={cn("gap-1.5", className)}
      data-testid={`badge-stream-${status}`}
    >
      <div className={cn("w-2 h-2 rounded-full", config.color, config.pulse && "pulse-dot")} />
      {config.label}
    </Badge>
  );
}

interface WidgetFreshnessProps {
  title: string;
  lastUpdate?: Date | string | null;
  children: React.ReactNode;
  className?: string;
}

export function WidgetWithFreshness({ title, lastUpdate, children, className }: WidgetFreshnessProps) {
  return (
    <div className={cn("relative", className)}>
      <div className="absolute top-2 right-2 z-10">
        <DataFreshnessIndicator lastUpdate={lastUpdate} streamName={title} size="sm" />
      </div>
      {children}
    </div>
  );
}
