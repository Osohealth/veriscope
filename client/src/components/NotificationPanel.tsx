import { useNotifications } from "@/hooks/use-notifications";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { X, Clock, AlertTriangle, Info, TrendingUp, AlertCircle } from "lucide-react";
import { formatDistanceToNow } from "date-fns";

interface NotificationPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

export function NotificationPanel({ isOpen, onClose }: NotificationPanelProps) {
  const { signals, isLoading, getSeverityColor, getSeverityLabel, markAsRead } = useNotifications();

  if (!isOpen) return null;

  const getSeverityIcon = (severity: number) => {
    switch (severity) {
      case 1: return <Info className="w-4 h-4" />;
      case 2: return <TrendingUp className="w-4 h-4" />;
      case 3: return <Clock className="w-4 h-4" />;
      case 4: return <AlertTriangle className="w-4 h-4" />;
      case 5: return <AlertCircle className="w-4 h-4" />;
      default: return <Info className="w-4 h-4" />;
    }
  };

  const getSeverityBadgeVariant = (severity: number) => {
    switch (severity) {
      case 1: return "secondary";
      case 2: return "outline";
      case 3: return "default";
      case 4: return "destructive";
      case 5: return "destructive";
      default: return "secondary";
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-end p-4">
      {/* Backdrop */}
      <div 
        className="fixed inset-0 bg-black/20" 
        onClick={onClose}
        data-testid="notification-backdrop"
      />
      
      {/* Panel */}
      <Card className="relative w-96 max-h-[80vh] bg-card border shadow-lg" data-testid="notification-panel">
        <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-3">
          <CardTitle className="text-lg font-semibold">Notifications</CardTitle>
          <div className="flex items-center space-x-2">
            {signals.length > 0 && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => markAsRead()}
                className="text-xs"
                data-testid="button-mark-all-read"
              >
                Mark all read
              </Button>
            )}
            <Button
              variant="ghost"
              size="sm"
              onClick={onClose}
              data-testid="button-close-notifications"
            >
              <X className="w-4 h-4" />
            </Button>
          </div>
        </CardHeader>
        
        <Separator />
        
        <CardContent className="p-0">
          <ScrollArea className="h-[calc(80vh-8rem)]">
            {isLoading ? (
              <div className="p-4 text-center text-muted-foreground">
                Loading notifications...
              </div>
            ) : signals.length === 0 ? (
              <div className="p-8 text-center">
                <div className="text-muted-foreground mb-2">
                  <Info className="w-8 h-8 mx-auto mb-2 opacity-50" />
                </div>
                <p className="text-sm text-muted-foreground">No notifications</p>
                <p className="text-xs text-muted-foreground mt-1">
                  You're all caught up!
                </p>
              </div>
            ) : (
              <div className="divide-y">
                {signals.map((signal, index) => (
                  <div
                    key={signal.id}
                    className="p-4 hover:bg-muted/50 transition-colors cursor-pointer"
                    onClick={() => markAsRead(signal.id)}
                    data-testid={`notification-item-${index}`}
                  >
                    <div className="flex items-start space-x-3">
                      <div className={`flex-shrink-0 p-1 rounded-full ${
                        signal.severity >= 4 ? 'bg-destructive/10 text-destructive' :
                        signal.severity === 3 ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300' :
                        signal.severity === 2 ? 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300' :
                        'bg-muted text-muted-foreground'
                      }`}>
                        {getSeverityIcon(signal.severity)}
                      </div>
                      
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <h4 className="text-sm font-medium truncate pr-2">
                            {signal.title}
                          </h4>
                          <Badge 
                            variant={getSeverityBadgeVariant(signal.severity) as any}
                            className="text-xs"
                          >
                            {getSeverityLabel(signal.severity)}
                          </Badge>
                        </div>
                        
                        {signal.description && (
                          <p className="text-sm text-muted-foreground mb-2 line-clamp-2">
                            {signal.description}
                          </p>
                        )}
                        
                        <div className="flex items-center justify-between text-xs text-muted-foreground">
                          <span className="capitalize">
                            {signal.signalType ? signal.signalType.replace(/_/g, ' ').toLowerCase() : 'alert'}
                          </span>
                          <span>
                            {signal.timestamp ? 
                              formatDistanceToNow(new Date(signal.timestamp), { addSuffix: true }) :
                              'Just now'
                            }
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </ScrollArea>
        </CardContent>
      </Card>
    </div>
  );
}