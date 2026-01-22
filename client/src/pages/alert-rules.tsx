import { useState } from "react";
import { useQuery, useMutation } from "@tanstack/react-query";
import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger, DialogFooter } from "@/components/ui/dialog";
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogFooter, AlertDialogHeader, AlertDialogTitle, AlertDialogTrigger } from "@/components/ui/alert-dialog";
import { Checkbox } from "@/components/ui/checkbox";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuSeparator, DropdownMenuTrigger } from "@/components/ui/dropdown-menu";
import { useToast } from "@/hooks/use-toast";
import { queryClient, apiRequest } from "@/lib/queryClient";
import { Bell, Plus, Trash2, Edit2, TrendingUp, Anchor, Droplets, MoreVertical, VolumeX, Volume2, Clock, AlertCircle } from "lucide-react";
import type { AlertRule, Watchlist } from "@shared/schema";

const ALERT_TYPES = [
  { value: "price_threshold", label: "Price Threshold", icon: TrendingUp },
  { value: "congestion", label: "Port Congestion", icon: Anchor },
  { value: "vessel_arrival", label: "Vessel Arrival", icon: Anchor },
  { value: "storage_level", label: "Storage Level", icon: Droplets },
];

const CHANNELS = [
  { value: "in_app", label: "In-App Notification" },
  { value: "email", label: "Email" },
  { value: "webhook", label: "Webhook" },
];

const SEVERITIES = [
  { value: "critical", label: "Critical", color: "bg-red-500" },
  { value: "high", label: "High", color: "bg-orange-500" },
  { value: "medium", label: "Medium", color: "bg-yellow-500" },
  { value: "low", label: "Low", color: "bg-blue-500" },
];

const SNOOZE_OPTIONS = [
  { hours: 1, label: "1 hour" },
  { hours: 4, label: "4 hours" },
  { hours: 8, label: "8 hours" },
  { hours: 24, label: "1 day" },
  { hours: 72, label: "3 days" },
  { hours: 168, label: "1 week" },
];

export default function AlertRulesPage() {
  const { toast } = useToast();
  const [isCreateOpen, setIsCreateOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<AlertRule | null>(null);
  const [formData, setFormData] = useState({
    name: "",
    type: "price_threshold",
    conditions: {
      operator: "greater_than",
      value: 0,
      commodity: "",
      port: ""
    },
    channels: ["in_app"] as string[],
    cooldownMinutes: 60,
    watchlistId: "",
    isActive: true,
    severity: "medium",
    isMuted: false
  });

  const { data: alertRules = [], isLoading } = useQuery<AlertRule[]>({
    queryKey: ["/api/alert-rules"]
  });

  const { data: watchlists = [] } = useQuery<Watchlist[]>({
    queryKey: ["/api/watchlists"]
  });

  const createMutation = useMutation({
    mutationFn: async (data: any) => {
      return await apiRequest("POST", "/api/alert-rules", data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      setIsCreateOpen(false);
      resetForm();
      toast({ title: "Alert rule created successfully" });
    },
    onError: () => {
      toast({ title: "Failed to create alert rule", variant: "destructive" });
    }
  });

  const updateMutation = useMutation({
    mutationFn: async ({ id, data }: { id: string; data: any }) => {
      return await apiRequest("PATCH", `/api/alert-rules/${id}`, data);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      setEditingRule(null);
      resetForm();
      toast({ title: "Alert rule updated successfully" });
    },
    onError: () => {
      toast({ title: "Failed to update alert rule", variant: "destructive" });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: async (id: string) => {
      return await apiRequest("DELETE", `/api/alert-rules/${id}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      toast({ title: "Alert rule deleted successfully" });
    },
    onError: () => {
      toast({ title: "Failed to delete alert rule", variant: "destructive" });
    }
  });

  const toggleMutation = useMutation({
    mutationFn: async ({ id, isActive }: { id: string; isActive: boolean }) => {
      return await apiRequest("PATCH", `/api/alert-rules/${id}`, { isActive });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
    }
  });

  const snoozeMutation = useMutation({
    mutationFn: async ({ id, hours }: { id: string; hours: number }) => {
      return await apiRequest("POST", `/api/alert-rules/${id}/snooze`, { hours });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      toast({ title: "Alert rule snoozed" });
    },
    onError: () => {
      toast({ title: "Failed to snooze alert rule", variant: "destructive" });
    }
  });

  const unsnoozeMutation = useMutation({
    mutationFn: async (id: string) => {
      return await apiRequest("POST", `/api/alert-rules/${id}/unsnooze`, {});
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      toast({ title: "Alert rule unsnoozed" });
    },
    onError: () => {
      toast({ title: "Failed to unsnooze alert rule", variant: "destructive" });
    }
  });

  const muteMutation = useMutation({
    mutationFn: async ({ id, muted }: { id: string; muted: boolean }) => {
      return await apiRequest("POST", `/api/alert-rules/${id}/mute`, { muted });
    },
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries({ queryKey: ["/api/alert-rules"] });
      toast({ title: variables.muted ? "Alert rule muted" : "Alert rule unmuted" });
    },
    onError: () => {
      toast({ title: "Failed to update mute status", variant: "destructive" });
    }
  });

  const resetForm = () => {
    setFormData({
      name: "",
      type: "price_threshold",
      conditions: { operator: "greater_than", value: 0, commodity: "", port: "" },
      channels: ["in_app"],
      cooldownMinutes: 60,
      watchlistId: "",
      isActive: true,
      severity: "medium",
      isMuted: false
    });
  };

  const handleChannelToggle = (channel: string) => {
    if (formData.channels.includes(channel)) {
      setFormData({
        ...formData,
        channels: formData.channels.filter((c) => c !== channel)
      });
    } else {
      setFormData({
        ...formData,
        channels: [...formData.channels, channel]
      });
    }
  };

  const handleSubmit = () => {
    const data = {
      name: formData.name,
      type: formData.type,
      conditions: formData.conditions,
      channels: formData.channels,
      cooldownMinutes: formData.cooldownMinutes,
      watchlistId: formData.watchlistId || undefined,
      isActive: formData.isActive,
      severity: formData.severity,
      isMuted: formData.isMuted
    };

    if (editingRule) {
      updateMutation.mutate({ id: editingRule.id, data });
    } else {
      createMutation.mutate(data);
    }
  };

  const openEdit = (rule: AlertRule) => {
    setEditingRule(rule);
    const conditions = typeof rule.conditions === 'object' ? rule.conditions : {};
    const channels = Array.isArray(rule.channels) ? rule.channels : ["in_app"];
    setFormData({
      name: rule.name,
      type: rule.type,
      conditions: conditions as any,
      channels: channels as string[],
      cooldownMinutes: rule.cooldownMinutes || 60,
      watchlistId: rule.watchlistId || "",
      isActive: rule.isActive ?? true,
      severity: (rule as any).severity || "medium",
      isMuted: (rule as any).isMuted || false
    });
  };

  const getTypeInfo = (type: string) => {
    return ALERT_TYPES.find((t) => t.value === type) || ALERT_TYPES[0];
  };

  const getSeverityInfo = (severity: string) => {
    return SEVERITIES.find((s) => s.value === severity) || SEVERITIES[2];
  };

  const isSnoozed = (rule: AlertRule) => {
    const snoozedUntil = (rule as any).snoozedUntil;
    return snoozedUntil && new Date(snoozedUntil) > new Date();
  };

  const getSnoozeRemaining = (rule: AlertRule) => {
    const snoozedUntil = (rule as any).snoozedUntil;
    if (!snoozedUntil) return null;
    const remaining = new Date(snoozedUntil).getTime() - Date.now();
    if (remaining <= 0) return null;
    const hours = Math.floor(remaining / (1000 * 60 * 60));
    const mins = Math.floor((remaining % (1000 * 60 * 60)) / (1000 * 60));
    if (hours > 24) return `${Math.floor(hours / 24)}d`;
    if (hours > 0) return `${hours}h ${mins}m`;
    return `${mins}m`;
  };

  return (
    <div className="p-6 space-y-6 bg-slate-950 min-h-screen">
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-3xl font-bold text-white">Alert Rules</h1>
          <p className="text-slate-400 mt-1">Configure automated alerts for market conditions</p>
        </div>
        <Dialog open={isCreateOpen} onOpenChange={setIsCreateOpen}>
          <DialogTrigger asChild>
            <Button data-testid="button-create-alert" className="bg-blue-600 hover:bg-blue-700">
              <Plus className="h-4 w-4 mr-2" />
              Create Alert Rule
            </Button>
          </DialogTrigger>
          <DialogContent className="bg-slate-900 border-slate-700 max-w-lg max-h-[90vh] overflow-y-auto">
            <DialogHeader>
              <DialogTitle className="text-white">Create Alert Rule</DialogTitle>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label className="text-slate-300">Name</Label>
                <Input
                  data-testid="input-alert-name"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  placeholder="My Alert Rule"
                  className="bg-slate-800 border-slate-700 text-white"
                />
              </div>
              
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label className="text-slate-300">Alert Type</Label>
                  <Select
                    value={formData.type}
                    onValueChange={(value) => setFormData({ ...formData, type: value })}
                  >
                    <SelectTrigger data-testid="select-alert-type" className="bg-slate-800 border-slate-700 text-white">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-slate-800 border-slate-700">
                      {ALERT_TYPES.map((type) => (
                        <SelectItem key={type.value} value={type.value}>
                          {type.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
                
                <div className="space-y-2">
                  <Label className="text-slate-300">Severity</Label>
                  <Select
                    value={formData.severity}
                    onValueChange={(value) => setFormData({ ...formData, severity: value })}
                  >
                    <SelectTrigger data-testid="select-severity" className="bg-slate-800 border-slate-700 text-white">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent className="bg-slate-800 border-slate-700">
                      {SEVERITIES.map((sev) => (
                        <SelectItem key={sev.value} value={sev.value}>
                          <div className="flex items-center gap-2">
                            <div className={`w-2 h-2 rounded-full ${sev.color}`} />
                            {sev.label}
                          </div>
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </div>

              {formData.type === "price_threshold" && (
                <div className="space-y-4 p-4 bg-slate-800 rounded-lg">
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label className="text-slate-300">Operator</Label>
                      <Select
                        value={formData.conditions.operator}
                        onValueChange={(value) => setFormData({
                          ...formData,
                          conditions: { ...formData.conditions, operator: value }
                        })}
                      >
                        <SelectTrigger className="bg-slate-700 border-slate-600 text-white">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="bg-slate-800 border-slate-700">
                          <SelectItem value="greater_than">Greater Than</SelectItem>
                          <SelectItem value="less_than">Less Than</SelectItem>
                          <SelectItem value="equals">Equals</SelectItem>
                          <SelectItem value="change_percent">% Change</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>
                    <div className="space-y-2">
                      <Label className="text-slate-300">Value</Label>
                      <Input
                        type="number"
                        value={formData.conditions.value}
                        onChange={(e) => setFormData({
                          ...formData,
                          conditions: { ...formData.conditions, value: Number(e.target.value) }
                        })}
                        className="bg-slate-700 border-slate-600 text-white"
                      />
                    </div>
                  </div>
                </div>
              )}

              {formData.type === "congestion" && (
                <div className="space-y-4 p-4 bg-slate-800 rounded-lg">
                  <div className="space-y-2">
                    <Label className="text-slate-300">Queue Length Threshold</Label>
                    <Input
                      type="number"
                      value={formData.conditions.value}
                      onChange={(e) => setFormData({
                        ...formData,
                        conditions: { ...formData.conditions, value: Number(e.target.value) }
                      })}
                      placeholder="Minimum vessels in queue"
                      className="bg-slate-700 border-slate-600 text-white"
                    />
                  </div>
                </div>
              )}

              <div className="space-y-2">
                <Label className="text-slate-300">Link to Watchlist (Optional)</Label>
                <Select
                  value={formData.watchlistId || "_none"}
                  onValueChange={(value) => setFormData({ ...formData, watchlistId: value === "_none" ? "" : value })}
                >
                  <SelectTrigger className="bg-slate-800 border-slate-700 text-white">
                    <SelectValue placeholder="Select a watchlist" />
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 border-slate-700">
                    <SelectItem value="_none">None</SelectItem>
                    {watchlists.map((wl) => (
                      <SelectItem key={wl.id} value={wl.id}>{wl.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label className="text-slate-300">Notification Channels</Label>
                <div className="space-y-2">
                  {CHANNELS.map((channel) => (
                    <div key={channel.value} className="flex items-center space-x-2">
                      <Checkbox
                        id={channel.value}
                        checked={formData.channels.includes(channel.value)}
                        onCheckedChange={() => handleChannelToggle(channel.value)}
                        className="border-slate-600"
                      />
                      <label htmlFor={channel.value} className="text-slate-300 text-sm">
                        {channel.label}
                      </label>
                    </div>
                  ))}
                </div>
              </div>

              <div className="space-y-2">
                <Label className="text-slate-300">Cooldown (minutes)</Label>
                <Input
                  type="number"
                  value={formData.cooldownMinutes}
                  onChange={(e) => setFormData({ ...formData, cooldownMinutes: Number(e.target.value) })}
                  className="bg-slate-800 border-slate-700 text-white"
                />
                <p className="text-xs text-slate-500">Minimum time between repeated alerts</p>
              </div>
            </div>
            <DialogFooter>
              <Button
                data-testid="button-save-alert"
                onClick={handleSubmit}
                disabled={!formData.name || formData.channels.length === 0 || createMutation.isPending}
                className="bg-blue-600 hover:bg-blue-700"
              >
                {createMutation.isPending ? "Creating..." : "Create Rule"}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {isLoading ? (
        <div className="text-slate-400">Loading alert rules...</div>
      ) : alertRules.length === 0 ? (
        <Card className="bg-slate-900 border-slate-800">
          <CardContent className="p-12 text-center">
            <Bell className="h-12 w-12 mx-auto text-slate-600 mb-4" />
            <h3 className="text-xl font-semibold text-white mb-2">No Alert Rules Yet</h3>
            <p className="text-slate-400 mb-4">Create alert rules to get notified about market changes</p>
            <Button 
              data-testid="button-create-first-alert"
              onClick={() => setIsCreateOpen(true)} 
              className="bg-blue-600 hover:bg-blue-700"
            >
              Create Your First Alert
            </Button>
          </CardContent>
        </Card>
      ) : (
        <div className="space-y-4">
          {alertRules.map((rule) => {
            const typeInfo = getTypeInfo(rule.type);
            const severityInfo = getSeverityInfo((rule as any).severity || "medium");
            const IconComponent = typeInfo.icon;
            const snoozed = isSnoozed(rule);
            const muted = (rule as any).isMuted;
            const snoozeRemaining = getSnoozeRemaining(rule);
            
            return (
              <Card key={rule.id} data-testid={`card-alert-${rule.id}`} className="bg-slate-900 border-slate-800">
                <CardContent className="p-4">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-4">
                      <div className={`p-3 rounded-lg relative ${rule.isActive && !muted && !snoozed ? 'bg-blue-900/50 text-blue-400' : 'bg-slate-800 text-slate-500'}`}>
                        <IconComponent className="h-5 w-5" />
                        <div className={`absolute -top-1 -right-1 w-3 h-3 rounded-full ${severityInfo.color}`} />
                      </div>
                      <div>
                        <div className="flex items-center gap-2 flex-wrap">
                          <h3 className="text-lg font-semibold text-white">{rule.name}</h3>
                          <Badge variant="outline" className={`${severityInfo.color} text-white border-0 text-xs`}>
                            {severityInfo.label}
                          </Badge>
                          {!rule.isActive && (
                            <Badge variant="outline" className="border-slate-700 text-slate-500">Disabled</Badge>
                          )}
                          {muted && (
                            <Badge variant="outline" className="border-orange-700 text-orange-400 flex items-center gap-1">
                              <VolumeX className="h-3 w-3" /> Muted
                            </Badge>
                          )}
                          {snoozed && (
                            <Badge variant="outline" className="border-purple-700 text-purple-400 flex items-center gap-1">
                              <Clock className="h-3 w-3" /> Snoozed ({snoozeRemaining})
                            </Badge>
                          )}
                        </div>
                        <p className="text-slate-400 text-sm">{typeInfo.label}</p>
                        <div className="flex gap-2 mt-2">
                          {Array.isArray(rule.channels) && rule.channels.map((channel: string) => (
                            <Badge key={channel} variant="secondary" className="bg-slate-800 text-slate-300 text-xs">
                              {channel.replace("_", " ")}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center gap-4">
                      <div className="text-right">
                        <p className="text-xs text-slate-500">Triggered {rule.triggerCount || 0} times</p>
                        {rule.lastTriggered && (
                          <p className="text-xs text-slate-500">
                            Last: {new Date(rule.lastTriggered).toLocaleDateString()}
                          </p>
                        )}
                      </div>
                      <Switch
                        data-testid={`switch-toggle-${rule.id}`}
                        checked={rule.isActive ?? true}
                        onCheckedChange={(checked) => toggleMutation.mutate({ id: rule.id, isActive: checked })}
                      />
                      
                      <DropdownMenu>
                        <DropdownMenuTrigger asChild>
                          <Button
                            data-testid={`button-menu-${rule.id}`}
                            variant="ghost"
                            size="sm"
                            className="text-slate-400 hover:text-white"
                          >
                            <MoreVertical className="h-4 w-4" />
                          </Button>
                        </DropdownMenuTrigger>
                        <DropdownMenuContent className="bg-slate-800 border-slate-700" align="end">
                          <DropdownMenuItem
                            data-testid={`menu-edit-${rule.id}`}
                            onClick={() => openEdit(rule)}
                            className="text-slate-300 hover:text-white focus:text-white"
                          >
                            <Edit2 className="h-4 w-4 mr-2" />
                            Edit
                          </DropdownMenuItem>
                          
                          <DropdownMenuSeparator className="bg-slate-700" />
                          
                          {muted ? (
                            <DropdownMenuItem
                              data-testid={`menu-unmute-${rule.id}`}
                              onClick={() => muteMutation.mutate({ id: rule.id, muted: false })}
                              className="text-slate-300 hover:text-white focus:text-white"
                            >
                              <Volume2 className="h-4 w-4 mr-2" />
                              Unmute
                            </DropdownMenuItem>
                          ) : (
                            <DropdownMenuItem
                              data-testid={`menu-mute-${rule.id}`}
                              onClick={() => muteMutation.mutate({ id: rule.id, muted: true })}
                              className="text-slate-300 hover:text-white focus:text-white"
                            >
                              <VolumeX className="h-4 w-4 mr-2" />
                              Mute
                            </DropdownMenuItem>
                          )}
                          
                          {snoozed ? (
                            <DropdownMenuItem
                              data-testid={`menu-unsnooze-${rule.id}`}
                              onClick={() => unsnoozeMutation.mutate(rule.id)}
                              className="text-slate-300 hover:text-white focus:text-white"
                            >
                              <AlertCircle className="h-4 w-4 mr-2" />
                              Unsnooze
                            </DropdownMenuItem>
                          ) : (
                            <>
                              <DropdownMenuSeparator className="bg-slate-700" />
                              <div className="px-2 py-1.5 text-xs text-slate-500">Snooze for...</div>
                              {SNOOZE_OPTIONS.map((option) => (
                                <DropdownMenuItem
                                  key={option.hours}
                                  data-testid={`menu-snooze-${option.hours}h-${rule.id}`}
                                  onClick={() => snoozeMutation.mutate({ id: rule.id, hours: option.hours })}
                                  className="text-slate-300 hover:text-white focus:text-white pl-4"
                                >
                                  <Clock className="h-4 w-4 mr-2" />
                                  {option.label}
                                </DropdownMenuItem>
                              ))}
                            </>
                          )}
                          
                          <DropdownMenuSeparator className="bg-slate-700" />
                          
                          <AlertDialog>
                            <AlertDialogTrigger asChild>
                              <DropdownMenuItem
                                data-testid={`menu-delete-${rule.id}`}
                                className="text-red-400 hover:text-red-300 focus:text-red-300"
                                onSelect={(e) => e.preventDefault()}
                              >
                                <Trash2 className="h-4 w-4 mr-2" />
                                Delete
                              </DropdownMenuItem>
                            </AlertDialogTrigger>
                            <AlertDialogContent className="bg-slate-900 border-slate-700">
                              <AlertDialogHeader>
                                <AlertDialogTitle className="text-white">Delete Alert Rule</AlertDialogTitle>
                                <AlertDialogDescription className="text-slate-400">
                                  Are you sure you want to delete "{rule.name}"? This action cannot be undone.
                                </AlertDialogDescription>
                              </AlertDialogHeader>
                              <AlertDialogFooter>
                                <AlertDialogCancel className="bg-slate-800 border-slate-700 text-white">Cancel</AlertDialogCancel>
                                <AlertDialogAction
                                  data-testid="button-confirm-delete-alert"
                                  onClick={() => deleteMutation.mutate(rule.id)}
                                  className="bg-red-600 hover:bg-red-700"
                                >
                                  Delete
                                </AlertDialogAction>
                              </AlertDialogFooter>
                            </AlertDialogContent>
                          </AlertDialog>
                        </DropdownMenuContent>
                      </DropdownMenu>
                    </div>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      )}

      <Dialog open={!!editingRule} onOpenChange={(open) => !open && setEditingRule(null)}>
        <DialogContent className="bg-slate-900 border-slate-700 max-w-lg max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="text-white">Edit Alert Rule</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <Label className="text-slate-300">Name</Label>
              <Input
                data-testid="input-edit-alert-name"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                className="bg-slate-800 border-slate-700 text-white"
              />
            </div>
            
            <div className="space-y-2">
              <Label className="text-slate-300">Severity</Label>
              <Select
                value={formData.severity}
                onValueChange={(value) => setFormData({ ...formData, severity: value })}
              >
                <SelectTrigger data-testid="select-edit-severity" className="bg-slate-800 border-slate-700 text-white">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent className="bg-slate-800 border-slate-700">
                  {SEVERITIES.map((sev) => (
                    <SelectItem key={sev.value} value={sev.value}>
                      <div className="flex items-center gap-2">
                        <div className={`w-2 h-2 rounded-full ${sev.color}`} />
                        {sev.label}
                      </div>
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            
            <div className="space-y-2">
              <Label className="text-slate-300">Cooldown (minutes)</Label>
              <Input
                type="number"
                value={formData.cooldownMinutes}
                onChange={(e) => setFormData({ ...formData, cooldownMinutes: Number(e.target.value) })}
                className="bg-slate-800 border-slate-700 text-white"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-slate-300">Notification Channels</Label>
              <div className="space-y-2">
                {CHANNELS.map((channel) => (
                  <div key={channel.value} className="flex items-center space-x-2">
                    <Checkbox
                      id={`edit-${channel.value}`}
                      checked={formData.channels.includes(channel.value)}
                      onCheckedChange={() => handleChannelToggle(channel.value)}
                      className="border-slate-600"
                    />
                    <label htmlFor={`edit-${channel.value}`} className="text-slate-300 text-sm">
                      {channel.label}
                    </label>
                  </div>
                ))}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button
              data-testid="button-update-alert"
              onClick={handleSubmit}
              disabled={!formData.name || updateMutation.isPending}
              className="bg-blue-600 hover:bg-blue-700"
            >
              {updateMutation.isPending ? "Saving..." : "Save Changes"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
