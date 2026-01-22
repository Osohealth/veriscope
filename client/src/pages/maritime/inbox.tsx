import { useQuery, useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { ChevronLeft, Inbox, AlertCircle, Bell, Archive, Mail, MailOpen, Star } from "lucide-react";
import { useLocation } from "wouter";
import type { Communication } from "@shared/schema";
import { Skeleton } from "@/components/ui/skeleton";
import { apiRequest, queryClient } from "@/lib/queryClient";
import { useToast } from "@/hooks/use-toast";

export default function MaritimeInbox() {
  const [, setLocation] = useLocation();
  const [selectedMessageType, setSelectedMessageType] = useState<string>("all");
  const [selectedPriority, setSelectedPriority] = useState<string>("all");
  const { toast } = useToast();

  // Fetch communications
  const { data: communications = [], isLoading } = useQuery<Communication[]>({
    queryKey: ["/api/communications"]
  });

  // Filter by message type
  const typeFilteredCommunications = selectedMessageType === "all" 
    ? communications 
    : communications.filter(c => c.messageType === selectedMessageType);

  // Filter by priority
  const filteredCommunications = selectedPriority === "all" 
    ? typeFilteredCommunications 
    : typeFilteredCommunications.filter(c => c.priority === selectedPriority);

  // Calculate statistics from all communications (not filtered)
  const stats = {
    totalMessages: communications.length,
    unreadMessages: communications.filter(c => !c.isRead).length,
    alertMessages: communications.filter(c => c.messageType === 'alert').length,
    criticalMessages: communications.filter(c => c.priority === 'critical').length,
    archivedMessages: communications.filter(c => c.isArchived).length,
    notificationMessages: communications.filter(c => c.messageType === 'notification').length
  };

  // Message type distribution for analytics
  const messageTypeDistribution = communications.reduce((acc, comm) => {
    const type = comm.messageType || 'unknown';
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  // Priority distribution
  const priorityDistribution = communications.reduce((acc, comm) => {
    const priority = comm.priority || 'normal';
    acc[priority] = (acc[priority] || 0) + 1;
    return acc;
  }, {} as Record<string, number>);

  // Mark as read mutation
  const markAsReadMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiRequest("PATCH", `/api/communications/${id}/read`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/communications"] });
      toast({
        title: "Message marked as read"
      });
    }
  });

  // Archive mutation
  const archiveMutation = useMutation({
    mutationFn: async (id: string) => {
      return apiRequest("PATCH", `/api/communications/${id}/archive`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["/api/communications"] });
      toast({
        title: "Message archived"
      });
    }
  });

  const getPriorityColor = (priority?: string | null) => {
    switch (priority) {
      case 'critical': return 'bg-red-500';
      case 'high': return 'bg-orange-500';
      case 'normal': return 'bg-blue-500';
      case 'low': return 'bg-gray-500';
      default: return 'bg-gray-500';
    }
  };

  const getPriorityBadgeVariant = (priority?: string | null) => {
    switch (priority) {
      case 'critical': return 'destructive';
      case 'high': return 'destructive';
      default: return 'secondary';
    }
  };

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Button 
              variant="ghost" 
              size="icon" 
              onClick={() => setLocation("/maritime")}
              data-testid="button-back"
            >
              <ChevronLeft className="h-5 w-5" />
            </Button>
            <div>
              <h1 className="text-3xl font-bold" data-testid="text-page-title">Maritime Inbox</h1>
              <p className="text-muted-foreground">Email hub and communications overlay</p>
            </div>
          </div>
        </div>

        {/* Statistics Cards */}
        <div className="grid gap-4 md:grid-cols-3 lg:grid-cols-6">
          <Card data-testid="card-total-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Messages</CardTitle>
              <Inbox className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-messages">{stats.totalMessages}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-unread-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Unread</CardTitle>
              <Mail className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-unread-messages">{stats.unreadMessages}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-alert-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Alerts</CardTitle>
              <Bell className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-alert-messages">{stats.alertMessages}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-critical-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Critical</CardTitle>
              <AlertCircle className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-critical-messages">{stats.criticalMessages}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-archived-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Archived</CardTitle>
              <Archive className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-archived-messages">{stats.archivedMessages}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-notification-messages">
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Notifications</CardTitle>
              <Star className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-notification-messages">{stats.notificationMessages}</div>
            </CardContent>
          </Card>
        </div>

        {/* Filters */}
        <div className="flex gap-4">
          <Select value={selectedMessageType} onValueChange={setSelectedMessageType}>
            <SelectTrigger className="w-[200px]" data-testid="select-message-type">
              <SelectValue placeholder="Filter by message type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="alert">Alerts</SelectItem>
              <SelectItem value="notification">Notifications</SelectItem>
              <SelectItem value="system">System</SelectItem>
              <SelectItem value="user_message">User Messages</SelectItem>
            </SelectContent>
          </Select>

          <Select value={selectedPriority} onValueChange={setSelectedPriority}>
            <SelectTrigger className="w-[200px]" data-testid="select-priority-filter">
              <SelectValue placeholder="Filter by priority" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Priorities</SelectItem>
              <SelectItem value="critical">Critical</SelectItem>
              <SelectItem value="high">High</SelectItem>
              <SelectItem value="normal">Normal</SelectItem>
              <SelectItem value="low">Low</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Content Tabs */}
        <Tabs defaultValue="inbox" className="space-y-4">
          <TabsList>
            <TabsTrigger value="inbox" data-testid="tab-inbox">Inbox</TabsTrigger>
            <TabsTrigger value="analytics" data-testid="tab-analytics">Analytics</TabsTrigger>
          </TabsList>

          <TabsContent value="inbox" className="space-y-4">
            {isLoading ? (
              <div className="space-y-4">
                {[1, 2, 3].map(i => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : filteredCommunications.length === 0 ? (
              <Card>
                <CardContent className="pt-6">
                  <p className="text-center text-muted-foreground">No messages found</p>
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-4">
                {filteredCommunications.map(comm => (
                  <Card 
                    key={comm.id} 
                    data-testid={`card-message-${comm.id}`}
                    className={comm.isRead ? "opacity-60" : ""}
                  >
                    <CardHeader>
                      <div className="flex items-start justify-between">
                        <div className="flex items-start gap-3 flex-1">
                          <div className={`w-1 h-full rounded ${getPriorityColor(comm.priority)}`} />
                          <div className="flex-1">
                            <div className="flex items-center gap-2 mb-2">
                              <CardTitle className="text-lg" data-testid={`text-subject-${comm.id}`}>
                                {comm.subject}
                              </CardTitle>
                              <Badge variant={getPriorityBadgeVariant(comm.priority)}>
                                {comm.priority || 'normal'}
                              </Badge>
                              <Badge variant="outline">
                                {comm.messageType}
                              </Badge>
                              {!comm.isRead && (
                                <Badge variant="default" className="bg-blue-500">
                                  New
                                </Badge>
                              )}
                            </div>
                            {comm.category && (
                              <p className="text-xs text-muted-foreground mb-2">
                                Category: {comm.category.replace('_', ' ')}
                              </p>
                            )}
                            {comm.body && (
                              <p className="text-sm text-muted-foreground" data-testid={`text-body-${comm.id}`}>
                                {comm.body}
                              </p>
                            )}
                            <p className="text-xs text-muted-foreground mt-2">
                              {comm.createdAt ? new Date(comm.createdAt).toLocaleString() : 'N/A'}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center gap-2">
                          {!comm.isRead && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => markAsReadMutation.mutate(comm.id)}
                              disabled={markAsReadMutation.isPending}
                              data-testid={`button-mark-read-${comm.id}`}
                            >
                              <MailOpen className="h-4 w-4" />
                            </Button>
                          )}
                          {!comm.isArchived && (
                            <Button
                              variant="ghost"
                              size="sm"
                              onClick={() => archiveMutation.mutate(comm.id)}
                              disabled={archiveMutation.isPending}
                              data-testid={`button-archive-${comm.id}`}
                            >
                              <Archive className="h-4 w-4" />
                            </Button>
                          )}
                        </div>
                      </div>
                    </CardHeader>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <div className="grid gap-4 md:grid-cols-2">
              <Card>
                <CardHeader>
                  <CardTitle>Message Type Distribution</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {Object.entries(messageTypeDistribution).map(([type, count]) => (
                      <div key={type} className="flex items-center justify-between">
                        <span className="text-sm capitalize">{type.replace('_', ' ')}</span>
                        <div className="flex items-center gap-2">
                          <div className="w-32 h-2 bg-secondary rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-primary" 
                              style={{ width: stats.totalMessages > 0 ? `${(count / stats.totalMessages) * 100}%` : '0%' }}
                            />
                          </div>
                          <span className="text-sm font-medium w-12 text-right">{count}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle>Priority Distribution</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {Object.entries(priorityDistribution).map(([priority, count]) => (
                      <div key={priority} className="flex items-center justify-between">
                        <span className="text-sm capitalize">{priority}</span>
                        <div className="flex items-center gap-2">
                          <div className="w-32 h-2 bg-secondary rounded-full overflow-hidden">
                            <div 
                              className="h-full bg-primary" 
                              style={{ width: stats.totalMessages > 0 ? `${(count / stats.totalMessages) * 100}%` : '0%' }}
                            />
                          </div>
                          <span className="text-sm font-medium w-12 text-right">{count}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              <Card className="md:col-span-2">
                <CardHeader>
                  <CardTitle>Communication Insights</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Read Rate</p>
                      <p className="text-2xl font-bold">
                        {stats.totalMessages > 0 
                          ? Math.round(((stats.totalMessages - stats.unreadMessages) / stats.totalMessages) * 100)
                          : 0}%
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Archive Rate</p>
                      <p className="text-2xl font-bold">
                        {stats.totalMessages > 0 
                          ? Math.round((stats.archivedMessages / stats.totalMessages) * 100)
                          : 0}%
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Alert Rate</p>
                      <p className="text-2xl font-bold">
                        {stats.totalMessages > 0 
                          ? Math.round((stats.alertMessages / stats.totalMessages) * 100)
                          : 0}%
                      </p>
                    </div>
                    <div className="text-center">
                      <p className="text-sm text-muted-foreground">Critical Rate</p>
                      <p className="text-2xl font-bold">
                        {stats.totalMessages > 0 
                          ? Math.round((stats.criticalMessages / stats.totalMessages) * 100)
                          : 0}%
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
