import { useState, useEffect, useCallback } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useWebSocket } from './use-websocket';
import { useToast } from './use-toast';
import { useSignals } from './use-signals';

interface Signal {
  id: string;
  timestamp?: string;
  entityType: string;
  entityId: string;
  signalType: string;
  severity: number;
  title: string;
  description?: string;
  metadata?: any;
  isActive: boolean;
}

interface SignalMessage {
  type: 'new_signal';
  data: Signal;
}

interface DelayAlertMessage {
  type: 'delay_alert';
  data: {
    vessel: string;
    port: string;
    delayHours: number;
    cargoVolume: number;
    severity: string;
  };
}

export function useNotifications() {
  const [unreadCount, setUnreadCount] = useState(0);
  const [isNotificationPanelOpen, setIsNotificationPanelOpen] = useState(false);
  const queryClient = useQueryClient();
  const { toast } = useToast();

  // Fetch active signals from API
  const { data: signals = [], isLoading, error } = useSignals();

  // Handle new signals and delay alerts from WebSocket
  const handleWebSocketMessage = useCallback((message: any) => {
    if (message.type === 'new_signal') {
      const signalMessage = message as SignalMessage;
      const newSignal = signalMessage.data;
      
      console.log('Received new signal:', newSignal);

      // Update the signals query cache
      queryClient.setQueryData(['/api/signals'], (oldSignals: Signal[] = []) => {
        // Check if signal already exists to avoid duplicates
        const exists = oldSignals.some(signal => signal.id === newSignal.id);
        if (exists) return oldSignals;
        
        return [newSignal, ...oldSignals];
      });

      // Increment unread count
      setUnreadCount(prev => prev + 1);

      // Show toast notification
      const severityColor = getSeverityColor(newSignal.severity);
      toast({
        title: newSignal.title,
        description: newSignal.description,
        variant: newSignal.severity >= 4 ? "destructive" : "default",
        duration: newSignal.severity >= 4 ? 8000 : 5000, // Longer duration for high severity
      });
    } else if (message.type === 'delay_alert') {
      const delayAlert = message as DelayAlertMessage;
      const { vessel, port, delayHours, cargoVolume, severity } = delayAlert.data;
      
      console.log('Received delay alert:', delayAlert.data);

      // Show toast notification for delay alert
      toast({
        title: `⚠️ Port Delay Alert: ${port}`,
        description: `${vessel} delayed ${delayHours.toFixed(1)}h with ${(cargoVolume / 1000).toFixed(1)}K tons cargo`,
        variant: severity === 'critical' || severity === 'high' ? "destructive" : "default",
        duration: 8000,
      });

      // Increment unread count for delay alerts too
      setUnreadCount(prev => prev + 1);
      
      // Invalidate delay-related queries to refresh data
      queryClient.invalidateQueries({ queryKey: ['/api/market/delays/impact'] });
      queryClient.invalidateQueries({ queryKey: ['/api/vessels/delays'] });
    }
  }, [queryClient, toast]);

  // Initialize WebSocket connection
  useWebSocket('/ws', {
    onMessage: handleWebSocketMessage,
    onConnect: () => {
      console.log('Notifications WebSocket connected');
    },
    onDisconnect: () => {
      console.log('Notifications WebSocket disconnected');
    }
  });

  // Initialize unread count based on signals
  useEffect(() => {
    if (signals.length > 0) {
      // For now, consider all signals as unread
      // In a real app, you'd track read status per user
      setUnreadCount(signals.length);
    }
  }, [signals]);

  const getSeverityColor = (severity: number) => {
    switch (severity) {
      case 1: return 'blue';
      case 2: return 'green';
      case 3: return 'yellow';
      case 4: return 'orange';
      case 5: return 'red';
      default: return 'gray';
    }
  };

  const getSeverityLabel = (severity: number) => {
    switch (severity) {
      case 1: return 'Info';
      case 2: return 'Low';
      case 3: return 'Medium';
      case 4: return 'High';
      case 5: return 'Critical';
      default: return 'Unknown';
    }
  };

  const markAsRead = useCallback((signalId?: string) => {
    if (signalId) {
      // Mark specific notification as read
      queryClient.setQueryData(['/api/signals'], (oldSignals: Signal[] = []) => {
        return oldSignals.filter(signal => signal.id !== signalId);
      });
      setUnreadCount(prev => Math.max(0, prev - 1));
    } else {
      // Mark all as read - clear all signals
      queryClient.setQueryData(['/api/signals'], () => []);
      setUnreadCount(0);
    }
  }, [queryClient]);

  const toggleNotificationPanel = useCallback(() => {
    setIsNotificationPanelOpen(prev => !prev);
  }, []);

  const closeNotificationPanel = useCallback(() => {
    setIsNotificationPanelOpen(false);
  }, []);

  return {
    signals,
    unreadCount,
    isLoading,
    error,
    isNotificationPanelOpen,
    getSeverityColor,
    getSeverityLabel,
    markAsRead,
    toggleNotificationPanel,
    closeNotificationPanel,
  };
}