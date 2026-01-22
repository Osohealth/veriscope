import { useEffect, useRef, useState, useCallback } from 'react';

const MESSAGE_SCHEMA_VERSION = '1.0.0';

interface WsMessage {
  version: string;
  type: string;
  topic?: string;
  payload: any;
  timestamp: string;
  messageId: string;
}

interface UseWebSocketOptions {
  onMessage?: (message: WsMessage) => void;
  onConnect?: (clientId: string) => void;
  onDisconnect?: () => void;
  topics?: string[];
  maxReconnectAttempts?: number;
  initialReconnectDelay?: number;
  maxReconnectDelay?: number;
}

export function useWebSocket(url: string, options: UseWebSocketOptions = {}) {
  const {
    onMessage,
    onConnect,
    onDisconnect,
    topics = [],
    maxReconnectAttempts = 10,
    initialReconnectDelay = 1000,
    maxReconnectDelay = 30000
  } = options;

  const [isConnected, setIsConnected] = useState(false);
  const [connectionError, setConnectionError] = useState<string | null>(null);
  const [reconnectAttempt, setReconnectAttempt] = useState(0);
  const [clientId, setClientId] = useState<string | null>(null);
  
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const pingIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const mountedRef = useRef(true);
  const topicsRef = useRef(topics);

  topicsRef.current = topics;

  const getReconnectDelay = useCallback((attempt: number) => {
    const delay = Math.min(
      initialReconnectDelay * Math.pow(2, attempt),
      maxReconnectDelay
    );
    return delay + Math.random() * 1000;
  }, [initialReconnectDelay, maxReconnectDelay]);

  const sendMessage = useCallback((type: string, payload: any = {}) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      const message = {
        version: MESSAGE_SCHEMA_VERSION,
        type,
        ...payload
      };
      wsRef.current.send(JSON.stringify(message));
    }
  }, []);

  const subscribe = useCallback((newTopics: string[]) => {
    sendMessage('subscribe', { topics: newTopics });
  }, [sendMessage]);

  const unsubscribe = useCallback((topicsToRemove: string[]) => {
    sendMessage('unsubscribe', { topics: topicsToRemove });
  }, [sendMessage]);

  const connect = useCallback(() => {
    try {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}${url}`;
      
      wsRef.current = new WebSocket(wsUrl);

      wsRef.current.onopen = () => {
        if (!mountedRef.current) return;
        
        console.log('WebSocket connected');
        setIsConnected(true);
        setConnectionError(null);
        setReconnectAttempt(0);
        
        pingIntervalRef.current = setInterval(() => {
          sendMessage('ping');
        }, 30000);
      };

      wsRef.current.onmessage = (event) => {
        if (!mountedRef.current) return;
        
        try {
          const message: WsMessage = JSON.parse(event.data);
          
          if (message.type === 'connected') {
            setClientId(message.payload.clientId);
            onConnect?.(message.payload.clientId);
            
            if (topicsRef.current.length > 0) {
              sendMessage('subscribe', { topics: topicsRef.current });
            }
          } else if (message.type === 'error') {
            console.warn('WebSocket error:', message.payload);
            if (message.payload.code === 'RATE_LIMITED') {
              setConnectionError(`Rate limited. Retry in ${message.payload.retryAfterMs}ms`);
            }
          } else if (message.type !== 'pong' && message.type !== 'subscribed' && message.type !== 'unsubscribed') {
            onMessage?.(message);
          }
        } catch (error) {
          console.error('Failed to parse WebSocket message:', error);
        }
      };

      wsRef.current.onclose = () => {
        if (!mountedRef.current) return;
        
        console.log('WebSocket disconnected');
        setIsConnected(false);
        setClientId(null);
        onDisconnect?.();

        if (pingIntervalRef.current) {
          clearInterval(pingIntervalRef.current);
          pingIntervalRef.current = null;
        }

        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current);
        }
        
        setReconnectAttempt(prev => {
          const newAttempt = prev + 1;
          
          if (newAttempt <= maxReconnectAttempts) {
            const delay = getReconnectDelay(newAttempt);
            console.log(`Reconnecting in ${Math.round(delay)}ms (attempt ${newAttempt}/${maxReconnectAttempts})`);
            
            reconnectTimeoutRef.current = setTimeout(() => {
              if (mountedRef.current) {
                connect();
              }
            }, delay);
          } else {
            setConnectionError('Max reconnection attempts reached');
          }
          
          return newAttempt;
        });
      };

      wsRef.current.onerror = (error) => {
        if (!mountedRef.current) return;
        console.error('WebSocket error:', error);
      };

    } catch (error) {
      console.error('Failed to create WebSocket connection:', error);
      setConnectionError('Failed to connect');
    }
  }, [url, onConnect, onDisconnect, onMessage, maxReconnectAttempts, getReconnectDelay, sendMessage]);

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
      reconnectTimeoutRef.current = null;
    }
    
    if (pingIntervalRef.current) {
      clearInterval(pingIntervalRef.current);
      pingIntervalRef.current = null;
    }
    
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    
    setIsConnected(false);
    setClientId(null);
  }, []);

  const reconnect = useCallback(() => {
    disconnect();
    setReconnectAttempt(0);
    setTimeout(() => connect(), 100);
  }, [disconnect, connect]);

  useEffect(() => {
    mountedRef.current = true;
    connect();

    return () => {
      mountedRef.current = false;
      disconnect();
    };
  }, [url]);

  return {
    isConnected,
    connectionError,
    reconnectAttempt,
    clientId,
    sendMessage,
    subscribe,
    unsubscribe,
    disconnect,
    reconnect
  };
}
