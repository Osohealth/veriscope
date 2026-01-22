import type { Express } from "express";
import { createServer, type Server } from "http";
import { WebSocketServer } from "ws";
import swaggerUi from "swagger-ui-express";
import { storage } from "./storage";
import { aisService } from "./services/aisService";
import { signalsService } from "./services/signalsService";
import { predictionService } from "./services/predictionService";
import { delayService } from "./services/delayService";
import { mockDataService } from "./services/mockDataService";
import { portCallService } from "./services/portCallService";
import { rotterdamDataService } from "./services/rotterdamDataService";
import { authService } from "./services/authService";
import { sessionService } from "./services/sessionService";
import { auditService } from "./services/auditService";
import { requestTrackingMiddleware, metricsCollector, getHealthStatus, setWsHealth, setDbHealth } from "./middleware/observability";
import { authRateLimiter, apiRateLimiter } from "./middleware/rateLimiter";
import { wsManager } from "./services/wsManager";
import { authenticate, optionalAuth, requirePermission, requireRole, requireAdmin, requireSelfOrAdmin, logAccess } from "./middleware/rbac";
import { cacheService, CACHE_KEYS, CACHE_TTL } from "./services/cacheService";
import { parsePaginationParams, paginateArray, parseGeoQueryParams, filterByGeoRadius } from "./utils/pagination";
import { openApiSpec } from "./openapi";
import { getAuthenticatedUser, createRepository, listRepositories } from "./services/githubService";

export async function registerRoutes(app: Express): Promise<Server> {
  const httpServer = createServer(app);
  
  // Apply observability middleware
  app.use(requestTrackingMiddleware);
  
  // ===== HEALTH & OBSERVABILITY ENDPOINTS =====
  
  app.get('/health', (req, res) => {
    const status = getHealthStatus();
    res.status(status.status === 'healthy' ? 200 : status.status === 'degraded' ? 200 : 503).json(status);
  });

  app.get('/ready', async (req, res) => {
    try {
      await storage.getPorts();
      setDbHealth(true);
      res.json({ status: 'ready', timestamp: new Date().toISOString() });
    } catch {
      setDbHealth(false);
      res.status(503).json({ status: 'not_ready', timestamp: new Date().toISOString() });
    }
  });

  app.get('/live', (req, res) => {
    res.json({ status: 'alive', timestamp: new Date().toISOString() });
  });

  // Data status endpoint - shows counts for debugging production data
  app.get('/api/status/data', async (req, res) => {
    try {
      const [vessels, ports] = await Promise.all([
        storage.getVessels(),
        storage.getPorts()
      ]);
      res.json({
        status: 'ok',
        timestamp: new Date().toISOString(),
        counts: {
          vessels: vessels.length,
          ports: ports.length
        },
        environment: process.env.NODE_ENV || 'development'
      });
    } catch (error: any) {
      res.status(500).json({
        status: 'error',
        error: error.message,
        timestamp: new Date().toISOString()
      });
    }
  });

  app.get('/metrics', (req, res) => {
    res.json(metricsCollector.getMetrics());
  });

  // ===== GITHUB INTEGRATION ENDPOINTS =====
  
  app.get('/api/github/user', async (req, res) => {
    try {
      const user = await getAuthenticatedUser();
      res.json({
        login: user.login,
        name: user.name,
        avatar_url: user.avatar_url,
        html_url: user.html_url
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.get('/api/github/repos', async (req, res) => {
    try {
      const repos = await listRepositories();
      res.json(repos.map(r => ({
        name: r.name,
        full_name: r.full_name,
        html_url: r.html_url,
        private: r.private,
        updated_at: r.updated_at
      })));
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });

  app.post('/api/github/repos', async (req, res) => {
    try {
      const { name, description, isPrivate } = req.body;
      if (!name) {
        return res.status(400).json({ error: 'Repository name is required' });
      }
      const repo = await createRepository(name, description || '', isPrivate || false);
      res.json({
        name: repo.name,
        full_name: repo.full_name,
        html_url: repo.html_url,
        clone_url: repo.clone_url,
        ssh_url: repo.ssh_url
      });
    } catch (error: any) {
      res.status(500).json({ error: error.message });
    }
  });
  
  // OpenAPI documentation
  app.use('/docs', swaggerUi.serve, swaggerUi.setup(openApiSpec));
  app.get('/openapi.json', (req, res) => {
    res.json(openApiSpec);
  });
  
  // WebSocket setup with message schema versioning, throttling, and topics
  const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
  wsManager.initialize(wss);
  
  // WebSocket stats endpoint
  app.get('/api/ws/stats', authenticate, requireRole('admin', 'operator'), (req, res) => {
    res.json(wsManager.getStats());
  });

  // AIS stream status endpoint
  app.get('/api/ais/status', authenticate, requireRole('admin', 'operator'), (req, res) => {
    res.json(aisService.getStatus());
  });

  // ===== AUTHENTICATION ENDPOINTS =====
  
  app.post('/api/auth/register', authRateLimiter, async (req, res) => {
    try {
      const { email, password, name, full_name, organization_name } = req.body;
      const fullName = full_name || name;
      
      if (!email || !password || !fullName) {
        return res.status(400).json({ error: 'Email, password, and name are required' });
      }
      
      if (password.length < 6) {
        return res.status(400).json({ error: 'Password must be at least 6 characters' });
      }
      
      const result = await authService.register(email, password, fullName, organization_name);
      
      if (!result.success) {
        return res.status(400).json({ error: result.error });
      }
      
      res.status(201).json({ message: 'Registration successful', ...result.data });
    } catch (error: any) {
      console.error('Registration error:', error);
      res.status(500).json({ error: 'Registration failed' });
    }
  });

  app.post('/api/auth/login', authRateLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      
      if (!email || !password) {
        return res.status(400).json({ error: 'Email and password are required' });
      }
      
      const result = await authService.login(email, password);
      
      if (!result.success || !result.data) {
        await auditService.logLogin('', false, req, result.error);
        return res.status(401).json({ error: result.error });
      }
      
      await auditService.logLogin(result.data.user.id, true, req);
      
      res.json({ 
        message: 'Login successful', 
        ...result.data
      });
    } catch (error: any) {
      console.error('Login error:', error);
      res.status(500).json({ error: 'Login failed' });
    }
  });

  app.post('/api/auth/refresh', async (req, res) => {
    try {
      const { refreshToken, refresh_token } = req.body;
      const token = refreshToken || refresh_token;
      
      if (!token) {
        return res.status(400).json({ error: 'Refresh token required' });
      }
      
      const result = await authService.refreshTokens(token);
      
      if (!result.success || !result.data) {
        return res.status(401).json({ error: result.error || 'Invalid or expired refresh token' });
      }
      
      res.json(result.data);
    } catch (error: any) {
      console.error('Token refresh error:', error);
      res.status(500).json({ error: 'Token refresh failed' });
    }
  });

  app.post('/api/auth/logout', async (req, res) => {
    try {
      const { refreshToken } = req.body;
      const authHeader = req.headers.authorization;
      
      if (refreshToken) {
        sessionService.revokeRefreshToken(refreshToken);
      }
      
      if (authHeader?.startsWith('Bearer ')) {
        const token = authHeader.substring(7);
        const payload = sessionService.verifyToken(token);
        if (payload) {
          await auditService.logLogout(payload.userId, req);
        }
      }
      
      res.json({ message: 'Logged out successfully' });
    } catch (error: any) {
      console.error('Logout error:', error);
      res.status(500).json({ error: 'Logout failed' });
    }
  });

  // ===== PHASE ONE V1 API ENDPOINTS =====
  
  // V1 Auth - Register
  app.post('/v1/auth/register', authRateLimiter, async (req, res) => {
    try {
      const { email, password, full_name, organization_name } = req.body;
      
      if (!email || !password || !full_name) {
        return res.status(400).json({ error: 'Email, password, and full_name are required' });
      }
      
      if (password.length < 8) {
        return res.status(400).json({ error: 'Password must be at least 8 characters' });
      }
      
      const result = await authService.register(email, password, full_name, organization_name);
      
      if (!result.success || !result.data) {
        return res.status(400).json({ error: result.error });
      }
      
      res.status(201).json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Registration error:', error);
      res.status(500).json({ error: 'Registration failed' });
    }
  });

  // V1 Auth - Login
  app.post('/v1/auth/login', authRateLimiter, async (req, res) => {
    try {
      const { email, password } = req.body;
      
      if (!email || !password) {
        return res.status(400).json({ error: 'Email and password are required' });
      }
      
      const result = await authService.login(email, password);
      
      if (!result.success || !result.data) {
        await auditService.logLogin('', false, req, result.error);
        return res.status(401).json({ error: result.error });
      }
      
      await auditService.logLogin(result.data.user.id, true, req);
      
      res.json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Login error:', error);
      res.status(500).json({ error: 'Login failed' });
    }
  });

  // V1 Auth - Refresh Token
  app.post('/v1/auth/refresh', async (req, res) => {
    try {
      const { refresh_token } = req.body;
      
      if (!refresh_token) {
        return res.status(400).json({ error: 'refresh_token is required' });
      }
      
      const result = await authService.refreshTokens(refresh_token);
      
      if (!result.success || !result.data) {
        return res.status(401).json({ error: result.error || 'Invalid or expired refresh token' });
      }
      
      res.json({
        access_token: result.data.accessToken,
        refresh_token: result.data.refreshToken,
        token_type: result.data.tokenType,
        user: {
          id: result.data.user.id,
          email: result.data.user.email,
          full_name: result.data.user.fullName,
          role: result.data.user.role,
        }
      });
    } catch (error: any) {
      console.error('V1 Token refresh error:', error);
      res.status(500).json({ error: 'Token refresh failed' });
    }
  });

  // V1 Ports - List with search and filtering
  app.get('/v1/ports', optionalAuth, async (req, res) => {
    try {
      const { q, country_code, limit = '50' } = req.query;
      const ports = await storage.getPorts();
      
      let filtered = ports;
      
      if (q) {
        const search = String(q).toLowerCase();
        filtered = filtered.filter(p => 
          p.name.toLowerCase().includes(search) || 
          p.code.toLowerCase().includes(search) ||
          (p.unlocode && p.unlocode.toLowerCase().includes(search))
        );
      }
      
      if (country_code) {
        const cc = String(country_code).toUpperCase();
        filtered = filtered.filter(p => p.countryCode === cc || p.country.toUpperCase().includes(cc));
      }
      
      const limitNum = Math.min(parseInt(String(limit)) || 50, 500);
      filtered = filtered.slice(0, limitNum);
      
      res.json({
        items: filtered.map(p => ({
          id: p.id,
          name: p.name,
          unlocode: p.unlocode || p.code,
          country_code: p.countryCode || p.country?.substring(0, 2).toUpperCase(),
          latitude: parseFloat(String(p.latitude)),
          longitude: parseFloat(String(p.longitude)),
          timezone: p.timezone || 'UTC',
        })),
        total: filtered.length,
      });
    } catch (error) {
      console.error('V1 ports list error:', error);
      res.status(500).json({ error: 'Failed to fetch ports' });
    }
  });

  // V1 Ports - Get by ID with 7-day KPIs (optimized SQL)
  app.get('/v1/ports/:port_id', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const ports = await storage.getPorts();
      const port = ports.find(p => p.id === port_id);
      
      if (!port) {
        return res.status(404).json({ error: 'Port not found' });
      }
      
      const { getPortMetrics7d } = await import('./services/portStatisticsService');
      const metrics = await getPortMetrics7d(port_id);
      
      res.json({
        id: port.id,
        name: port.name,
        unlocode: port.unlocode || port.code,
        country_code: port.countryCode || port.country?.substring(0, 2).toUpperCase(),
        latitude: parseFloat(String(port.latitude)),
        longitude: parseFloat(String(port.longitude)),
        timezone: port.timezone || 'UTC',
        metrics_7d: {
          arrivals: metrics.arrivals,
          departures: metrics.departures,
          unique_vessels: metrics.unique_vessels,
          avg_dwell_hours: metrics.avg_dwell_hours,
          median_dwell_hours: metrics.median_dwell_hours,
          open_calls: metrics.open_calls,
        },
      });
    } catch (error) {
      console.error('V1 port detail error:', error);
      res.status(500).json({ error: 'Failed to fetch port' });
    }
  });

  // V1 Ports - Get port calls
  app.get('/v1/ports/:port_id/calls', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const { start_time, end_time, limit = '100' } = req.query;
      
      const startDate = start_time ? new Date(String(start_time)) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
      const endDate = end_time ? new Date(String(end_time)) : new Date();
      
      const portCalls = await storage.getPortCallsByPort(port_id, startDate, endDate);
      const limitNum = Math.min(parseInt(String(limit)) || 100, 500);
      
      const items = await Promise.all(
        portCalls.slice(0, limitNum).map(async (call) => {
          const vessel = await storage.getVessel(call.vesselId);
          
          let dwellHours = null;
          if (call.departureTime && call.arrivalTime) {
            const arrival = new Date(call.arrivalTime);
            const departure = new Date(call.departureTime);
            dwellHours = Math.round((departure.getTime() - arrival.getTime()) / (1000 * 60 * 60) * 10) / 10;
          }
          
          return {
            id: call.id,
            vessel_id: call.vesselId,
            vessel_name: vessel?.name || 'Unknown',
            arrival_time_utc: call.arrivalTime,
            departure_time_utc: call.departureTime,
            dwell_hours: dwellHours,
          };
        })
      );
      
      res.json({ items });
    } catch (error) {
      console.error('V1 port calls error:', error);
      res.status(500).json({ error: 'Failed to fetch port calls' });
    }
  });

  // V1 Ports - Daily arrivals/departures time series (last 7 days)
  app.get('/v1/ports/:port_id/daily_stats', optionalAuth, async (req, res) => {
    try {
      const { port_id } = req.params;
      const { getDailyArrivalsTimeSeries } = await import('./services/portStatisticsService');
      const timeSeries = await getDailyArrivalsTimeSeries(port_id);
      res.json({ port_id, items: timeSeries });
    } catch (error) {
      console.error('V1 port daily stats error:', error);
      res.status(500).json({ error: 'Failed to fetch daily statistics' });
    }
  });

  // V1 Ports - Top busy ports (last 7 days)
  app.get('/v1/ports/stats/top_busy', optionalAuth, async (req, res) => {
    try {
      const { limit = '20' } = req.query;
      const { getTopBusyPorts } = await import('./services/portStatisticsService');
      const topPorts = await getTopBusyPorts(Math.min(parseInt(String(limit)) || 20, 100));
      res.json({ items: topPorts });
    } catch (error) {
      console.error('V1 top busy ports error:', error);
      res.status(500).json({ error: 'Failed to fetch top busy ports' });
    }
  });

  // V1 Vessels - List with filters
  app.get('/v1/vessels', optionalAuth, async (req, res) => {
    try {
      const { mmsi, imo, name } = req.query;
      const vessels = await storage.getVessels();
      
      let filtered = vessels;
      
      if (mmsi) {
        filtered = filtered.filter(v => v.mmsi === String(mmsi));
      } else if (imo) {
        filtered = filtered.filter(v => v.imo === String(imo));
      } else if (name) {
        const search = String(name).toLowerCase();
        filtered = filtered.filter(v => v.name.toLowerCase().includes(search));
      }
      
      res.json({
        items: filtered.map(v => ({
          id: v.id,
          mmsi: v.mmsi,
          imo: v.imo,
          name: v.name,
          vessel_type: v.vesselType,
          flag: v.flag,
        })),
      });
    } catch (error) {
      console.error('V1 vessels list error:', error);
      res.status(500).json({ error: 'Failed to fetch vessels' });
    }
  });

  // V1 Vessels - Get latest position
  app.get('/v1/vessels/:vessel_id/latest_position', optionalAuth, async (req, res) => {
    try {
      const { vessel_id } = req.params;
      const vessel = await storage.getVessel(vessel_id);
      
      if (!vessel) {
        return res.status(404).json({ error: 'Vessel not found' });
      }
      
      const positions = await storage.getVesselPositions(vessel_id);
      const latest = positions.sort((a, b) => 
        new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
      )[0];
      
      if (!latest) {
        return res.status(404).json({ error: 'No position data available' });
      }
      
      res.json({
        vessel_id: vessel.id,
        mmsi: vessel.mmsi,
        timestamp_utc: latest.timestampUtc || latest.timestamp,
        latitude: parseFloat(String(latest.latitude)),
        longitude: parseFloat(String(latest.longitude)),
        sog_knots: latest.sogKnots ? parseFloat(String(latest.sogKnots)) : (latest.speed ? parseFloat(String(latest.speed)) : null),
        cog_deg: latest.cogDeg ? parseFloat(String(latest.cogDeg)) : (latest.course ? parseFloat(String(latest.course)) : null),
      });
    } catch (error) {
      console.error('V1 vessel latest position error:', error);
      res.status(500).json({ error: 'Failed to fetch vessel position' });
    }
  });

  // V1 Vessels - Get positions (GeoJSON)
  app.get('/v1/vessels/positions', optionalAuth, async (req, res) => {
    try {
      const { bbox, since_minutes = '60', limit = '2000' } = req.query;
      
      if (!bbox) {
        return res.status(400).json({ error: 'bbox parameter is required (minLon,minLat,maxLon,maxLat)' });
      }
      
      const [minLon, minLat, maxLon, maxLat] = String(bbox).split(',').map(parseFloat);
      const sinceMinutes = parseInt(String(since_minutes)) || 60;
      const limitNum = Math.min(parseInt(String(limit)) || 2000, 5000);
      
      const sinceTime = new Date(Date.now() - sinceMinutes * 60 * 1000);
      
      const vessels = await storage.getVessels();
      const features: any[] = [];
      
      for (const vessel of vessels.slice(0, 100)) {
        const positions = await storage.getVesselPositions(vessel.id);
        const recentPositions = positions.filter(p => {
          const posTime = new Date(p.timestampUtc || p.timestamp || 0);
          const lat = parseFloat(String(p.latitude));
          const lon = parseFloat(String(p.longitude));
          return posTime >= sinceTime && 
                 lat >= minLat && lat <= maxLat &&
                 lon >= minLon && lon <= maxLon;
        });
        
        const latest = recentPositions.sort((a, b) => 
          new Date(b.timestamp || 0).getTime() - new Date(a.timestamp || 0).getTime()
        )[0];
        
        if (latest) {
          features.push({
            type: 'Feature',
            geometry: {
              type: 'Point',
              coordinates: [parseFloat(String(latest.longitude)), parseFloat(String(latest.latitude))],
            },
            properties: {
              vessel_id: vessel.id,
              mmsi: vessel.mmsi,
              name: vessel.name,
              sog_knots: latest.sogKnots ? parseFloat(String(latest.sogKnots)) : (latest.speed ? parseFloat(String(latest.speed)) : null),
              cog_deg: latest.cogDeg ? parseFloat(String(latest.cogDeg)) : (latest.course ? parseFloat(String(latest.course)) : null),
              timestamp_utc: latest.timestampUtc || latest.timestamp,
            },
          });
        }
        
        if (features.length >= limitNum) break;
      }
      
      res.json({
        type: 'FeatureCollection',
        features,
      });
    } catch (error) {
      console.error('V1 vessels positions error:', error);
      res.status(500).json({ error: 'Failed to fetch vessel positions' });
    }
  });

  // GraphQL-style API endpoints (simplified REST for now)
  
  // Cache stats endpoint
  app.get('/api/cache/stats', authenticate, requireRole('admin'), (req, res) => {
    res.json({
      ...cacheService.getStats(),
      hitRate: `${(cacheService.getHitRate() * 100).toFixed(2)}%`
    });
  });

  // Vessels endpoint with caching, optional pagination, and geo-filtering
  app.get('/api/vessels', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      const geoParams = parseGeoQueryParams(req);
      
      const vessels = await cacheService.getOrSet(
        CACHE_KEYS.VESSELS,
        () => storage.getVessels(),
        CACHE_TTL.MEDIUM
      );
      
      // Get latest positions for all vessels
      const latestPositions = await storage.getLatestVesselPositions();
      
      // Map positions by vesselId for quick lookup
      const positionMap = new Map<string, any>();
      for (const pos of latestPositions) {
        if (!positionMap.has(pos.vesselId) || new Date(pos.timestamp || 0) > new Date(positionMap.get(pos.vesselId).timestamp || 0)) {
          positionMap.set(pos.vesselId, pos);
        }
      }
      
      // Attach latest position to each vessel
      let result = vessels.map((vessel: any) => {
        const position = positionMap.get(vessel.id);
        return {
          ...vessel,
          position: position ? {
            latitude: position.latitude,
            longitude: position.longitude,
            speedOverGround: position.speedOverGround || 0,
            navigationStatus: position.navigationStatus || 'unknown',
            timestamp: position.timestamp,
          } : null,
        };
      });
      
      if (geoParams) {
        result = filterByGeoRadius(result, geoParams);
      }
      
      if (usePagination) {
        res.json(paginateArray(result, pagination));
      } else {
        res.json(result);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch vessels' });
    }
  });

  // Ports endpoint with caching, optional pagination, and geo-filtering (public access)
  app.get('/api/ports', optionalAuth, async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      const geoParams = parseGeoQueryParams(req);
      
      const ports = await cacheService.getOrSet(
        CACHE_KEYS.PORTS,
        () => storage.getPorts(),
        CACHE_TTL.LONG
      );
      
      let result = ports;
      
      if (geoParams) {
        result = filterByGeoRadius(ports, geoParams);
      }
      
      if (usePagination) {
        res.json(paginateArray(result, pagination));
      } else {
        res.json(result);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch ports' });
    }
  });

  // Port statistics endpoint with caching
  app.get('/api/ports/:portId/stats', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const stats = await cacheService.getOrSet(
        CACHE_KEYS.PORT_STATS(portId),
        () => storage.getLatestPortStats(portId),
        CACHE_TTL.MEDIUM
      );
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch port statistics' });
    }
  });

  // Storage sites endpoint with caching and optional pagination (public access)
  app.get('/api/storage/sites', optionalAuth, async (req, res) => {
    try {
      const { portId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 50 });
      
      const cacheKey = CACHE_KEYS.STORAGE_SITES(portId as string);
      const sites = await cacheService.getOrSet(
        cacheKey,
        async () => {
          const rawSites = await storage.getStorageSites(portId as string);
          return Promise.all(
            rawSites.map(async (site) => {
              const fillData = await storage.getLatestStorageFill(site.id);
              return { ...site, fillData };
            })
          );
        },
        CACHE_TTL.MEDIUM
      );
      
      if (usePagination) {
        res.json(paginateArray(sites, pagination));
      } else {
        res.json(sites);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch storage sites' });
    }
  });

  // Floating storage endpoint (public access)
  app.get('/api/storage/floating', optionalAuth, async (req, res) => {
    try {
      const { getFloatingStorageStats } = await import('./services/storageDataService');
      const data = await getFloatingStorageStats();
      res.json(data);
    } catch (error) {
      console.error('Error fetching floating storage:', error);
      res.status(500).json({ error: 'Failed to fetch floating storage data' });
    }
  });

  // SPR reserves endpoint (public access)
  app.get('/api/storage/spr', optionalAuth, async (req, res) => {
    try {
      const { getSprStats } = await import('./services/storageDataService');
      const data = await getSprStats();
      res.json(data);
    } catch (error) {
      console.error('Error fetching SPR data:', error);
      res.status(500).json({ error: 'Failed to fetch SPR reserves data' });
    }
  });

  // Storage time series endpoint (public access)
  app.get('/api/storage/timeseries', optionalAuth, async (req, res) => {
    try {
      const { metricType, region, storageType, weeks } = req.query;
      const { getStorageTimeSeriesData } = await import('./services/storageDataService');
      const data = await getStorageTimeSeriesData({
        metricType: metricType as string,
        region: region as string,
        storageType: storageType as string,
        weeks: weeks ? parseInt(weeks as string) : 52,
      });
      res.json(data);
    } catch (error) {
      console.error('Error fetching storage time series:', error);
      res.status(500).json({ error: 'Failed to fetch storage time series data' });
    }
  });

  // Signals endpoint with caching and optional pagination
  app.get('/api/signals', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 50 });
      
      const signals = await cacheService.getOrSet(
        CACHE_KEYS.ACTIVE_SIGNALS,
        () => storage.getActiveSignals(),
        CACHE_TTL.SHORT
      );
      
      if (usePagination) {
        res.json(paginateArray(signals, pagination));
      } else {
        res.json(signals);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch signals' });
    }
  });

  // Predictions endpoint with caching and optional pagination
  app.get('/api/predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { target } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 50 });
      
      const predictions = await cacheService.getOrSet(
        CACHE_KEYS.LATEST_PREDICTIONS(target as string),
        () => storage.getLatestPredictions(target as string),
        CACHE_TTL.MEDIUM
      );
      
      if (usePagination) {
        res.json(paginateArray(predictions, pagination));
      } else {
        res.json(predictions);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch predictions' });
    }
  });

  // Port delay events endpoint
  app.get('/api/ports/:portId/delays', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const { limit } = req.query;
      const delays = await storage.getPortDelayEvents(portId, limit ? parseInt(limit as string) : 50);
      res.json(delays);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch port delay events' });
    }
  });

  // Vessel delay snapshots endpoint
  app.get('/api/vessels/delays', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId, portId, limit } = req.query;
      const snapshots = await storage.getVesselDelaySnapshots(
        vesselId as string | undefined,
        portId as string | undefined,
        limit ? parseInt(limit as string) : 50
      );
      res.json(snapshots);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch vessel delay snapshots' });
    }
  });

  // Market delay impact endpoint
  app.get('/api/market/delays/impact', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { portId, commodityId, limit } = req.query;
      const impacts = await storage.getMarketDelayImpacts(
        portId as string | undefined,
        commodityId as string | undefined,
        limit ? parseInt(limit as string) : 20
      );
      res.json(impacts);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch market delay impacts' });
    }
  });

  // Delay-adjusted predictions endpoint
  app.get('/api/predictions/delay-adjusted', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { portId, commodityCode } = req.query;
      
      // Get latest market delay impact
      const impacts = await storage.getMarketDelayImpacts(portId as string | undefined, undefined, 1);
      const latestImpact = impacts[0];
      
      // Get base prediction
      const predictions = await storage.getLatestPredictions(commodityCode as string);
      const basePrediction = predictions[0];
      
      if (!basePrediction || !latestImpact) {
        return res.json({ 
          delayAdjusted: false,
          prediction: basePrediction,
          message: 'No delay impact data available'
        });
      }
      
      // Adjust prediction based on delay impact
      const priceImpact = parseFloat(latestImpact.priceImpact || '0');
      const basePrice = parseFloat(basePrediction.predictedPrice);
      const adjustedPrice = basePrice + priceImpact;
      
      res.json({
        delayAdjusted: true,
        basePrediction: basePrediction,
        delayImpact: latestImpact,
        adjustedPrediction: {
          ...basePrediction,
          predictedPrice: adjustedPrice.toFixed(2),
          adjustmentReason: `Adjusted for ${latestImpact.vesselCount} delayed vessels carrying ${latestImpact.totalDelayedVolume} tons`
        }
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch delay-adjusted predictions' });
    }
  });

  // Rotterdam data endpoints
  app.get('/api/rotterdam-data', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { month } = req.query;
      
      if (month) {
        const data = rotterdamDataService.getDataByMonth(month as string);
        const stats = rotterdamDataService.getAggregatedStats(month as string);
        res.json({ data, stats });
      } else {
        const data = rotterdamDataService.getAllData();
        const stats = rotterdamDataService.getAggregatedStats();
        res.json({ data, stats });
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam data' });
    }
  });
  
  app.get('/api/rotterdam-data/months', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const months = rotterdamDataService.getAvailableMonths();
      res.json(months);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch available months' });
    }
  });
  
  app.get('/api/rotterdam-data/latest', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const latest = rotterdamDataService.getLatestData();
      res.json(latest);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch latest Rotterdam data' });
    }
  });

  // Real-time Port of Rotterdam arrivals/departures (simulated API)
  app.get('/api/rotterdam/arrivals', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const arrivals = await rotterdamDataService.getExpectedArrivals();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        count: arrivals.length,
        arrivals,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam arrivals' });
    }
  });

  app.get('/api/rotterdam/departures', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const departures = await rotterdamDataService.getRecentDepartures();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        count: departures.length,
        departures,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam departures' });
    }
  });

  app.get('/api/rotterdam/vessels-at-port', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const vessels = await rotterdamDataService.getVesselsAtPort();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        atBerth: vessels.atBerth,
        atAnchor: vessels.atAnchor,
        totalAtBerth: vessels.atBerth.length,
        totalAtAnchor: vessels.atAnchor.length,
        lastUpdated: new Date().toISOString()
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch vessels at Rotterdam' });
    }
  });

  app.get('/api/rotterdam/activity-summary', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const summary = await rotterdamDataService.getPortActivitySummary();
      res.json({
        port: 'NLRTM',
        portName: 'Port of Rotterdam',
        ...summary
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch Rotterdam activity summary' });
    }
  });

  // ===== CARGO CHAINS & TRADE FLOWS =====
  
  // Get all trade flows with cargo chain details
  app.get('/api/trade-flows', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { limit } = req.query;
      const flows = await storage.getActiveTradeFlows();
      
      // Enrich with cargo legs, STS events, and splits
      const enrichedFlows = await Promise.all(
        flows.slice(0, limit ? parseInt(limit as string) : 50).map(async (flow) => {
          const [legs, stsEvents, splits] = await Promise.all([
            storage.getCargoLegsByTradeFlow(flow.id),
            storage.getSTSEventsByTradeFlow(flow.id),
            storage.getCargoSplitsByTradeFlow(flow.id)
          ]);
          
          return {
            ...flow,
            cargoChain: legs,
            stsEvents,
            splits
          };
        })
      );
      
      res.json(enrichedFlows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch trade flows' });
    }
  });

  // Get single trade flow with complete cargo chain
  app.get('/api/trade-flows/:flowId', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { flowId } = req.params;
      const flows = await storage.getActiveTradeFlows();
      const flow = flows.find(f => f.id === flowId);
      
      if (!flow) {
        return res.status(404).json({ error: 'Trade flow not found' });
      }
      
      const [legs, stsEvents, splits, vessel, commodity] = await Promise.all([
        storage.getCargoLegsByTradeFlow(flowId),
        storage.getSTSEventsByTradeFlow(flowId),
        storage.getCargoSplitsByTradeFlow(flowId),
        storage.getVesselByMMSI(flow.vesselId),
        storage.getCommodities().then(c => c.find(com => com.id === flow.commodityId))
      ]);
      
      res.json({
        ...flow,
        vessel,
        commodity,
        cargoChain: legs,
        stsEvents,
        splits
      });
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch trade flow details' });
    }
  });

  // STS Events endpoints
  app.get('/api/sts-events', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { vesselId, limit } = req.query;
      
      if (vesselId) {
        const events = await storage.getSTSEventsByVessel(vesselId as string);
        res.json(events);
      } else {
        const events = await storage.getSTSEvents(limit ? parseInt(limit as string) : 50);
        res.json(events);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch STS events' });
    }
  });

  // Flow Forecasts endpoints
  app.get('/api/flow-forecasts', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { originPortId, destinationPortId, limit } = req.query;
      
      if (originPortId && destinationPortId) {
        const forecasts = await storage.getFlowForecastsByRoute(
          originPortId as string,
          destinationPortId as string
        );
        res.json(forecasts);
      } else {
        const forecasts = await storage.getActiveFlowForecasts();
        res.json(forecasts.slice(0, limit ? parseInt(limit as string) : 20));
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch flow forecasts' });
    }
  });

  // Cargo splits endpoint
  app.get('/api/cargo-splits', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { tradeFlowId, limit } = req.query;
      
      if (tradeFlowId) {
        const splits = await storage.getCargoSplitsByTradeFlow(tradeFlowId as string);
        res.json(splits);
      } else {
        const splits = await storage.getCargoSplits(limit ? parseInt(limit as string) : 50);
        res.json(splits);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch cargo splits' });
    }
  });

  // ===== MARITIME INTELLIGENCE =====
  
  // Port Calls endpoints with optional pagination
  app.get('/api/port-calls', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId, vesselId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      
      const calls = await storage.getPortCalls(
        portId as string | undefined,
        vesselId as string | undefined,
        500
      );
      
      if (usePagination) {
        res.json(paginateArray(calls, pagination));
      } else {
        res.json(calls);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch port calls' });
    }
  });

  app.post('/api/port-calls', authenticate, requirePermission('write:ports'), async (req, res) => {
    try {
      const portCall = await storage.createPortCall(req.body);
      res.status(201).json(portCall);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create port call' });
    }
  });

  // Container Operations endpoints with optional pagination
  app.get('/api/container-operations', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId, vesselId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      
      const operations = await storage.getContainerOperations(
        portId as string | undefined,
        vesselId as string | undefined,
        500
      );
      
      if (usePagination) {
        res.json(paginateArray(operations, pagination));
      } else {
        res.json(operations);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch container operations' });
    }
  });

  app.get('/api/container-operations/stats/:portId', authenticate, requirePermission('read:ports'), async (req, res) => {
    try {
      const { portId } = req.params;
      const stats = await storage.getContainerStatsByPort(portId);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch container stats' });
    }
  });

  app.post('/api/container-operations', authenticate, requirePermission('write:ports'), async (req, res) => {
    try {
      const operation = await storage.createContainerOperation(req.body);
      res.status(201).json(operation);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create container operation' });
    }
  });

  // Bunkering Events endpoints with optional pagination
  app.get('/api/bunkering-events', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId, portId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      
      const events = await storage.getBunkeringEvents(
        vesselId as string | undefined,
        portId as string | undefined,
        500
      );
      
      if (usePagination) {
        res.json(paginateArray(events, pagination));
      } else {
        res.json(events);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch bunkering events' });
    }
  });

  app.get('/api/bunkering-events/stats/:vesselId', authenticate, requirePermission('read:vessels'), async (req, res) => {
    try {
      const { vesselId } = req.params;
      const stats = await storage.getBunkeringStatsByVessel(vesselId);
      res.json(stats);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch bunkering stats' });
    }
  });

  app.post('/api/bunkering-events', authenticate, requirePermission('write:vessels'), async (req, res) => {
    try {
      const event = await storage.createBunkeringEvent(req.body);
      res.status(201).json(event);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create bunkering event' });
    }
  });

  // Communications/Inbox endpoints with optional pagination
  app.get('/api/communications', authenticate, requirePermission('read:alerts'), async (req, res) => {
    try {
      const { userId } = req.query;
      const usePagination = req.query.paginate === 'true';
      const pagination = parsePaginationParams(req, { limit: 100 });
      
      const communications = await storage.getCommunications(
        userId as string | undefined,
        500
      );
      
      if (usePagination) {
        res.json(paginateArray(communications, pagination));
      } else {
        res.json(communications);
      }
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch communications' });
    }
  });

  app.get('/api/communications/unread', authenticate, requirePermission('read:alerts'), async (req, res) => {
    try {
      const { userId } = req.query;
      const unread = await storage.getUnreadCommunications(userId as string || 'default');
      res.json(unread);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch unread communications' });
    }
  });

  app.post('/api/communications', authenticate, requirePermission('write:alerts'), async (req, res) => {
    try {
      const communication = await storage.createCommunication(req.body);
      res.status(201).json(communication);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create communication' });
    }
  });

  app.patch('/api/communications/:id/read', authenticate, requirePermission('write:alerts'), async (req, res) => {
    try {
      const { id } = req.params;
      const communication = await storage.markCommunicationAsRead(id);
      res.json(communication);
    } catch (error) {
      res.status(500).json({ error: 'Failed to mark communication as read' });
    }
  });

  // ===== COMMODITY PACK ROUTES =====
  
  // Crude & Products Pack
  app.get('/api/crude-grades', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { category, limit } = req.query;
      const grades = await storage.getCrudeGrades(
        category as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(grades);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch crude grades' });
    }
  });

  app.post('/api/crude-grades', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const grade = await storage.createCrudeGrade(req.body);
      res.status(201).json(grade);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create crude grade' });
    }
  });

  // LNG/LPG Pack
  app.get('/api/lng-cargoes', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { cargoType, portId, limit } = req.query;
      const cargoes = await storage.getLngCargoes(
        cargoType as string | undefined,
        portId as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(cargoes);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch LNG cargoes' });
    }
  });

  app.get('/api/lng-cargoes/diversions', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { limit } = req.query;
      const cargoes = await storage.getDiversionCargoes(
        limit ? parseInt(limit as string) : 100
      );
      res.json(cargoes);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch diversion cargoes' });
    }
  });

  app.post('/api/lng-cargoes', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const cargo = await storage.createLngCargo(req.body);
      res.status(201).json(cargo);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create LNG cargo' });
    }
  });

  // Dry Bulk Pack
  app.get('/api/dry-bulk-fixtures', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodityType, vesselSize, limit } = req.query;
      const fixtures = await storage.getDryBulkFixtures(
        commodityType as string | undefined,
        vesselSize as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(fixtures);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch dry bulk fixtures' });
    }
  });

  app.post('/api/dry-bulk-fixtures', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const fixture = await storage.createDryBulkFixture(req.body);
      res.status(201).json(fixture);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create dry bulk fixture' });
    }
  });

  // Petrochem Pack
  app.get('/api/petrochem-products', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { category, region, limit } = req.query;
      const products = await storage.getPetrochemProducts(
        category as string | undefined,
        region as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(products);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch petrochem products' });
    }
  });

  app.post('/api/petrochem-products', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const product = await storage.createPetrochemProduct(req.body);
      res.status(201).json(product);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create petrochem product' });
    }
  });

  // Agri & Biofuel Pack
  app.get('/api/agri-biofuel-flows', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodityType, flowType, limit } = req.query;
      const flows = await storage.getAgriBiofuelFlows(
        commodityType as string | undefined,
        flowType as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(flows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch agri/biofuel flows' });
    }
  });

  app.get('/api/agri-biofuel-flows/sustainable', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { limit } = req.query;
      const flows = await storage.getSustainableBiofuelFlows(
        limit ? parseInt(limit as string) : 100
      );
      res.json(flows);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch sustainable biofuel flows' });
    }
  });

  app.post('/api/agri-biofuel-flows', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const flow = await storage.createAgriBiofuelFlow(req.body);
      res.status(201).json(flow);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create agri/biofuel flow' });
    }
  });

  // ===== REFINERY/PLANT INTELLIGENCE ROUTES =====
  app.get('/api/refineries', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { region, maintenanceStatus, limit } = req.query;
      const refineries = await storage.getRefineries(
        region as string | undefined,
        maintenanceStatus as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(refineries);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refineries' });
    }
  });

  app.get('/api/refineries/:refineryCode', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const refinery = await storage.getRefineryByCode(req.params.refineryCode);
      if (!refinery) {
        return res.status(404).json({ error: 'Refinery not found' });
      }
      res.json(refinery);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery' });
    }
  });

  app.post('/api/refineries', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const refinery = await storage.createRefinery(req.body);
      res.status(201).json(refinery);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create refinery' });
    }
  });

  // ===== SUPPLY & DEMAND BALANCES ROUTES =====
  app.get('/api/supply-demand-balances', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodity, region, period, limit } = req.query;
      const balances = await storage.getSupplyDemandBalances(
        commodity as string | undefined,
        region as string | undefined,
        period as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(balances);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch supply & demand balances' });
    }
  });

  app.get('/api/supply-demand-balances/latest', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { commodity, region, limit } = req.query;
      const balances = await storage.getLatestBalances(
        commodity as string | undefined,
        region as string | undefined,
        limit ? parseInt(limit as string) : 10
      );
      res.json(balances);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch latest balances' });
    }
  });

  app.post('/api/supply-demand-balances', authenticate, requirePermission('write:storage'), async (req, res) => {
    try {
      const balance = await storage.createSupplyDemandBalance(req.body);
      res.status(201).json(balance);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create supply & demand balance' });
    }
  });

  // ===== RESEARCH & INSIGHT LAYER ROUTES =====
  app.get('/api/research-reports', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { category, subcategory, limit } = req.query;
      const reports = await storage.getResearchReports(
        category as string | undefined,
        subcategory as string | undefined,
        limit ? parseInt(limit as string) : 100
      );
      res.json(reports);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch research reports' });
    }
  });

  app.get('/api/research-reports/published', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { limit } = req.query;
      const reports = await storage.getPublishedReports(
        limit ? parseInt(limit as string) : 10
      );
      res.json(reports);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch published reports' });
    }
  });

  app.get('/api/research-reports/:reportId', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const report = await storage.getReportById(req.params.reportId);
      if (!report) {
        return res.status(404).json({ error: 'Report not found' });
      }
      res.json(report);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch report' });
    }
  });

  app.post('/api/research-reports', authenticate, requirePermission('write:signals'), async (req, res) => {
    try {
      const report = await storage.createResearchReport(req.body);
      res.status(201).json(report);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create research report' });
    }
  });

  // Initialize services and start mock data generation (no auth required for auto-start)
  app.post('/api/init', async (req, res) => {
    try {
      console.log('Initializing Veriscope services...');
      
      // Seed global ports database
      const { seedGlobalPorts, getPortCount, seedPortCalls, getPortCallCount } = await import('./services/portSeedService');
      await seedGlobalPorts();
      const portCount = await getPortCount();
      console.log(`Total ports in database: ${portCount}`);
      
      // Initialize mock data (creates vessels needed for port calls)
      await mockDataService.initializeBaseData();
      
      // Seed port call data for arrivals/departures/dwell time (after vessels exist)
      await seedPortCalls();
      const portCallCount = await getPortCallCount();
      console.log(`Total port calls in database: ${portCallCount}`);
      
      // Initialize refinery satellite data
      const { initializeRefineryAois, generateMockSatelliteData } = await import('./services/refinerySatelliteService');
      await initializeRefineryAois();
      await generateMockSatelliteData();
      
      // Initialize storage data (floating storage, SPR reserves, time series)
      const { initializeStorageData } = await import('./services/storageDataService');
      await initializeStorageData();
      
      // Start services
      aisService.startSimulation(wss);
      signalsService.startMonitoring(wss);
      predictionService.startPredictionService();
      delayService.start(wss);
      portCallService.start();
      
      res.json({ message: 'Veriscope services initialized successfully', portCount });
    } catch (error) {
      console.error('Initialization error:', error);
      res.status(500).json({ error: 'Failed to initialize services' });
    }
  });

  // ===== CSV-BASED DATA ENDPOINTS =====
  
  // Refinery Units
  app.get('/api/refinery/units', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { plant } = req.query;
      const units = await storage.getRefineryUnits(plant as string);
      res.json(units);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery units' });
    }
  });

  // Refinery Utilization
  app.get('/api/refinery/utilization', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { startDate, endDate, plant } = req.query;
      const utilization = await storage.getRefineryUtilization(
        startDate as string,
        endDate as string,
        plant as string
      );
      res.json(utilization);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch refinery utilization' });
    }
  });

  // Refinery Crack Spreads
  app.get('/api/refinery/crack-spreads', authenticate, requirePermission('read:storage'), async (req, res) => {
    try {
      const { startDate, endDate } = req.query;
      const spreads = await storage.getRefineryCrackSpreads(
        startDate as string,
        endDate as string
      );
      res.json(spreads);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch crack spreads' });
    }
  });

  // Supply & Demand Models Daily
  app.get('/api/supply-demand/models-daily', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { startDate, endDate, region } = req.query;
      const models = await storage.getSdModelsDaily(
        startDate as string,
        endDate as string,
        region as string
      );
      res.json(models);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch S&D models' });
    }
  });

  // Supply & Demand Forecasts Weekly
  app.get('/api/supply-demand/forecasts-weekly', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { startDate, endDate, region } = req.query;
      const forecasts = await storage.getSdForecastsWeekly(
        startDate as string,
        endDate as string,
        region as string
      );
      res.json(forecasts);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch S&D forecasts' });
    }
  });

  // Research Insights Daily
  app.get('/api/research-insights/daily', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { startDate, endDate, limit } = req.query;
      const insights = await storage.getResearchInsightsDaily(
        startDate as string,
        endDate as string,
        limit ? parseInt(limit as string) : 100
      );
      res.json(insights);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch research insights' });
    }
  });

  // ===== ML PRICE PREDICTIONS =====
  
  // Get all ML predictions (optional filter by commodity type)
  app.get('/api/ml-predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { commodityType, limit } = req.query;
      const predictions = await storage.getMlPredictions(
        commodityType as string,
        limit ? parseInt(limit as string) : 10
      );
      res.json(predictions);
    } catch (error) {
      console.error('Error fetching ML predictions:', error);
      res.status(500).json({ error: 'Failed to fetch ML predictions' });
    }
  });

  // Get latest ML prediction for a specific commodity
  app.get('/api/ml-predictions/latest/:commodityType', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { commodityType } = req.params;
      const prediction = await storage.getLatestMlPrediction(commodityType);
      
      if (!prediction) {
        return res.status(404).json({ error: 'No prediction found for this commodity' });
      }
      
      res.json(prediction);
    } catch (error) {
      console.error('Error fetching latest prediction:', error);
      res.status(500).json({ error: 'Failed to fetch latest prediction' });
    }
  });

  // Generate new ML prediction for a commodity
  app.post('/api/ml-predictions/generate', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { commodityType, currentPrice } = req.body;
      
      if (!commodityType) {
        return res.status(400).json({ error: 'commodityType is required' });
      }
      
      const { mlPredictionService } = await import('./services/mlPredictionService');
      const prediction = await mlPredictionService.generatePrediction(
        commodityType,
        currentPrice || 80
      );
      
      if (!prediction) {
        return res.status(500).json({ error: 'Failed to generate prediction' });
      }
      
      res.json(prediction);
    } catch (error) {
      console.error('Error generating prediction:', error);
      res.status(500).json({ error: 'Failed to generate prediction' });
    }
  });

  // ===== DATA QUALITY ENDPOINTS =====
  
  app.get('/api/data-quality/scores', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { entityId, limit } = req.query;
      const { dataQualityService } = await import('./services/dataQualityService');
      const scores = await dataQualityService.getLatestQualityScores(
        entityId as string | undefined,
        limit ? parseInt(limit as string) : 10
      );
      res.json(scores);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch data quality scores' });
    }
  });

  app.get('/api/data-quality/streams', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { dataQualityService } = await import('./services/dataQualityService');
      const streams = await dataQualityService.getAllStreamHealth();
      res.json(streams);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch stream health' });
    }
  });

  app.get('/api/data-quality/streams/:streamName', authenticate, requirePermission('read:signals'), async (req, res) => {
    try {
      const { streamName } = req.params;
      const { dataQualityService } = await import('./services/dataQualityService');
      const health = await dataQualityService.getStreamHealth(streamName);
      res.json(health);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch stream health' });
    }
  });

  // Import CSV data endpoint for testing
  app.post('/api/import-csv', authenticate, requireRole('admin', 'operator'), async (req, res) => {
    try {
      const { importAllCSVData } = await import('./services/csvImportService');
      console.log('Starting CSV import...');
      await importAllCSVData();
      res.json({ message: 'CSV data imported successfully' });
    } catch (error: any) {
      console.error('CSV import error:', error);
      res.status(500).json({ error: 'Failed to import CSV data', details: error.message });
    }
  });

  // ===== MODEL REGISTRY & ML CREDIBILITY ENDPOINTS =====

  // List all models
  app.get('/api/models', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { status } = req.query;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const models = await modelRegistryService.listModels(status as string | undefined);
      res.json(models);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch models' });
    }
  });

  // Get single model
  app.get('/api/models/:modelId', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.getModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch model' });
    }
  });

  // Create new model
  app.post('/api/models', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelName, version, modelType, features, hyperparameters, trainingMetrics, validationMetrics, status } = req.body;
      
      if (!modelName || typeof modelName !== 'string' || modelName.trim() === '') {
        return res.status(400).json({ error: 'modelName is required and must be a non-empty string' });
      }
      if (!version || typeof version !== 'string' || version.trim() === '') {
        return res.status(400).json({ error: 'version is required and must be a non-empty string' });
      }
      if (status && !['active', 'deprecated', 'archived'].includes(status)) {
        return res.status(400).json({ error: 'status must be one of: active, deprecated, archived' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.createModel({
        modelName: modelName.trim(),
        version: version.trim(),
        modelType,
        features,
        hyperparameters,
        trainingMetrics,
        validationMetrics,
        status
      });
      if (!model) {
        return res.status(400).json({ error: 'Failed to create model' });
      }
      res.status(201).json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create model' });
    }
  });

  // Activate model (deprecates other active versions of same model)
  app.post('/api/models/:modelId/activate', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.activateModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to activate model' });
    }
  });

  // Deprecate model
  app.post('/api/models/:modelId/deprecate', authenticate, requirePermission('write:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const model = await modelRegistryService.deprecateModel(modelId);
      if (!model) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(model);
    } catch (error) {
      res.status(500).json({ error: 'Failed to deprecate model' });
    }
  });

  // Get predictions for a model (with confidence intervals)
  app.get('/api/models/:modelId/predictions', authenticate, requirePermission('read:predictions'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { limit } = req.query;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const predictions = await modelRegistryService.getPredictions(
        modelId,
        limit ? parseInt(limit as string) : 100
      );
      res.json(predictions);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch predictions' });
    }
  });

  // Create prediction with confidence interval
  app.post('/api/models/:modelId/predictions', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { target, predictionDate, predictedValue, confidenceLevel, horizon, featuresUsed } = req.body;
      
      if (!target || typeof target !== 'string' || target.trim() === '') {
        return res.status(400).json({ error: 'target is required and must be a non-empty string' });
      }
      if (!predictionDate) {
        return res.status(400).json({ error: 'predictionDate is required' });
      }
      const parsedDate = new Date(predictionDate);
      if (isNaN(parsedDate.getTime())) {
        return res.status(400).json({ error: 'predictionDate must be a valid date' });
      }
      if (predictedValue === undefined || typeof predictedValue !== 'number' || isNaN(predictedValue)) {
        return res.status(400).json({ error: 'predictedValue is required and must be a number' });
      }
      if (confidenceLevel !== undefined && (typeof confidenceLevel !== 'number' || confidenceLevel <= 0 || confidenceLevel >= 1)) {
        return res.status(400).json({ error: 'confidenceLevel must be a number between 0 and 1' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const prediction = await modelRegistryService.generatePredictionWithConfidence(
        modelId,
        target.trim(),
        parsedDate,
        predictedValue,
        confidenceLevel || 0.95,
        horizon,
        featuresUsed
      );
      
      if (!prediction) {
        return res.status(400).json({ error: 'Failed to create prediction. Model may not exist.' });
      }
      res.status(201).json(prediction);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create prediction' });
    }
  });

  // Record actual value for backtesting
  app.post('/api/predictions/:predictionId/actual', authenticate, requirePermission('write:predictions'), async (req, res) => {
    try {
      const { predictionId } = req.params;
      const { actualValue } = req.body;
      
      if (actualValue === undefined || typeof actualValue !== 'number' || isNaN(actualValue)) {
        return res.status(400).json({ error: 'actualValue is required and must be a number' });
      }
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const prediction = await modelRegistryService.recordActualValue(predictionId, actualValue);
      
      if (!prediction) {
        return res.status(404).json({ error: 'Prediction not found' });
      }
      res.json(prediction);
    } catch (error) {
      res.status(500).json({ error: 'Failed to record actual value' });
    }
  });

  // Get backtest results for a model
  app.get('/api/models/:modelId/backtest', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { startDate, endDate } = req.query;
      
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const results = await modelRegistryService.getBacktestResults(
        modelId,
        startDate ? new Date(startDate as string) : undefined,
        endDate ? new Date(endDate as string) : undefined
      );
      
      if (!results) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(results);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch backtest results' });
    }
  });

  // Get drift metrics for a model
  app.get('/api/models/:modelId/drift', authenticate, requirePermission('read:models'), async (req, res) => {
    try {
      const { modelId } = req.params;
      const { modelRegistryService } = await import('./services/modelRegistryService');
      const metrics = await modelRegistryService.getDriftMetrics(modelId);
      
      if (!metrics) {
        return res.status(404).json({ error: 'Model not found' });
      }
      res.json(metrics);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch drift metrics' });
    }
  });

  // ===== WATCHLISTS API =====
  
  app.get('/api/watchlists', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const watchlists = await storage.getWatchlists(userId);
      res.json(watchlists);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch watchlists' });
    }
  });

  app.post('/api/watchlists', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const { name, type, items, alertSettings, isDefault } = req.body;
      
      if (!name || !type || !items) {
        return res.status(400).json({ error: 'Name, type, and items are required' });
      }
      
      const watchlist = await storage.createWatchlist({
        userId,
        name,
        type,
        items,
        alertSettings,
        isDefault: isDefault || false
      });
      res.status(201).json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to create watchlist' });
    }
  });

  app.get('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      const watchlist = await storage.getWatchlistById(req.params.id);
      if (!watchlist) {
        return res.status(404).json({ error: 'Watchlist not found' });
      }
      res.json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch watchlist' });
    }
  });

  app.patch('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      const { name, items, alertSettings, isDefault } = req.body;
      const watchlist = await storage.updateWatchlist(req.params.id, {
        name,
        items,
        alertSettings,
        isDefault
      });
      res.json(watchlist);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update watchlist' });
    }
  });

  app.delete('/api/watchlists/:id', optionalAuth, async (req, res) => {
    try {
      await storage.deleteWatchlist(req.params.id);
      res.json({ success: true });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete watchlist' });
    }
  });

  // ===== ALERT RULES API =====
  
  app.get('/api/alert-rules', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const rules = await storage.getAlertRules(userId);
      res.json(rules);
    } catch (error) {
      console.error('Error fetching alert rules:', error);
      res.status(500).json({ error: 'Failed to fetch alert rules' });
    }
  });

  app.post('/api/alert-rules', optionalAuth, async (req, res) => {
    try {
      const userId = (req as any).user?.id || 'demo-user';
      const { name, type, conditions, channels, cooldownMinutes, watchlistId, isActive, severity, isMuted } = req.body;
      
      if (!name || !type || !conditions || !channels) {
        return res.status(400).json({ error: 'Name, type, conditions, and channels are required' });
      }
      
      const rule = await storage.createAlertRule({
        userId,
        name,
        type,
        conditions,
        channels,
        cooldownMinutes: cooldownMinutes || 60,
        watchlistId,
        isActive: isActive !== false,
        severity: severity || 'medium',
        isMuted: isMuted || false
      });
      res.status(201).json(rule);
    } catch (error) {
      console.error('Error creating alert rule:', error);
      res.status(500).json({ error: 'Failed to create alert rule' });
    }
  });

  app.get('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      const rule = await storage.getAlertRuleById(req.params.id);
      if (!rule) {
        return res.status(404).json({ error: 'Alert rule not found' });
      }
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to fetch alert rule' });
    }
  });

  app.patch('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      const { name, conditions, channels, cooldownMinutes, isActive, watchlistId, severity, isMuted, snoozedUntil } = req.body;
      const rule = await storage.updateAlertRule(req.params.id, {
        name,
        conditions,
        channels,
        cooldownMinutes,
        isActive,
        watchlistId,
        severity,
        isMuted,
        snoozedUntil: snoozedUntil ? new Date(snoozedUntil) : undefined
      });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to update alert rule' });
    }
  });

  // Snooze an alert rule
  app.post('/api/alert-rules/:id/snooze', optionalAuth, async (req, res) => {
    try {
      const { hours } = req.body;
      if (!hours || hours < 1 || hours > 168) {
        return res.status(400).json({ error: 'Hours must be between 1 and 168 (7 days)' });
      }
      const snoozedUntil = new Date(Date.now() + hours * 60 * 60 * 1000);
      const rule = await storage.updateAlertRule(req.params.id, { snoozedUntil });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to snooze alert rule' });
    }
  });

  // Unsnooze an alert rule
  app.post('/api/alert-rules/:id/unsnooze', optionalAuth, async (req, res) => {
    try {
      const rule = await storage.updateAlertRule(req.params.id, { snoozedUntil: null as any });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to unsnooze alert rule' });
    }
  });

  // Mute/unmute an alert rule
  app.post('/api/alert-rules/:id/mute', optionalAuth, async (req, res) => {
    try {
      const { muted } = req.body;
      const rule = await storage.updateAlertRule(req.params.id, { isMuted: muted !== false });
      res.json(rule);
    } catch (error) {
      res.status(500).json({ error: 'Failed to mute/unmute alert rule' });
    }
  });

  app.delete('/api/alert-rules/:id', optionalAuth, async (req, res) => {
    try {
      await storage.deleteAlertRule(req.params.id);
      res.json({ success: true });
    } catch (error) {
      res.status(500).json({ error: 'Failed to delete alert rule' });
    }
  });

  // ===== REFINERY SATELLITE MONITORING API =====
  
  app.get('/api/refinery/aois', optionalAuth, async (req, res) => {
    try {
      const { getAois } = await import('./services/refinerySatelliteService');
      const aois = await getAois();
      res.json(aois);
    } catch (error) {
      console.error('Error fetching AOIs:', error);
      res.status(500).json({ error: 'Failed to fetch AOIs' });
    }
  });

  app.get('/api/refinery/aois/:code', optionalAuth, async (req, res) => {
    try {
      const { getAoiByCode } = await import('./services/refinerySatelliteService');
      const aoi = await getAoiByCode(req.params.code);
      if (!aoi) {
        return res.status(404).json({ error: 'AOI not found' });
      }
      res.json(aoi);
    } catch (error) {
      console.error('Error fetching AOI:', error);
      res.status(500).json({ error: 'Failed to fetch AOI' });
    }
  });

  app.get('/api/refinery/activity/latest', optionalAuth, async (req, res) => {
    try {
      const { getLatestActivityIndex } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const latest = await getLatestActivityIndex(aoiCode);
      if (!latest) {
        return res.status(404).json({ error: 'No activity data found' });
      }
      res.json(latest);
    } catch (error) {
      console.error('Error fetching latest activity:', error);
      res.status(500).json({ error: 'Failed to fetch latest activity' });
    }
  });

  app.get('/api/refinery/activity/timeline', optionalAuth, async (req, res) => {
    try {
      const { getActivityTimeline } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const weeks = parseInt(req.query.weeks as string) || 12;
      const timeline = await getActivityTimeline(aoiCode, weeks);
      res.json(timeline);
    } catch (error) {
      console.error('Error fetching activity timeline:', error);
      res.status(500).json({ error: 'Failed to fetch activity timeline' });
    }
  });

  app.get('/api/refinery/observations', optionalAuth, async (req, res) => {
    try {
      const { getRecentObservations } = await import('./services/refinerySatelliteService');
      const aoiCode = (req.query.aoi as string) || 'rotterdam_full';
      const limit = parseInt(req.query.limit as string) || 10;
      const observations = await getRecentObservations(aoiCode, limit);
      res.json(observations);
    } catch (error) {
      console.error('Error fetching observations:', error);
      res.status(500).json({ error: 'Failed to fetch observations' });
    }
  });

  app.get('/api/refinery/summary', optionalAuth, async (req, res) => {
    try {
      const { getSummaryStats } = await import('./services/refinerySatelliteService');
      const summary = await getSummaryStats();
      res.json(summary);
    } catch (error) {
      console.error('Error fetching summary:', error);
      res.status(500).json({ error: 'Failed to fetch summary' });
    }
  });

  app.post('/api/refinery/refresh', optionalAuth, async (req, res) => {
    try {
      const { refreshSatelliteData } = await import('./services/refinerySatelliteService');
      const result = await refreshSatelliteData();
      res.json(result);
    } catch (error) {
      console.error('Error refreshing satellite data:', error);
      res.status(500).json({ error: 'Failed to refresh satellite data' });
    }
  });

  // ===== CSV EXPORT API =====
  
  app.get('/api/export/vessels', optionalAuth, async (req, res) => {
    try {
      const vessels = await storage.getVessels();
      const csv = generateCSV(vessels, ['id', 'mmsi', 'name', 'imo', 'vesselType', 'flag', 'owner', 'operator', 'buildYear', 'deadweight', 'length', 'beam', 'draft', 'capacity']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=vessels.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export vessels' });
    }
  });

  app.get('/api/export/ports', optionalAuth, async (req, res) => {
    try {
      const ports = await storage.getPorts();
      const csv = generateCSV(ports, ['id', 'name', 'code', 'country', 'region', 'latitude', 'longitude', 'type', 'capacity', 'depth', 'operationalStatus']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=ports.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export ports' });
    }
  });

  app.get('/api/export/signals', optionalAuth, async (req, res) => {
    try {
      const signals = await storage.getActiveSignals(500);
      const csv = generateCSV(signals, ['id', 'type', 'title', 'description', 'frequency', 'isActive', 'lastTriggered', 'createdAt']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=signals.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export signals' });
    }
  });

  app.get('/api/export/predictions', optionalAuth, async (req, res) => {
    try {
      const predictions = await storage.getPredictions();
      const csv = generateCSV(predictions, ['id', 'commodityId', 'marketId', 'timeframe', 'currentPrice', 'predictedPrice', 'confidence', 'direction', 'validUntil', 'createdAt']);
      res.setHeader('Content-Type', 'text/csv');
      res.setHeader('Content-Disposition', 'attachment; filename=predictions.csv');
      res.send(csv);
    } catch (error) {
      res.status(500).json({ error: 'Failed to export predictions' });
    }
  });

  return httpServer;
}

function generateCSV(data: any[], columns: string[]): string {
  if (!data || data.length === 0) {
    return columns.join(',') + '\n';
  }
  
  const header = columns.join(',');
  const rows = data.map(row => {
    return columns.map(col => {
      const value = row[col];
      if (value === null || value === undefined) return '';
      if (typeof value === 'string' && (value.includes(',') || value.includes('"') || value.includes('\n'))) {
        return `"${value.replace(/"/g, '""')}"`;
      }
      if (typeof value === 'object') {
        return `"${JSON.stringify(value).replace(/"/g, '""')}"`;
      }
      return String(value);
    }).join(',');
  });
  
  return [header, ...rows].join('\n');
}
