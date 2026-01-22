# Veriscope - Maritime Intelligence Platform

## Overview
Veriscope is an enterprise-grade maritime and commodity intelligence platform. It delivers real-time vessel tracking, port monitoring, AI-powered market predictions, and comprehensive commodity analytics. The platform features an advanced dashboard with global summaries and modular intelligence hubs for commodities, maritime, and energy transition. It integrates live AIS feeds, port statistics, storage monitoring, and predictive insights, combining real-world movement data with satellite imagery to generate actionable insights through interactive dashboards, automated alerts, and machine learning forecasts.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
The frontend is a React-based application using Vite, shadcn/ui, Radix UI, and Tailwind CSS for a modern, dark-themed UI. TanStack React Query manages server state, and Wouter handles client-side routing. Key features include an enterprise dashboard with global summaries, structured navigation, real-time WebSocket-integrated notifications, and dedicated intelligence hubs for Commodities, Maritime, and Energy Transition. Specific modules cover cargo chain visualization, inventory tracking, freight analytics, refinery intelligence, global supply & demand balances, and specialized commodity pillar packs (Crude & Products, LNG/LPG, Dry Bulk, Petrochem, Agri/Biofuel). Maritime intelligence includes AIS/Vessel tracking, Port/Event Engine, Container Intelligence, Bunkering/Fuel Events, Maritime Inbox, and Predictive Schedules. The platform also features a Signals & Predict page for AI-powered price predictions, Rotterdam port delay tracking, Rotterdam crude oil data visualization, a TankScope Classic Dashboard, and Sentinel-2 based satellite monitoring for the Rotterdam industrial cluster.

### Backend Architecture
The backend is an Express.js and TypeScript application following a REST API architecture (with a future GraphQL migration planned). It uses a three-layer structure for routes, business logic (services), and data storage. WebSocket connections provide real-time updates. Middleware handles logging, JSON parsing, and error management.

### Database Design
PostgreSQL with Drizzle ORM is used for type-safe database operations. The schema supports maritime time-series data, including tables for vessels, ports, AIS positions, port statistics, storage sites, signals, predictions, and detailed delay tracking. It also includes comprehensive tables for cargo operations, maritime intelligence (port calls, container operations, bunkering, communications), and commodity intelligence (refineries, supply-demand balances, research reports). The design emphasizes real-time data ingestion, historical analysis, and efficient time-series queries using UUIDs and indexing.

### Data Processing Pipeline
Background services include an AIS Service for real-time vessel position simulation, a Port Call Detection Service using Haversine geofencing, a Delay Service for calculating and alerting on vessel delays, a Signals Service for monitoring events, and a Prediction Service for ML-powered price forecasts. A Mock Data Service initializes baseline data.

### Phase One API Endpoints (V1)
The V1 API provides RESTful endpoints for authentication, ports (listing, details, calls), and vessels (listing, details, historical positions). Test credentials: admin@example.com / admin123. API documentation is available via Swagger/OpenAPI at `/docs`.

### Core Ports
Three core ports are configured with Haversine geofencing: Rotterdam (NLRTM), Singapore (SGSIN), and Fujairah (AEFJR).

### Production Infrastructure
The platform includes robust observability with structured logging, health endpoints (`/health`, `/ready`, `/live`), and a metrics endpoint (`/metrics`). Security features comprise JWT-based session management, rate limiting on authentication endpoints, and audit logging. Data quality is ensured through event logs, ingestion checkpoints, and data quality scores. ML infrastructure includes model registry and model predictions tables. User features are supported by watchlists and alert rules. Performance optimizations include an in-memory caching layer, opt-in pagination for list endpoints, geospatial queries, and cached endpoints for frequently accessed data. Pilot UX features include watchlists, configurable alert rules, CSV export functionality, and a 5-step onboarding modal.

## External Dependencies

### Core Infrastructure
- **Neon Database:** PostgreSQL hosting.
- **Drizzle ORM:** Type-safe database operations.
- **WebSocket:** Real-time communication.

### UI Framework
- **React + Vite:** Frontend development.
- **shadcn/ui + Radix UI:** Component library.
- **Tailwind CSS:** Styling.
- **TanStack React Query:** Server state management.
- **Wouter:** Lightweight routing.

### Development Tools
- **TypeScript:** Full-stack type safety.

### Planned Integrations
- **OpenAI API:** ML model integration.
- **Mapbox:** Interactive map visualization.
- **Maritime Data Providers:** AIS feeds, port APIs, satellite imagery.
- **GraphQL:** Future API architecture.