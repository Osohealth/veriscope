export const openApiSpec = {
  openapi: '3.0.3',
  info: {
    title: 'Maritime Intelligence API',
    version: '1.0.0',
    description: 'Phase One Maritime Intelligence Platform API with AIS-based vessel tracking, port geofencing, and real-time position monitoring.',
  },
  servers: [
    { url: '/v1', description: 'API v1' }
  ],
  components: {
    securitySchemes: {
      bearerAuth: {
        type: 'http',
        scheme: 'bearer',
        bearerFormat: 'JWT'
      }
    },
    schemas: {
      Port: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid' },
          name: { type: 'string' },
          unlocode: { type: 'string' },
          country_code: { type: 'string' },
          latitude: { type: 'number' },
          longitude: { type: 'number' },
          timezone: { type: 'string' }
        }
      },
      PortDetail: {
        allOf: [
          { $ref: '#/components/schemas/Port' },
          {
            type: 'object',
            properties: {
              metrics_7d: {
                type: 'object',
                properties: {
                  arrivals: { type: 'integer' },
                  departures: { type: 'integer' },
                  unique_vessels: { type: 'integer' },
                  avg_dwell_hours: { type: 'number' }
                }
              }
            }
          }
        ]
      },
      PortCall: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid' },
          vessel_id: { type: 'string', format: 'uuid' },
          vessel_name: { type: 'string' },
          arrival_time_utc: { type: 'string', format: 'date-time' },
          departure_time_utc: { type: 'string', format: 'date-time', nullable: true },
          dwell_hours: { type: 'number', nullable: true }
        }
      },
      Vessel: {
        type: 'object',
        properties: {
          id: { type: 'string', format: 'uuid' },
          mmsi: { type: 'string' },
          imo: { type: 'string' },
          name: { type: 'string' },
          vessel_type: { type: 'string' },
          flag: { type: 'string' }
        }
      },
      VesselPosition: {
        type: 'object',
        properties: {
          vessel_id: { type: 'string', format: 'uuid' },
          latitude: { type: 'number' },
          longitude: { type: 'number' },
          timestamp_utc: { type: 'string', format: 'date-time' },
          sog_knots: { type: 'number' },
          cog_deg: { type: 'number' },
          nav_status: { type: 'string' }
        }
      },
      AuthResponse: {
        type: 'object',
        properties: {
          access_token: { type: 'string' },
          refresh_token: { type: 'string' },
          token_type: { type: 'string' },
          expires_in: { type: 'integer' }
        }
      },
      Error: {
        type: 'object',
        properties: {
          error: { type: 'string' }
        }
      }
    }
  },
  paths: {
    '/auth/register': {
      post: {
        tags: ['Authentication'],
        summary: 'Register a new user',
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: {
                type: 'object',
                required: ['email', 'password', 'full_name'],
                properties: {
                  email: { type: 'string', format: 'email' },
                  password: { type: 'string', minLength: 8 },
                  full_name: { type: 'string' },
                  organization_name: { type: 'string' }
                }
              }
            }
          }
        },
        responses: {
          '201': {
            description: 'User registered successfully',
            content: { 'application/json': { schema: { $ref: '#/components/schemas/AuthResponse' } } }
          },
          '400': { description: 'Invalid input', content: { 'application/json': { schema: { $ref: '#/components/schemas/Error' } } } }
        }
      }
    },
    '/auth/login': {
      post: {
        tags: ['Authentication'],
        summary: 'Login with email and password',
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: {
                type: 'object',
                required: ['email', 'password'],
                properties: {
                  email: { type: 'string', format: 'email' },
                  password: { type: 'string' }
                }
              }
            }
          }
        },
        responses: {
          '200': {
            description: 'Login successful',
            content: { 'application/json': { schema: { $ref: '#/components/schemas/AuthResponse' } } }
          },
          '401': { description: 'Invalid credentials' }
        }
      }
    },
    '/auth/refresh': {
      post: {
        tags: ['Authentication'],
        summary: 'Refresh access token',
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: {
                type: 'object',
                required: ['refresh_token'],
                properties: {
                  refresh_token: { type: 'string' }
                }
              }
            }
          }
        },
        responses: {
          '200': {
            description: 'Token refreshed',
            content: { 'application/json': { schema: { $ref: '#/components/schemas/AuthResponse' } } }
          },
          '401': { description: 'Invalid refresh token' }
        }
      }
    },
    '/ports': {
      get: {
        tags: ['Ports'],
        summary: 'List all ports',
        security: [{ bearerAuth: [] }],
        parameters: [
          { name: 'country_code', in: 'query', schema: { type: 'string' }, description: 'Filter by country code' },
          { name: 'search', in: 'query', schema: { type: 'string' }, description: 'Search by name or UNLOCODE' }
        ],
        responses: {
          '200': {
            description: 'List of ports',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    items: { type: 'array', items: { $ref: '#/components/schemas/Port' } },
                    total: { type: 'integer' }
                  }
                }
              }
            }
          }
        }
      }
    },
    '/ports/{port_id}': {
      get: {
        tags: ['Ports'],
        summary: 'Get port details with 7-day KPIs',
        security: [{ bearerAuth: [] }],
        parameters: [
          { name: 'port_id', in: 'path', required: true, schema: { type: 'string', format: 'uuid' } }
        ],
        responses: {
          '200': {
            description: 'Port details with metrics',
            content: { 'application/json': { schema: { $ref: '#/components/schemas/PortDetail' } } }
          },
          '404': { description: 'Port not found' }
        }
      }
    },
    '/ports/{port_id}/calls': {
      get: {
        tags: ['Ports'],
        summary: 'Get port calls history',
        security: [{ bearerAuth: [] }],
        parameters: [
          { name: 'port_id', in: 'path', required: true, schema: { type: 'string', format: 'uuid' } },
          { name: 'start_time', in: 'query', schema: { type: 'string', format: 'date-time' } },
          { name: 'end_time', in: 'query', schema: { type: 'string', format: 'date-time' } },
          { name: 'limit', in: 'query', schema: { type: 'integer', default: 100, maximum: 500 } }
        ],
        responses: {
          '200': {
            description: 'List of port calls',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    items: { type: 'array', items: { $ref: '#/components/schemas/PortCall' } }
                  }
                }
              }
            }
          }
        }
      }
    },
    '/vessels': {
      get: {
        tags: ['Vessels'],
        summary: 'List vessels',
        security: [{ bearerAuth: [] }],
        parameters: [
          { name: 'mmsi', in: 'query', schema: { type: 'string' } },
          { name: 'imo', in: 'query', schema: { type: 'string' } },
          { name: 'name', in: 'query', schema: { type: 'string' } }
        ],
        responses: {
          '200': {
            description: 'List of vessels',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    items: { type: 'array', items: { $ref: '#/components/schemas/Vessel' } },
                    total: { type: 'integer' }
                  }
                }
              }
            }
          }
        }
      }
    },
    '/vessels/{vessel_id}/latest_position': {
      get: {
        tags: ['Vessels'],
        summary: 'Get latest vessel position',
        security: [{ bearerAuth: [] }],
        parameters: [
          { name: 'vessel_id', in: 'path', required: true, schema: { type: 'string', format: 'uuid' } }
        ],
        responses: {
          '200': {
            description: 'Latest vessel position',
            content: { 'application/json': { schema: { $ref: '#/components/schemas/VesselPosition' } } }
          },
          '404': { description: 'No position found' }
        }
      }
    },
    '/vessels/positions': {
      get: {
        tags: ['Vessels'],
        summary: 'Get all vessel positions as GeoJSON',
        security: [{ bearerAuth: [] }],
        responses: {
          '200': {
            description: 'GeoJSON FeatureCollection of vessel positions',
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    type: { type: 'string', enum: ['FeatureCollection'] },
                    features: { type: 'array', items: { type: 'object' } }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
};
