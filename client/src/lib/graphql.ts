// GraphQL client utility for future implementation
// Currently using REST endpoints, but structured for easy migration to GraphQL

export interface GraphQLQuery {
  query: string;
  variables?: Record<string, any>;
}

export interface GraphQLResponse<T> {
  data?: T;
  errors?: Array<{
    message: string;
    locations?: Array<{
      line: number;
      column: number;
    }>;
    path?: string[];
  }>;
}

export class GraphQLClient {
  private endpoint: string;
  
  constructor(endpoint = '/graphql') {
    this.endpoint = endpoint;
  }
  
  async query<T>(query: GraphQLQuery): Promise<GraphQLResponse<T>> {
    const response = await fetch(this.endpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(query),
      credentials: 'include',
    });
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    
    return await response.json();
  }
}

export const graphqlClient = new GraphQLClient();

// Example queries for future use
export const VESSEL_QUERY = `
  query GetVessels($bbox: BBox, $filter: VesselFilter) {
    vessels(bbox: $bbox, filter: $filter) {
      mmsi
      imo
      name
      type
      lat
      lon
      sog
      heading
      status
      draught
      lastUpdate
    }
  }
`;

export const PORT_STATS_QUERY = `
  query GetPortStats($portId: ID!, $range: DateRange!) {
    portStats(portId: $portId, range: $range) {
      date
      arrivals
      departures
      queueLen
      avgWaitHours
      byClass {
        vesselClass
        count
      }
    }
  }
`;

export const SIGNALS_QUERY = `
  query GetSignals($filter: SignalFilter, $range: DateRange) {
    signals(filter: $filter, range: $range) {
      id
      ts
      entityId
      entityType
      severity
      summary
      payload
    }
  }
`;

export const PREDICTIONS_QUERY = `
  query GetPredictions($target: PredictionTarget!, $range: DateRange!) {
    predictions(target: $target, range: $range) {
      date
      target
      class
      probUp
      probDown
      modelVersion
    }
  }
`;
