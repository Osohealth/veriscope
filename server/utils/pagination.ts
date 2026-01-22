import { Request } from 'express';

export interface PaginationParams {
  page: number;
  limit: number;
  offset: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
    hasNext: boolean;
    hasPrev: boolean;
  };
}

export function parsePaginationParams(req: Request, defaults: { page?: number; limit?: number } = {}): PaginationParams {
  const defaultPage = defaults.page ?? 1;
  const defaultLimit = defaults.limit ?? 50;
  const maxLimit = 500;

  let page = parseInt(req.query.page as string) || defaultPage;
  let limit = parseInt(req.query.limit as string) || defaultLimit;

  page = Math.max(1, page);
  limit = Math.min(Math.max(1, limit), maxLimit);

  const offset = (page - 1) * limit;

  return { page, limit, offset };
}

export function paginateArray<T>(items: T[], params: PaginationParams): PaginatedResponse<T> {
  const total = items.length;
  const totalPages = Math.ceil(total / params.limit);
  const data = items.slice(params.offset, params.offset + params.limit);

  return {
    data,
    pagination: {
      page: params.page,
      limit: params.limit,
      total,
      totalPages,
      hasNext: params.page < totalPages,
      hasPrev: params.page > 1
    }
  };
}

export function createPaginatedResponse<T>(
  data: T[],
  total: number,
  params: PaginationParams
): PaginatedResponse<T> {
  const totalPages = Math.ceil(total / params.limit);

  return {
    data,
    pagination: {
      page: params.page,
      limit: params.limit,
      total,
      totalPages,
      hasNext: params.page < totalPages,
      hasPrev: params.page > 1
    }
  };
}

export function buildSortParams(req: Request, allowedFields: string[], defaultField: string = 'createdAt', defaultOrder: 'asc' | 'desc' = 'desc') {
  const sortBy = (req.query.sortBy as string) || defaultField;
  const sortOrder = ((req.query.sortOrder as string) || defaultOrder).toLowerCase() as 'asc' | 'desc';

  const validSortBy = allowedFields.includes(sortBy) ? sortBy : defaultField;
  const validSortOrder = ['asc', 'desc'].includes(sortOrder) ? sortOrder : defaultOrder;

  return { sortBy: validSortBy, sortOrder: validSortOrder };
}

export function buildFilterParams(req: Request, allowedFilters: string[]): Record<string, string | undefined> {
  const filters: Record<string, string | undefined> = {};
  
  for (const filter of allowedFilters) {
    const value = req.query[filter];
    if (typeof value === 'string' && value.trim()) {
      filters[filter] = value.trim();
    }
  }

  return filters;
}

export interface GeoQueryParams {
  lat?: number;
  lng?: number;
  radiusKm?: number;
}

export function parseGeoQueryParams(req: Request): GeoQueryParams | null {
  const lat = parseFloat(req.query.lat as string);
  const lng = parseFloat(req.query.lng as string);
  const radiusKm = parseFloat(req.query.radiusKm as string) || 50;

  if (isNaN(lat) || isNaN(lng)) {
    return null;
  }

  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    return null;
  }

  return { lat, lng, radiusKm: Math.min(radiusKm, 500) };
}

export function haversineDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a = 
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function toRad(deg: number): number {
  return deg * (Math.PI / 180);
}

export function filterByGeoRadius<T extends { latitude?: number | string | null; longitude?: number | string | null }>(
  items: T[],
  geo: GeoQueryParams
): (T & { distance: number })[] {
  if (!geo.lat || !geo.lng) return items.map(item => ({ ...item, distance: 0 }));

  return items
    .map(item => {
      const lat = typeof item.latitude === 'string' ? parseFloat(item.latitude) : item.latitude;
      const lng = typeof item.longitude === 'string' ? parseFloat(item.longitude) : item.longitude;
      
      if (!lat || !lng) return null;

      const distance = haversineDistance(geo.lat!, geo.lng!, lat, lng);
      
      if (distance <= geo.radiusKm!) {
        return { ...item, distance };
      }
      return null;
    })
    .filter((item): item is T & { distance: number } => item !== null)
    .sort((a, b) => a.distance - b.distance);
}

export function getBoundingBox(lat: number, lng: number, radiusKm: number): { 
  minLat: number; maxLat: number; minLng: number; maxLng: number 
} {
  const latDelta = radiusKm / 111.32;
  const lngDelta = radiusKm / (111.32 * Math.cos(toRad(lat)));

  return {
    minLat: lat - latDelta,
    maxLat: lat + latDelta,
    minLng: lng - lngDelta,
    maxLng: lng + lngDelta
  };
}
