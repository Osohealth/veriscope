import { db } from "../src/db";
import { ports } from "../drizzle/schema";

const DEFAULT_GEOFENCE_RADIUS_KM = 5;

type PortSeed = {
  name: string;
  countryCode: string;
  lat: number;
  lon: number;
  geofenceRadiusKm?: number;
};

const PORTS: PortSeed[] = [
  { name: "Port of Rotterdam", countryCode: "NL", lat: 51.94, lon: 4.14 },
  { name: "Port of Amsterdam", countryCode: "NL", lat: 52.38, lon: 4.89 },
  { name: "Port of Antwerp", countryCode: "BE", lat: 51.26, lon: 4.41 },
  { name: "Port of Hamburg", countryCode: "DE", lat: 53.54, lon: 9.99 },
  { name: "Port of Bremerhaven", countryCode: "DE", lat: 53.55, lon: 8.58 },
  { name: "Port of Felixstowe", countryCode: "GB", lat: 51.96, lon: 1.32 },
  { name: "Port of London", countryCode: "GB", lat: 51.51, lon: 0.04 },
  { name: "Port of Le Havre", countryCode: "FR", lat: 49.49, lon: 0.12 },
  { name: "Port of Marseille", countryCode: "FR", lat: 43.35, lon: 5.33 },
  { name: "Port of Valencia", countryCode: "ES", lat: 39.44, lon: -0.33 },
  { name: "Port of Barcelona", countryCode: "ES", lat: 41.35, lon: 2.17 },
  { name: "Port of Algeciras", countryCode: "ES", lat: 36.13, lon: -5.45 },
  { name: "Port of Genoa", countryCode: "IT", lat: 44.41, lon: 8.92 },
  { name: "Port of Trieste", countryCode: "IT", lat: 45.65, lon: 13.77 },
  { name: "Port of Piraeus", countryCode: "GR", lat: 37.94, lon: 23.63 },
  { name: "Port of Istanbul", countryCode: "TR", lat: 41.01, lon: 28.98 },
  { name: "Port of Constanta", countryCode: "RO", lat: 44.18, lon: 28.65 },
  { name: "Port of Gdansk", countryCode: "PL", lat: 54.37, lon: 18.64 },
  { name: "Port of Gothenburg", countryCode: "SE", lat: 57.71, lon: 11.95 },
  { name: "Port of Aarhus", countryCode: "DK", lat: 56.16, lon: 10.22 },
  { name: "Port of Oslo", countryCode: "NO", lat: 59.9, lon: 10.76 },
  { name: "Port of Dublin", countryCode: "IE", lat: 53.35, lon: -6.2 },
  { name: "Port of Lisbon", countryCode: "PT", lat: 38.71, lon: -9.13 },
  { name: "Port of Cape Town", countryCode: "ZA", lat: -33.92, lon: 18.43 },
  { name: "Port of Durban", countryCode: "ZA", lat: -29.87, lon: 31.05 },
  { name: "Port of Lagos", countryCode: "NG", lat: 6.45, lon: 3.39 },
  { name: "Port of Mombasa", countryCode: "KE", lat: -4.05, lon: 39.66 },
  { name: "Port of Djibouti", countryCode: "DJ", lat: 11.59, lon: 43.14 },
  { name: "Port of Jeddah", countryCode: "SA", lat: 21.48, lon: 39.16 },
  { name: "Port of Dammam", countryCode: "SA", lat: 26.42, lon: 50.1 },
  { name: "Port of Dubai", countryCode: "AE", lat: 25.0, lon: 55.07 },
  { name: "Port of Abu Dhabi", countryCode: "AE", lat: 24.5, lon: 54.38 },
  { name: "Port of Doha", countryCode: "QA", lat: 25.29, lon: 51.55 },
  { name: "Port of Karachi", countryCode: "PK", lat: 24.84, lon: 66.98 },
  { name: "Port of Colombo", countryCode: "LK", lat: 6.93, lon: 79.85 },
  { name: "Port of Mumbai", countryCode: "IN", lat: 18.94, lon: 72.84 },
  { name: "Port of Mundra", countryCode: "IN", lat: 22.76, lon: 69.7 },
  { name: "Port of Chennai", countryCode: "IN", lat: 13.09, lon: 80.29 },
  { name: "Port of Visakhapatnam", countryCode: "IN", lat: 17.68, lon: 83.3 },
  { name: "Port of Singapore", countryCode: "SG", lat: 1.26, lon: 103.84 },
  { name: "Port of Tanjung Pelepas", countryCode: "MY", lat: 1.35, lon: 103.54 },
  { name: "Port of Port Klang", countryCode: "MY", lat: 2.99, lon: 101.38 },
  { name: "Port of Jakarta", countryCode: "ID", lat: -6.11, lon: 106.88 },
  { name: "Port of Surabaya", countryCode: "ID", lat: -7.21, lon: 112.73 },
  { name: "Port of Manila", countryCode: "PH", lat: 14.59, lon: 120.97 },
  { name: "Port of Laem Chabang", countryCode: "TH", lat: 13.08, lon: 100.9 },
  { name: "Port of Hai Phong", countryCode: "VN", lat: 20.86, lon: 106.68 },
  { name: "Port of Ho Chi Minh", countryCode: "VN", lat: 10.77, lon: 106.7 },
  { name: "Port of Hong Kong", countryCode: "HK", lat: 22.3, lon: 114.17 },
  { name: "Port of Shanghai", countryCode: "CN", lat: 31.23, lon: 121.49 },
  { name: "Port of Ningbo", countryCode: "CN", lat: 29.87, lon: 121.55 },
  { name: "Port of Qingdao", countryCode: "CN", lat: 36.07, lon: 120.33 },
  { name: "Port of Tianjin", countryCode: "CN", lat: 39.0, lon: 117.72 },
  { name: "Port of Shenzhen", countryCode: "CN", lat: 22.54, lon: 114.12 },
  { name: "Port of Guangzhou", countryCode: "CN", lat: 23.13, lon: 113.27 },
  { name: "Port of Xiamen", countryCode: "CN", lat: 24.48, lon: 118.09 },
  { name: "Port of Dalian", countryCode: "CN", lat: 38.92, lon: 121.63 },
  { name: "Port of Busan", countryCode: "KR", lat: 35.1, lon: 129.04 },
  { name: "Port of Incheon", countryCode: "KR", lat: 37.45, lon: 126.62 },
  { name: "Port of Tokyo", countryCode: "JP", lat: 35.63, lon: 139.79 },
  { name: "Port of Yokohama", countryCode: "JP", lat: 35.45, lon: 139.65 },
  { name: "Port of Kobe", countryCode: "JP", lat: 34.68, lon: 135.2 },
  { name: "Port of Osaka", countryCode: "JP", lat: 34.66, lon: 135.43 },
  { name: "Port of Nagoya", countryCode: "JP", lat: 35.1, lon: 136.88 },
  { name: "Port of Vancouver", countryCode: "CA", lat: 49.29, lon: -123.11 },
  { name: "Port of Prince Rupert", countryCode: "CA", lat: 54.32, lon: -130.32 },
  { name: "Port of Los Angeles", countryCode: "US", lat: 33.74, lon: -118.27 },
  { name: "Port of Long Beach", countryCode: "US", lat: 33.75, lon: -118.19 },
  { name: "Port of Oakland", countryCode: "US", lat: 37.8, lon: -122.31 },
  { name: "Port of Seattle", countryCode: "US", lat: 47.6, lon: -122.34 },
  { name: "Port of Tacoma", countryCode: "US", lat: 47.25, lon: -122.44 },
  { name: "Port of Houston", countryCode: "US", lat: 29.73, lon: -95.26 },
  { name: "Port of New Orleans", countryCode: "US", lat: 29.94, lon: -90.06 },
  { name: "Port of Miami", countryCode: "US", lat: 25.78, lon: -80.17 },
  { name: "Port of Savannah", countryCode: "US", lat: 32.08, lon: -81.09 },
  { name: "Port of Charleston", countryCode: "US", lat: 32.78, lon: -79.93 },
  { name: "Port of Norfolk", countryCode: "US", lat: 36.85, lon: -76.29 },
  { name: "Port of New York/New Jersey", countryCode: "US", lat: 40.66, lon: -74.08 },
  { name: "Port of Baltimore", countryCode: "US", lat: 39.26, lon: -76.58 },
  { name: "Port of Montreal", countryCode: "CA", lat: 45.5, lon: -73.55 },
  { name: "Port of Halifax", countryCode: "CA", lat: 44.65, lon: -63.57 },
  { name: "Port of Veracruz", countryCode: "MX", lat: 19.2, lon: -96.13 },
  { name: "Port of Santos", countryCode: "BR", lat: -23.95, lon: -46.33 },
  { name: "Port of Rio de Janeiro", countryCode: "BR", lat: -22.9, lon: -43.17 },
  { name: "Port of Buenos Aires", countryCode: "AR", lat: -34.6, lon: -58.37 },
  { name: "Port of Montevideo", countryCode: "UY", lat: -34.9, lon: -56.2 },
  { name: "Port of Callao", countryCode: "PE", lat: -12.05, lon: -77.15 },
  { name: "Port of Cartagena", countryCode: "CO", lat: 10.39, lon: -75.53 },
  { name: "Port of Colon", countryCode: "PA", lat: 9.36, lon: -79.9 },
  { name: "Port of Balboa", countryCode: "PA", lat: 8.95, lon: -79.55 },
  { name: "Port of Guayaquil", countryCode: "EC", lat: -2.2, lon: -79.9 },
  { name: "Port of San Antonio", countryCode: "CL", lat: -33.59, lon: -71.61 },
  { name: "Port of Valparaiso", countryCode: "CL", lat: -33.03, lon: -71.63 },
  { name: "Port of Auckland", countryCode: "NZ", lat: -36.84, lon: 174.77 },
  { name: "Port of Tauranga", countryCode: "NZ", lat: -37.7, lon: 176.17 },
  { name: "Port of Brisbane", countryCode: "AU", lat: -27.37, lon: 153.16 },
  { name: "Port of Sydney", countryCode: "AU", lat: -33.86, lon: 151.2 },
  { name: "Port of Melbourne", countryCode: "AU", lat: -37.83, lon: 144.94 },
  { name: "Port of Fremantle", countryCode: "AU", lat: -32.05, lon: 115.75 },
  { name: "Port of Brisbane (Fisherman Islands)", countryCode: "AU", lat: -27.4, lon: 153.18 },
  { name: "Port of Suape", countryCode: "BR", lat: -8.38, lon: -34.95 },
  { name: "Port of Alexandria", countryCode: "EG", lat: 31.2, lon: 29.88 },
  { name: "Port of Said", countryCode: "EG", lat: 31.26, lon: 32.3 },
  { name: "Port of Tangier Med", countryCode: "MA", lat: 35.89, lon: -5.5 },
  { name: "Port of Casablanca", countryCode: "MA", lat: 33.59, lon: -7.62 },
  { name: "Port of Tema", countryCode: "GH", lat: 5.64, lon: 0.01 },
  { name: "Port of Lome", countryCode: "TG", lat: 6.13, lon: 1.29 },
  { name: "Port of Abidjan", countryCode: "CI", lat: 5.31, lon: -4.02 },
  { name: "Port of Matadi", countryCode: "CD", lat: -5.82, lon: 13.45 },
];

async function seedPorts(): Promise<void> {
  const values = PORTS.map((port) => ({
    name: port.name,
    countryCode: port.countryCode,
    lat: port.lat,
    lon: port.lon,
    geofenceRadiusKm: port.geofenceRadiusKm ?? DEFAULT_GEOFENCE_RADIUS_KM,
  }));

  await db
    .insert(ports)
    .values(values)
    .onConflictDoNothing({ target: [ports.name, ports.countryCode] });

  console.log(`Seeded ${values.length} ports.`);
}

seedPorts().catch((error) => {
  console.error("Failed to seed ports", error);
  process.exitCode = 1;
});
