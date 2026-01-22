import { db } from '../db';
import { ports, vessels, portCalls } from '@shared/schema';
import { sql } from 'drizzle-orm';

const globalPorts = [
  { name: "Rotterdam", code: "NLRTM", unlocode: "NLRTM", country: "Netherlands", countryCode: "NL", region: "Europe", lat: 51.9225, lng: 4.4792, timezone: "Europe/Amsterdam", type: "oil_terminal" },
  { name: "Singapore", code: "SGSIN", unlocode: "SGSIN", country: "Singapore", countryCode: "SG", region: "Asia", lat: 1.2644, lng: 103.8203, timezone: "Asia/Singapore", type: "container_port" },
  { name: "Fujairah", code: "AEFJR", unlocode: "AEFJR", country: "UAE", countryCode: "AE", region: "Middle East", lat: 25.1288, lng: 56.3366, timezone: "Asia/Dubai", type: "oil_terminal" },
  { name: "Shanghai", code: "CNSHA", unlocode: "CNSHA", country: "China", countryCode: "CN", region: "Asia", lat: 31.2304, lng: 121.4737, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Ningbo-Zhoushan", code: "CNNGB", unlocode: "CNNGB", country: "China", countryCode: "CN", region: "Asia", lat: 29.8683, lng: 121.5440, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Busan", code: "KRPUS", unlocode: "KRPUS", country: "South Korea", countryCode: "KR", region: "Asia", lat: 35.1796, lng: 129.0756, timezone: "Asia/Seoul", type: "container_port" },
  { name: "Hong Kong", code: "HKHKG", unlocode: "HKHKG", country: "Hong Kong", countryCode: "HK", region: "Asia", lat: 22.3193, lng: 114.1694, timezone: "Asia/Hong_Kong", type: "container_port" },
  { name: "Shenzhen", code: "CNSZX", unlocode: "CNSZX", country: "China", countryCode: "CN", region: "Asia", lat: 22.5431, lng: 114.0579, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Guangzhou", code: "CNGZU", unlocode: "CNGZU", country: "China", countryCode: "CN", region: "Asia", lat: 23.1291, lng: 113.2644, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Qingdao", code: "CNTAO", unlocode: "CNTAO", country: "China", countryCode: "CN", region: "Asia", lat: 36.0671, lng: 120.3826, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Tianjin", code: "CNTSN", unlocode: "CNTSN", country: "China", countryCode: "CN", region: "Asia", lat: 39.0842, lng: 117.2009, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Dubai (Jebel Ali)", code: "AEJEA", unlocode: "AEJEA", country: "UAE", countryCode: "AE", region: "Middle East", lat: 25.0075, lng: 55.0628, timezone: "Asia/Dubai", type: "container_port" },
  { name: "Port Klang", code: "MYPKG", unlocode: "MYPKG", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 2.9995, lng: 101.3927, timezone: "Asia/Kuala_Lumpur", type: "container_port" },
  { name: "Antwerp", code: "BEANR", unlocode: "BEANR", country: "Belgium", countryCode: "BE", region: "Europe", lat: 51.2194, lng: 4.4025, timezone: "Europe/Brussels", type: "container_port" },
  { name: "Hamburg", code: "DEHAM", unlocode: "DEHAM", country: "Germany", countryCode: "DE", region: "Europe", lat: 53.5511, lng: 9.9937, timezone: "Europe/Berlin", type: "container_port" },
  { name: "Los Angeles", code: "USLAX", unlocode: "USLAX", country: "United States", countryCode: "US", region: "North America", lat: 33.7361, lng: -118.2611, timezone: "America/Los_Angeles", type: "container_port" },
  { name: "Long Beach", code: "USLGB", unlocode: "USLGB", country: "United States", countryCode: "US", region: "North America", lat: 33.7551, lng: -118.2162, timezone: "America/Los_Angeles", type: "container_port" },
  { name: "New York/New Jersey", code: "USNYC", unlocode: "USNYC", country: "United States", countryCode: "US", region: "North America", lat: 40.6608, lng: -74.0442, timezone: "America/New_York", type: "container_port" },
  { name: "Savannah", code: "USSAV", unlocode: "USSAV", country: "United States", countryCode: "US", region: "North America", lat: 32.0809, lng: -81.0912, timezone: "America/New_York", type: "container_port" },
  { name: "Houston", code: "USHOU", unlocode: "USHOU", country: "United States", countryCode: "US", region: "North America", lat: 29.7604, lng: -95.3698, timezone: "America/Chicago", type: "oil_terminal" },
  { name: "Tanjung Pelepas", code: "MYTPP", unlocode: "MYTPP", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 1.3621, lng: 103.5514, timezone: "Asia/Kuala_Lumpur", type: "container_port" },
  { name: "Kaohsiung", code: "TWKHH", unlocode: "TWKHH", country: "Taiwan", countryCode: "TW", region: "Asia", lat: 22.6273, lng: 120.3014, timezone: "Asia/Taipei", type: "container_port" },
  { name: "Laem Chabang", code: "THLCH", unlocode: "THLCH", country: "Thailand", countryCode: "TH", region: "Asia", lat: 13.0827, lng: 100.8814, timezone: "Asia/Bangkok", type: "container_port" },
  { name: "Colombo", code: "LKCMB", unlocode: "LKCMB", country: "Sri Lanka", countryCode: "LK", region: "Asia", lat: 6.9271, lng: 79.8612, timezone: "Asia/Colombo", type: "container_port" },
  { name: "Tanjung Priok (Jakarta)", code: "IDJKT", unlocode: "IDJKT", country: "Indonesia", countryCode: "ID", region: "Asia", lat: -6.1058, lng: 106.8788, timezone: "Asia/Jakarta", type: "container_port" },
  { name: "Ho Chi Minh City", code: "VNSGN", unlocode: "VNSGN", country: "Vietnam", countryCode: "VN", region: "Asia", lat: 10.8231, lng: 106.6297, timezone: "Asia/Ho_Chi_Minh", type: "container_port" },
  { name: "Haiphong", code: "VNHPH", unlocode: "VNHPH", country: "Vietnam", countryCode: "VN", region: "Asia", lat: 20.8449, lng: 106.6881, timezone: "Asia/Ho_Chi_Minh", type: "container_port" },
  { name: "Manila", code: "PHMNL", unlocode: "PHMNL", country: "Philippines", countryCode: "PH", region: "Asia", lat: 14.5995, lng: 120.9842, timezone: "Asia/Manila", type: "container_port" },
  { name: "Tokyo", code: "JPTYO", unlocode: "JPTYO", country: "Japan", countryCode: "JP", region: "Asia", lat: 35.6762, lng: 139.6503, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Yokohama", code: "JPYOK", unlocode: "JPYOK", country: "Japan", countryCode: "JP", region: "Asia", lat: 35.4437, lng: 139.6380, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Kobe", code: "JPUKB", unlocode: "JPUKB", country: "Japan", countryCode: "JP", region: "Asia", lat: 34.6901, lng: 135.1956, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Nagoya", code: "JPNGO", unlocode: "JPNGO", country: "Japan", countryCode: "JP", region: "Asia", lat: 35.1815, lng: 136.9066, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Osaka", code: "JPOSA", unlocode: "JPOSA", country: "Japan", countryCode: "JP", region: "Asia", lat: 34.6937, lng: 135.5023, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Felixstowe", code: "GBFXT", unlocode: "GBFXT", country: "United Kingdom", countryCode: "GB", region: "Europe", lat: 51.9611, lng: 1.3508, timezone: "Europe/London", type: "container_port" },
  { name: "Southampton", code: "GBSOU", unlocode: "GBSOU", country: "United Kingdom", countryCode: "GB", region: "Europe", lat: 50.9097, lng: -1.4044, timezone: "Europe/London", type: "container_port" },
  { name: "London Gateway", code: "GBLGP", unlocode: "GBLGP", country: "United Kingdom", countryCode: "GB", region: "Europe", lat: 51.4897, lng: 0.4656, timezone: "Europe/London", type: "container_port" },
  { name: "Bremen/Bremerhaven", code: "DEBRV", unlocode: "DEBRV", country: "Germany", countryCode: "DE", region: "Europe", lat: 53.5396, lng: 8.5809, timezone: "Europe/Berlin", type: "container_port" },
  { name: "Le Havre", code: "FRLEH", unlocode: "FRLEH", country: "France", countryCode: "FR", region: "Europe", lat: 49.4944, lng: 0.1079, timezone: "Europe/Paris", type: "container_port" },
  { name: "Marseille", code: "FRMRS", unlocode: "FRMRS", country: "France", countryCode: "FR", region: "Europe", lat: 43.2965, lng: 5.3698, timezone: "Europe/Paris", type: "container_port" },
  { name: "Barcelona", code: "ESBCN", unlocode: "ESBCN", country: "Spain", countryCode: "ES", region: "Europe", lat: 41.3851, lng: 2.1734, timezone: "Europe/Madrid", type: "container_port" },
  { name: "Valencia", code: "ESVLC", unlocode: "ESVLC", country: "Spain", countryCode: "ES", region: "Europe", lat: 39.4699, lng: -0.3763, timezone: "Europe/Madrid", type: "container_port" },
  { name: "Algeciras", code: "ESALG", unlocode: "ESALG", country: "Spain", countryCode: "ES", region: "Europe", lat: 36.1408, lng: -5.4537, timezone: "Europe/Madrid", type: "container_port" },
  { name: "Piraeus", code: "GRPIR", unlocode: "GRPIR", country: "Greece", countryCode: "GR", region: "Europe", lat: 37.9475, lng: 23.6412, timezone: "Europe/Athens", type: "container_port" },
  { name: "Genoa", code: "ITGOA", unlocode: "ITGOA", country: "Italy", countryCode: "IT", region: "Europe", lat: 44.4056, lng: 8.9463, timezone: "Europe/Rome", type: "container_port" },
  { name: "Gioia Tauro", code: "ITGIT", unlocode: "ITGIT", country: "Italy", countryCode: "IT", region: "Europe", lat: 38.4381, lng: 15.8910, timezone: "Europe/Rome", type: "container_port" },
  { name: "La Spezia", code: "ITLSP", unlocode: "ITLSP", country: "Italy", countryCode: "IT", region: "Europe", lat: 44.1024, lng: 9.8241, timezone: "Europe/Rome", type: "container_port" },
  { name: "Venice", code: "ITVCE", unlocode: "ITVCE", country: "Italy", countryCode: "IT", region: "Europe", lat: 45.4408, lng: 12.3155, timezone: "Europe/Rome", type: "container_port" },
  { name: "Trieste", code: "ITTRS", unlocode: "ITTRS", country: "Italy", countryCode: "IT", region: "Europe", lat: 45.6495, lng: 13.7768, timezone: "Europe/Rome", type: "container_port" },
  { name: "Gdansk", code: "PLGDN", unlocode: "PLGDN", country: "Poland", countryCode: "PL", region: "Europe", lat: 54.3520, lng: 18.6466, timezone: "Europe/Warsaw", type: "container_port" },
  { name: "St. Petersburg", code: "RULED", unlocode: "RULED", country: "Russia", countryCode: "RU", region: "Europe", lat: 59.9311, lng: 30.3609, timezone: "Europe/Moscow", type: "container_port" },
  { name: "Ras Tanura", code: "SARTA", unlocode: "SARTA", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: 26.6385, lng: 50.0508, timezone: "Asia/Riyadh", type: "oil_terminal" },
  { name: "Jubail", code: "SAJUB", unlocode: "SAJUB", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: 27.0174, lng: 49.6682, timezone: "Asia/Riyadh", type: "oil_terminal" },
  { name: "Yanbu", code: "SAYNB", unlocode: "SAYNB", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: 24.0895, lng: 38.0618, timezone: "Asia/Riyadh", type: "oil_terminal" },
  { name: "Jeddah", code: "SAJED", unlocode: "SAJED", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: 21.4858, lng: 39.1925, timezone: "Asia/Riyadh", type: "container_port" },
  { name: "Dammam", code: "SADMM", unlocode: "SADMM", country: "Saudi Arabia", countryCode: "SA", region: "Middle East", lat: 26.4207, lng: 50.0888, timezone: "Asia/Riyadh", type: "container_port" },
  { name: "Mina Al Ahmadi", code: "KWMAA", unlocode: "KWMAA", country: "Kuwait", countryCode: "KW", region: "Middle East", lat: 29.0765, lng: 48.1698, timezone: "Asia/Kuwait", type: "oil_terminal" },
  { name: "Basra", code: "IQBSR", unlocode: "IQBSR", country: "Iraq", countryCode: "IQ", region: "Middle East", lat: 30.5085, lng: 47.8130, timezone: "Asia/Baghdad", type: "oil_terminal" },
  { name: "Umm Qasr", code: "IQUQR", unlocode: "IQUQR", country: "Iraq", countryCode: "IQ", region: "Middle East", lat: 30.0271, lng: 47.9267, timezone: "Asia/Baghdad", type: "container_port" },
  { name: "Bandar Abbas", code: "IRBND", unlocode: "IRBND", country: "Iran", countryCode: "IR", region: "Middle East", lat: 27.1865, lng: 56.2808, timezone: "Asia/Tehran", type: "container_port" },
  { name: "Kharg Island", code: "IRKHI", unlocode: "IRKHI", country: "Iran", countryCode: "IR", region: "Middle East", lat: 29.2327, lng: 50.3254, timezone: "Asia/Tehran", type: "oil_terminal" },
  { name: "Muscat", code: "OMMCT", unlocode: "OMMCT", country: "Oman", countryCode: "OM", region: "Middle East", lat: 23.6139, lng: 58.5922, timezone: "Asia/Muscat", type: "container_port" },
  { name: "Sohar", code: "OMSOH", unlocode: "OMSOH", country: "Oman", countryCode: "OM", region: "Middle East", lat: 24.3499, lng: 56.7470, timezone: "Asia/Muscat", type: "container_port" },
  { name: "Salalah", code: "OMSLL", unlocode: "OMSLL", country: "Oman", countryCode: "OM", region: "Middle East", lat: 16.9554, lng: 54.0049, timezone: "Asia/Muscat", type: "container_port" },
  { name: "Aden", code: "YEADE", unlocode: "YEADE", country: "Yemen", countryCode: "YE", region: "Middle East", lat: 12.7855, lng: 45.0186, timezone: "Asia/Aden", type: "container_port" },
  { name: "Doha", code: "QADOH", unlocode: "QADOH", country: "Qatar", countryCode: "QA", region: "Middle East", lat: 25.2854, lng: 51.5310, timezone: "Asia/Qatar", type: "container_port" },
  { name: "Ras Laffan", code: "QARAF", unlocode: "QARAF", country: "Qatar", countryCode: "QA", region: "Middle East", lat: 25.9300, lng: 51.5300, timezone: "Asia/Qatar", type: "lng_terminal" },
  { name: "Bahrain", code: "BHBAH", unlocode: "BHBAH", country: "Bahrain", countryCode: "BH", region: "Middle East", lat: 26.2285, lng: 50.5860, timezone: "Asia/Bahrain", type: "container_port" },
  { name: "Mumbai (JNPT)", code: "INJNP", unlocode: "INJNP", country: "India", countryCode: "IN", region: "Asia", lat: 18.9500, lng: 72.9500, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Chennai", code: "INMAA", unlocode: "INMAA", country: "India", countryCode: "IN", region: "Asia", lat: 13.0827, lng: 80.2707, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Kolkata", code: "INCCU", unlocode: "INCCU", country: "India", countryCode: "IN", region: "Asia", lat: 22.5726, lng: 88.3639, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Mundra", code: "INMUN", unlocode: "INMUN", country: "India", countryCode: "IN", region: "Asia", lat: 22.8389, lng: 69.7199, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Visakhapatnam", code: "INVTZ", unlocode: "INVTZ", country: "India", countryCode: "IN", region: "Asia", lat: 17.6868, lng: 83.2185, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Cochin", code: "INCOK", unlocode: "INCOK", country: "India", countryCode: "IN", region: "Asia", lat: 9.9312, lng: 76.2673, timezone: "Asia/Kolkata", type: "container_port" },
  { name: "Karachi", code: "PKKHI", unlocode: "PKKHI", country: "Pakistan", countryCode: "PK", region: "Asia", lat: 24.8607, lng: 67.0011, timezone: "Asia/Karachi", type: "container_port" },
  { name: "Chittagong", code: "BDCGP", unlocode: "BDCGP", country: "Bangladesh", countryCode: "BD", region: "Asia", lat: 22.3569, lng: 91.7832, timezone: "Asia/Dhaka", type: "container_port" },
  { name: "Durban", code: "ZADUR", unlocode: "ZADUR", country: "South Africa", countryCode: "ZA", region: "Africa", lat: -29.8587, lng: 31.0218, timezone: "Africa/Johannesburg", type: "container_port" },
  { name: "Cape Town", code: "ZACPT", unlocode: "ZACPT", country: "South Africa", countryCode: "ZA", region: "Africa", lat: -33.9249, lng: 18.4241, timezone: "Africa/Johannesburg", type: "container_port" },
  { name: "Port Elizabeth", code: "ZAPLZ", unlocode: "ZAPLZ", country: "South Africa", countryCode: "ZA", region: "Africa", lat: -33.9608, lng: 25.6022, timezone: "Africa/Johannesburg", type: "container_port" },
  { name: "Lagos (Apapa)", code: "NGAPP", unlocode: "NGAPP", country: "Nigeria", countryCode: "NG", region: "Africa", lat: 6.4541, lng: 3.3947, timezone: "Africa/Lagos", type: "container_port" },
  { name: "Lagos (Tin Can)", code: "NGTIN", unlocode: "NGTIN", country: "Nigeria", countryCode: "NG", region: "Africa", lat: 6.4400, lng: 3.3650, timezone: "Africa/Lagos", type: "container_port" },
  { name: "Tema", code: "GHTEM", unlocode: "GHTEM", country: "Ghana", countryCode: "GH", region: "Africa", lat: 5.6219, lng: -0.0094, timezone: "Africa/Accra", type: "container_port" },
  { name: "Abidjan", code: "CIABJ", unlocode: "CIABJ", country: "Ivory Coast", countryCode: "CI", region: "Africa", lat: 5.2592, lng: -3.9830, timezone: "Africa/Abidjan", type: "container_port" },
  { name: "Dakar", code: "SNDKR", unlocode: "SNDKR", country: "Senegal", countryCode: "SN", region: "Africa", lat: 14.6928, lng: -17.4467, timezone: "Africa/Dakar", type: "container_port" },
  { name: "Casablanca", code: "MACAS", unlocode: "MACAS", country: "Morocco", countryCode: "MA", region: "Africa", lat: 33.5731, lng: -7.5898, timezone: "Africa/Casablanca", type: "container_port" },
  { name: "Tanger Med", code: "MAPTM", unlocode: "MAPTM", country: "Morocco", countryCode: "MA", region: "Africa", lat: 35.8934, lng: -5.5051, timezone: "Africa/Casablanca", type: "container_port" },
  { name: "Alexandria", code: "EGALY", unlocode: "EGALY", country: "Egypt", countryCode: "EG", region: "Africa", lat: 31.2001, lng: 29.9187, timezone: "Africa/Cairo", type: "container_port" },
  { name: "Port Said", code: "EGPSD", unlocode: "EGPSD", country: "Egypt", countryCode: "EG", region: "Africa", lat: 31.2653, lng: 32.3019, timezone: "Africa/Cairo", type: "container_port" },
  { name: "Suez", code: "EGSUZ", unlocode: "EGSUZ", country: "Egypt", countryCode: "EG", region: "Africa", lat: 29.9668, lng: 32.5498, timezone: "Africa/Cairo", type: "container_port" },
  { name: "Djibouti", code: "DJJIB", unlocode: "DJJIB", country: "Djibouti", countryCode: "DJ", region: "Africa", lat: 11.5886, lng: 43.1456, timezone: "Africa/Djibouti", type: "container_port" },
  { name: "Mombasa", code: "KEMBA", unlocode: "KEMBA", country: "Kenya", countryCode: "KE", region: "Africa", lat: -4.0435, lng: 39.6682, timezone: "Africa/Nairobi", type: "container_port" },
  { name: "Dar es Salaam", code: "TZDAR", unlocode: "TZDAR", country: "Tanzania", countryCode: "TZ", region: "Africa", lat: -6.7924, lng: 39.2083, timezone: "Africa/Dar_es_Salaam", type: "container_port" },
  { name: "Port Louis", code: "MUPLU", unlocode: "MUPLU", country: "Mauritius", countryCode: "MU", region: "Africa", lat: -20.1619, lng: 57.4990, timezone: "Indian/Mauritius", type: "container_port" },
  { name: "Santos", code: "BRSSZ", unlocode: "BRSSZ", country: "Brazil", countryCode: "BR", region: "South America", lat: -23.9544, lng: -46.3340, timezone: "America/Sao_Paulo", type: "container_port" },
  { name: "Paranagua", code: "BRPNG", unlocode: "BRPNG", country: "Brazil", countryCode: "BR", region: "South America", lat: -25.5205, lng: -48.5095, timezone: "America/Sao_Paulo", type: "container_port" },
  { name: "Rio Grande", code: "BRRIG", unlocode: "BRRIG", country: "Brazil", countryCode: "BR", region: "South America", lat: -32.0349, lng: -52.0986, timezone: "America/Sao_Paulo", type: "container_port" },
  { name: "Itajai", code: "BRITJ", unlocode: "BRITJ", country: "Brazil", countryCode: "BR", region: "South America", lat: -26.9072, lng: -48.6608, timezone: "America/Sao_Paulo", type: "container_port" },
  { name: "Buenos Aires", code: "ARBUE", unlocode: "ARBUE", country: "Argentina", countryCode: "AR", region: "South America", lat: -34.6037, lng: -58.3816, timezone: "America/Argentina/Buenos_Aires", type: "container_port" },
  { name: "Callao", code: "PECLL", unlocode: "PECLL", country: "Peru", countryCode: "PE", region: "South America", lat: -12.0464, lng: -77.1189, timezone: "America/Lima", type: "container_port" },
  { name: "Cartagena", code: "COCTG", unlocode: "COCTG", country: "Colombia", countryCode: "CO", region: "South America", lat: 10.3910, lng: -75.5143, timezone: "America/Bogota", type: "container_port" },
  { name: "Buenaventura", code: "COBUN", unlocode: "COBUN", country: "Colombia", countryCode: "CO", region: "South America", lat: 3.8859, lng: -77.0704, timezone: "America/Bogota", type: "container_port" },
  { name: "Guayaquil", code: "ECGYE", unlocode: "ECGYE", country: "Ecuador", countryCode: "EC", region: "South America", lat: -2.2092, lng: -79.9078, timezone: "America/Guayaquil", type: "container_port" },
  { name: "Valparaiso", code: "CLVAP", unlocode: "CLVAP", country: "Chile", countryCode: "CL", region: "South America", lat: -33.0472, lng: -71.6127, timezone: "America/Santiago", type: "container_port" },
  { name: "San Antonio", code: "CLSAI", unlocode: "CLSAI", country: "Chile", countryCode: "CL", region: "South America", lat: -33.5860, lng: -71.6085, timezone: "America/Santiago", type: "container_port" },
  { name: "Montevideo", code: "UYMVD", unlocode: "UYMVD", country: "Uruguay", countryCode: "UY", region: "South America", lat: -34.9011, lng: -56.2073, timezone: "America/Montevideo", type: "container_port" },
  { name: "Vancouver", code: "CAVAN", unlocode: "CAVAN", country: "Canada", countryCode: "CA", region: "North America", lat: 49.2827, lng: -123.1207, timezone: "America/Vancouver", type: "container_port" },
  { name: "Prince Rupert", code: "CAPRR", unlocode: "CAPRR", country: "Canada", countryCode: "CA", region: "North America", lat: 54.3150, lng: -130.3208, timezone: "America/Vancouver", type: "container_port" },
  { name: "Montreal", code: "CAMTR", unlocode: "CAMTR", country: "Canada", countryCode: "CA", region: "North America", lat: 45.5017, lng: -73.5673, timezone: "America/Toronto", type: "container_port" },
  { name: "Halifax", code: "CAHAL", unlocode: "CAHAL", country: "Canada", countryCode: "CA", region: "North America", lat: 44.6488, lng: -63.5752, timezone: "America/Halifax", type: "container_port" },
  { name: "Seattle", code: "USSEA", unlocode: "USSEA", country: "United States", countryCode: "US", region: "North America", lat: 47.6062, lng: -122.3321, timezone: "America/Los_Angeles", type: "container_port" },
  { name: "Tacoma", code: "USTAC", unlocode: "USTAC", country: "United States", countryCode: "US", region: "North America", lat: 47.2529, lng: -122.4443, timezone: "America/Los_Angeles", type: "container_port" },
  { name: "Oakland", code: "USOAK", unlocode: "USOAK", country: "United States", countryCode: "US", region: "North America", lat: 37.8044, lng: -122.2712, timezone: "America/Los_Angeles", type: "container_port" },
  { name: "Charleston", code: "USCHS", unlocode: "USCHS", country: "United States", countryCode: "US", region: "North America", lat: 32.7765, lng: -79.9311, timezone: "America/New_York", type: "container_port" },
  { name: "Norfolk", code: "USORF", unlocode: "USORF", country: "United States", countryCode: "US", region: "North America", lat: 36.8508, lng: -76.2859, timezone: "America/New_York", type: "container_port" },
  { name: "Baltimore", code: "USBAL", unlocode: "USBAL", country: "United States", countryCode: "US", region: "North America", lat: 39.2904, lng: -76.6122, timezone: "America/New_York", type: "container_port" },
  { name: "Miami", code: "USMIA", unlocode: "USMIA", country: "United States", countryCode: "US", region: "North America", lat: 25.7617, lng: -80.1918, timezone: "America/New_York", type: "container_port" },
  { name: "Port Everglades", code: "USPEF", unlocode: "USPEF", country: "United States", countryCode: "US", region: "North America", lat: 26.0853, lng: -80.1227, timezone: "America/New_York", type: "container_port" },
  { name: "Tampa", code: "USTPA", unlocode: "USTPA", country: "United States", countryCode: "US", region: "North America", lat: 27.9506, lng: -82.4572, timezone: "America/New_York", type: "container_port" },
  { name: "New Orleans", code: "USMSY", unlocode: "USMSY", country: "United States", countryCode: "US", region: "North America", lat: 29.9511, lng: -90.0715, timezone: "America/Chicago", type: "container_port" },
  { name: "Mobile", code: "USMOB", unlocode: "USMOB", country: "United States", countryCode: "US", region: "North America", lat: 30.6954, lng: -88.0399, timezone: "America/Chicago", type: "container_port" },
  { name: "Corpus Christi", code: "USCCC", unlocode: "USCCC", country: "United States", countryCode: "US", region: "North America", lat: 27.8006, lng: -97.3964, timezone: "America/Chicago", type: "oil_terminal" },
  { name: "Freeport", code: "USFRE", unlocode: "USFRE", country: "United States", countryCode: "US", region: "North America", lat: 28.9541, lng: -95.3597, timezone: "America/Chicago", type: "oil_terminal" },
  { name: "Beaumont", code: "USBPT", unlocode: "USBPT", country: "United States", countryCode: "US", region: "North America", lat: 30.0802, lng: -94.1266, timezone: "America/Chicago", type: "oil_terminal" },
  { name: "Lake Charles", code: "USLCH", unlocode: "USLCH", country: "United States", countryCode: "US", region: "North America", lat: 30.2266, lng: -93.2174, timezone: "America/Chicago", type: "oil_terminal" },
  { name: "Sydney", code: "AUSYD", unlocode: "AUSYD", country: "Australia", countryCode: "AU", region: "Oceania", lat: -33.8688, lng: 151.2093, timezone: "Australia/Sydney", type: "container_port" },
  { name: "Melbourne", code: "AUMEL", unlocode: "AUMEL", country: "Australia", countryCode: "AU", region: "Oceania", lat: -37.8136, lng: 144.9631, timezone: "Australia/Melbourne", type: "container_port" },
  { name: "Brisbane", code: "AUBNE", unlocode: "AUBNE", country: "Australia", countryCode: "AU", region: "Oceania", lat: -27.4698, lng: 153.0251, timezone: "Australia/Brisbane", type: "container_port" },
  { name: "Fremantle", code: "AUFRE", unlocode: "AUFRE", country: "Australia", countryCode: "AU", region: "Oceania", lat: -32.0569, lng: 115.7439, timezone: "Australia/Perth", type: "container_port" },
  { name: "Adelaide", code: "AUADL", unlocode: "AUADL", country: "Australia", countryCode: "AU", region: "Oceania", lat: -34.9285, lng: 138.6007, timezone: "Australia/Adelaide", type: "container_port" },
  { name: "Port Hedland", code: "AUPHE", unlocode: "AUPHE", country: "Australia", countryCode: "AU", region: "Oceania", lat: -20.3106, lng: 118.5753, timezone: "Australia/Perth", type: "dry_bulk" },
  { name: "Dampier", code: "AUDAM", unlocode: "AUDAM", country: "Australia", countryCode: "AU", region: "Oceania", lat: -20.6622, lng: 116.7074, timezone: "Australia/Perth", type: "dry_bulk" },
  { name: "Hay Point", code: "AUHPT", unlocode: "AUHPT", country: "Australia", countryCode: "AU", region: "Oceania", lat: -21.2756, lng: 149.2944, timezone: "Australia/Brisbane", type: "dry_bulk" },
  { name: "Newcastle", code: "AUNTL", unlocode: "AUNTL", country: "Australia", countryCode: "AU", region: "Oceania", lat: -32.9283, lng: 151.7817, timezone: "Australia/Sydney", type: "dry_bulk" },
  { name: "Auckland", code: "NZAKL", unlocode: "NZAKL", country: "New Zealand", countryCode: "NZ", region: "Oceania", lat: -36.8485, lng: 174.7633, timezone: "Pacific/Auckland", type: "container_port" },
  { name: "Tauranga", code: "NZTRG", unlocode: "NZTRG", country: "New Zealand", countryCode: "NZ", region: "Oceania", lat: -37.6878, lng: 176.1651, timezone: "Pacific/Auckland", type: "container_port" },
  { name: "Lyttelton", code: "NZLYT", unlocode: "NZLYT", country: "New Zealand", countryCode: "NZ", region: "Oceania", lat: -43.6030, lng: 172.7248, timezone: "Pacific/Auckland", type: "container_port" },
  { name: "Gothenburg", code: "SEGOT", unlocode: "SEGOT", country: "Sweden", countryCode: "SE", region: "Europe", lat: 57.7089, lng: 11.9746, timezone: "Europe/Stockholm", type: "container_port" },
  { name: "Copenhagen", code: "DKCPH", unlocode: "DKCPH", country: "Denmark", countryCode: "DK", region: "Europe", lat: 55.6761, lng: 12.5683, timezone: "Europe/Copenhagen", type: "container_port" },
  { name: "Oslo", code: "NOOSL", unlocode: "NOOSL", country: "Norway", countryCode: "NO", region: "Europe", lat: 59.9139, lng: 10.7522, timezone: "Europe/Oslo", type: "container_port" },
  { name: "Helsinki", code: "FIHEL", unlocode: "FIHEL", country: "Finland", countryCode: "FI", region: "Europe", lat: 60.1699, lng: 24.9384, timezone: "Europe/Helsinki", type: "container_port" },
  { name: "Amsterdam", code: "NLAMS", unlocode: "NLAMS", country: "Netherlands", countryCode: "NL", region: "Europe", lat: 52.3702, lng: 4.8952, timezone: "Europe/Amsterdam", type: "container_port" },
  { name: "Europoort", code: "NLEUR", unlocode: "NLEUR", country: "Netherlands", countryCode: "NL", region: "Europe", lat: 51.9516, lng: 4.1231, timezone: "Europe/Amsterdam", type: "oil_terminal" },
  { name: "Zeebrugge", code: "BEZEE", unlocode: "BEZEE", country: "Belgium", countryCode: "BE", region: "Europe", lat: 51.3331, lng: 3.2038, timezone: "Europe/Brussels", type: "container_port" },
  { name: "Dunkirk", code: "FRDKK", unlocode: "FRDKK", country: "France", countryCode: "FR", region: "Europe", lat: 51.0343, lng: 2.3768, timezone: "Europe/Paris", type: "container_port" },
  { name: "Bilbao", code: "ESBIO", unlocode: "ESBIO", country: "Spain", countryCode: "ES", region: "Europe", lat: 43.2630, lng: -2.9350, timezone: "Europe/Madrid", type: "container_port" },
  { name: "Lisbon", code: "PTLIS", unlocode: "PTLIS", country: "Portugal", countryCode: "PT", region: "Europe", lat: 38.7223, lng: -9.1393, timezone: "Europe/Lisbon", type: "container_port" },
  { name: "Sines", code: "PTSIE", unlocode: "PTSIE", country: "Portugal", countryCode: "PT", region: "Europe", lat: 37.9561, lng: -8.8639, timezone: "Europe/Lisbon", type: "container_port" },
  { name: "Leixoes", code: "PTLEI", unlocode: "PTLEI", country: "Portugal", countryCode: "PT", region: "Europe", lat: 41.1879, lng: -8.7056, timezone: "Europe/Lisbon", type: "container_port" },
  { name: "Thessaloniki", code: "GRSKG", unlocode: "GRSKG", country: "Greece", countryCode: "GR", region: "Europe", lat: 40.6401, lng: 22.9444, timezone: "Europe/Athens", type: "container_port" },
  { name: "Constanta", code: "ROCND", unlocode: "ROCND", country: "Romania", countryCode: "RO", region: "Europe", lat: 44.1598, lng: 28.6348, timezone: "Europe/Bucharest", type: "container_port" },
  { name: "Odessa", code: "UAODS", unlocode: "UAODS", country: "Ukraine", countryCode: "UA", region: "Europe", lat: 46.4825, lng: 30.7233, timezone: "Europe/Kiev", type: "container_port" },
  { name: "Novorossiysk", code: "RUNVS", unlocode: "RUNVS", country: "Russia", countryCode: "RU", region: "Europe", lat: 44.7239, lng: 37.7684, timezone: "Europe/Moscow", type: "oil_terminal" },
  { name: "Istanbul", code: "TRIST", unlocode: "TRIST", country: "Turkey", countryCode: "TR", region: "Europe", lat: 41.0082, lng: 28.9784, timezone: "Europe/Istanbul", type: "container_port" },
  { name: "Izmir", code: "TRIZM", unlocode: "TRIZM", country: "Turkey", countryCode: "TR", region: "Europe", lat: 38.4237, lng: 27.1428, timezone: "Europe/Istanbul", type: "container_port" },
  { name: "Mersin", code: "TRMER", unlocode: "TRMER", country: "Turkey", countryCode: "TR", region: "Europe", lat: 36.7992, lng: 34.6319, timezone: "Europe/Istanbul", type: "container_port" },
  { name: "Iskenderun", code: "TRISK", unlocode: "TRISK", country: "Turkey", countryCode: "TR", region: "Europe", lat: 36.5880, lng: 36.1690, timezone: "Europe/Istanbul", type: "container_port" },
  { name: "Haifa", code: "ILHFA", unlocode: "ILHFA", country: "Israel", countryCode: "IL", region: "Middle East", lat: 32.7940, lng: 34.9896, timezone: "Asia/Jerusalem", type: "container_port" },
  { name: "Ashdod", code: "ILASH", unlocode: "ILASH", country: "Israel", countryCode: "IL", region: "Middle East", lat: 31.7934, lng: 34.6413, timezone: "Asia/Jerusalem", type: "container_port" },
  { name: "Beirut", code: "LBBEY", unlocode: "LBBEY", country: "Lebanon", countryCode: "LB", region: "Middle East", lat: 33.8938, lng: 35.5018, timezone: "Asia/Beirut", type: "container_port" },
  { name: "Latakia", code: "SYLTK", unlocode: "SYLTK", country: "Syria", countryCode: "SY", region: "Middle East", lat: 35.5196, lng: 35.7814, timezone: "Asia/Damascus", type: "container_port" },
  { name: "Dalian", code: "CNDLC", unlocode: "CNDLC", country: "China", countryCode: "CN", region: "Asia", lat: 38.9140, lng: 121.6147, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Yingkou", code: "CNYIK", unlocode: "CNYIK", country: "China", countryCode: "CN", region: "Asia", lat: 40.6649, lng: 122.2281, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Lianyungang", code: "CNLYG", unlocode: "CNLYG", country: "China", countryCode: "CN", region: "Asia", lat: 34.5971, lng: 119.2215, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Rizhao", code: "CNRZH", unlocode: "CNRZH", country: "China", countryCode: "CN", region: "Asia", lat: 35.3820, lng: 119.5264, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Xiamen", code: "CNXMN", unlocode: "CNXMN", country: "China", countryCode: "CN", region: "Asia", lat: 24.4798, lng: 118.0894, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Fuzhou", code: "CNFOC", unlocode: "CNFOC", country: "China", countryCode: "CN", region: "Asia", lat: 26.0742, lng: 119.2965, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Zhuhai", code: "CNZUH", unlocode: "CNZUH", country: "China", countryCode: "CN", region: "Asia", lat: 22.2769, lng: 113.5678, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Dongguan", code: "CNDGG", unlocode: "CNDGG", country: "China", countryCode: "CN", region: "Asia", lat: 23.0489, lng: 113.7447, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Nanjing", code: "CNNKG", unlocode: "CNNKG", country: "China", countryCode: "CN", region: "Asia", lat: 32.0603, lng: 118.7969, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Wuhan", code: "CNWUH", unlocode: "CNWUH", country: "China", countryCode: "CN", region: "Asia", lat: 30.5928, lng: 114.3055, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Chongqing", code: "CNCKG", unlocode: "CNCKG", country: "China", countryCode: "CN", region: "Asia", lat: 29.4316, lng: 106.9123, timezone: "Asia/Shanghai", type: "container_port" },
  { name: "Taichung", code: "TWTXG", unlocode: "TWTXG", country: "Taiwan", countryCode: "TW", region: "Asia", lat: 24.1477, lng: 120.6736, timezone: "Asia/Taipei", type: "container_port" },
  { name: "Keelung", code: "TWKEL", unlocode: "TWKEL", country: "Taiwan", countryCode: "TW", region: "Asia", lat: 25.1276, lng: 121.7392, timezone: "Asia/Taipei", type: "container_port" },
  { name: "Incheon", code: "KRICH", unlocode: "KRICH", country: "South Korea", countryCode: "KR", region: "Asia", lat: 37.4563, lng: 126.7052, timezone: "Asia/Seoul", type: "container_port" },
  { name: "Ulsan", code: "KRULN", unlocode: "KRULN", country: "South Korea", countryCode: "KR", region: "Asia", lat: 35.5384, lng: 129.3114, timezone: "Asia/Seoul", type: "oil_terminal" },
  { name: "Gwangyang", code: "KRKWG", unlocode: "KRKWG", country: "South Korea", countryCode: "KR", region: "Asia", lat: 34.9232, lng: 127.6951, timezone: "Asia/Seoul", type: "container_port" },
  { name: "Pyeongtaek", code: "KRPTK", unlocode: "KRPTK", country: "South Korea", countryCode: "KR", region: "Asia", lat: 36.9921, lng: 126.8319, timezone: "Asia/Seoul", type: "container_port" },
  { name: "Hiroshima", code: "JPHIJ", unlocode: "JPHIJ", country: "Japan", countryCode: "JP", region: "Asia", lat: 34.3853, lng: 132.4553, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Kitakyushu", code: "JPKKJ", unlocode: "JPKKJ", country: "Japan", countryCode: "JP", region: "Asia", lat: 33.8834, lng: 130.8752, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Hakata", code: "JPHKT", unlocode: "JPHKT", country: "Japan", countryCode: "JP", region: "Asia", lat: 33.6064, lng: 130.4180, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Shimizu", code: "JPSMZ", unlocode: "JPSMZ", country: "Japan", countryCode: "JP", region: "Asia", lat: 35.0167, lng: 138.5000, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Niigata", code: "JPNII", unlocode: "JPNII", country: "Japan", countryCode: "JP", region: "Asia", lat: 37.9161, lng: 139.0364, timezone: "Asia/Tokyo", type: "container_port" },
  { name: "Vladivostok", code: "RUVVO", unlocode: "RUVVO", country: "Russia", countryCode: "RU", region: "Asia", lat: 43.1332, lng: 131.9113, timezone: "Asia/Vladivostok", type: "container_port" },
  { name: "Nakhodka", code: "RUNAH", unlocode: "RUNAH", country: "Russia", countryCode: "RU", region: "Asia", lat: 42.8139, lng: 132.8735, timezone: "Asia/Vladivostok", type: "oil_terminal" },
  { name: "Sihanoukville", code: "KHPSP", unlocode: "KHPSP", country: "Cambodia", countryCode: "KH", region: "Asia", lat: 10.6334, lng: 103.5000, timezone: "Asia/Phnom_Penh", type: "container_port" },
  { name: "Yangon", code: "MMRGN", unlocode: "MMRGN", country: "Myanmar", countryCode: "MM", region: "Asia", lat: 16.7967, lng: 96.1603, timezone: "Asia/Yangon", type: "container_port" },
  { name: "Belawan", code: "IDBLW", unlocode: "IDBLW", country: "Indonesia", countryCode: "ID", region: "Asia", lat: 3.7849, lng: 98.7041, timezone: "Asia/Jakarta", type: "container_port" },
  { name: "Surabaya", code: "IDSUB", unlocode: "IDSUB", country: "Indonesia", countryCode: "ID", region: "Asia", lat: -7.2504, lng: 112.7688, timezone: "Asia/Jakarta", type: "container_port" },
  { name: "Makassar", code: "IDMAK", unlocode: "IDMAK", country: "Indonesia", countryCode: "ID", region: "Asia", lat: -5.1477, lng: 119.4327, timezone: "Asia/Makassar", type: "container_port" },
  { name: "Semarang", code: "IDSRG", unlocode: "IDSRG", country: "Indonesia", countryCode: "ID", region: "Asia", lat: -6.9666, lng: 110.4196, timezone: "Asia/Jakarta", type: "container_port" },
  { name: "Balikpapan", code: "IDBPN", unlocode: "IDBPN", country: "Indonesia", countryCode: "ID", region: "Asia", lat: -1.2379, lng: 116.8529, timezone: "Asia/Makassar", type: "oil_terminal" },
  { name: "Bontang", code: "IDBXT", unlocode: "IDBXT", country: "Indonesia", countryCode: "ID", region: "Asia", lat: 0.1378, lng: 117.4754, timezone: "Asia/Makassar", type: "lng_terminal" },
  { name: "Cebu", code: "PHCEB", unlocode: "PHCEB", country: "Philippines", countryCode: "PH", region: "Asia", lat: 10.3157, lng: 123.8854, timezone: "Asia/Manila", type: "container_port" },
  { name: "Subic Bay", code: "PHSFS", unlocode: "PHSFS", country: "Philippines", countryCode: "PH", region: "Asia", lat: 14.7943, lng: 120.2867, timezone: "Asia/Manila", type: "container_port" },
  { name: "Batangas", code: "PHBTG", unlocode: "PHBTG", country: "Philippines", countryCode: "PH", region: "Asia", lat: 13.7567, lng: 121.0583, timezone: "Asia/Manila", type: "container_port" },
  { name: "Davao", code: "PHDVO", unlocode: "PHDVO", country: "Philippines", countryCode: "PH", region: "Asia", lat: 7.0731, lng: 125.6128, timezone: "Asia/Manila", type: "container_port" },
  { name: "Cai Mep", code: "VNCMT", unlocode: "VNCMT", country: "Vietnam", countryCode: "VN", region: "Asia", lat: 10.4907, lng: 107.0182, timezone: "Asia/Ho_Chi_Minh", type: "container_port" },
  { name: "Da Nang", code: "VNDAD", unlocode: "VNDAD", country: "Vietnam", countryCode: "VN", region: "Asia", lat: 16.0471, lng: 108.2068, timezone: "Asia/Ho_Chi_Minh", type: "container_port" },
  { name: "Johor", code: "MYJHB", unlocode: "MYJHB", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 1.4854, lng: 103.7618, timezone: "Asia/Kuala_Lumpur", type: "container_port" },
  { name: "Penang", code: "MYPEN", unlocode: "MYPEN", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 5.4141, lng: 100.3288, timezone: "Asia/Kuala_Lumpur", type: "container_port" },
  { name: "Kuantan", code: "MYKUA", unlocode: "MYKUA", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 3.8077, lng: 103.3260, timezone: "Asia/Kuala_Lumpur", type: "container_port" },
  { name: "Bintulu", code: "MYBTU", unlocode: "MYBTU", country: "Malaysia", countryCode: "MY", region: "Asia", lat: 3.1673, lng: 113.0413, timezone: "Asia/Kuching", type: "lng_terminal" },
  { name: "Brunei", code: "BNMUA", unlocode: "BNMUA", country: "Brunei", countryCode: "BN", region: "Asia", lat: 4.9431, lng: 114.9425, timezone: "Asia/Brunei", type: "oil_terminal" },
  { name: "Freetown", code: "SLFNA", unlocode: "SLFNA", country: "Sierra Leone", countryCode: "SL", region: "Africa", lat: 8.4657, lng: -13.2317, timezone: "Africa/Freetown", type: "container_port" },
  { name: "Conakry", code: "GNCKY", unlocode: "GNCKY", country: "Guinea", countryCode: "GN", region: "Africa", lat: 9.5092, lng: -13.7122, timezone: "Africa/Conakry", type: "container_port" },
  { name: "Monrovia", code: "LRMLW", unlocode: "LRMLW", country: "Liberia", countryCode: "LR", region: "Africa", lat: 6.3106, lng: -10.8047, timezone: "Africa/Monrovia", type: "container_port" },
  { name: "Lome", code: "TGLFW", unlocode: "TGLFW", country: "Togo", countryCode: "TG", region: "Africa", lat: 6.1375, lng: 1.2123, timezone: "Africa/Lome", type: "container_port" },
  { name: "Cotonou", code: "BJCOO", unlocode: "BJCOO", country: "Benin", countryCode: "BJ", region: "Africa", lat: 6.3703, lng: 2.4254, timezone: "Africa/Porto-Novo", type: "container_port" },
  { name: "Douala", code: "CMDLA", unlocode: "CMDLA", country: "Cameroon", countryCode: "CM", region: "Africa", lat: 4.0483, lng: 9.7043, timezone: "Africa/Douala", type: "container_port" },
  { name: "Libreville", code: "GALBV", unlocode: "GALBV", country: "Gabon", countryCode: "GA", region: "Africa", lat: 0.4162, lng: 9.4673, timezone: "Africa/Libreville", type: "oil_terminal" },
  { name: "Pointe-Noire", code: "CGPNR", unlocode: "CGPNR", country: "Congo", countryCode: "CG", region: "Africa", lat: -4.7692, lng: 11.8664, timezone: "Africa/Brazzaville", type: "oil_terminal" },
  { name: "Luanda", code: "AOLAD", unlocode: "AOLAD", country: "Angola", countryCode: "AO", region: "Africa", lat: -8.8383, lng: 13.2344, timezone: "Africa/Luanda", type: "oil_terminal" },
  { name: "Walvis Bay", code: "NAWVB", unlocode: "NAWVB", country: "Namibia", countryCode: "NA", region: "Africa", lat: -22.9575, lng: 14.5053, timezone: "Africa/Windhoek", type: "container_port" },
  { name: "Maputo", code: "MZMPM", unlocode: "MZMPM", country: "Mozambique", countryCode: "MZ", region: "Africa", lat: -25.9692, lng: 32.5732, timezone: "Africa/Maputo", type: "container_port" },
  { name: "Beira", code: "MZBEW", unlocode: "MZBEW", country: "Mozambique", countryCode: "MZ", region: "Africa", lat: -19.8436, lng: 34.8389, timezone: "Africa/Maputo", type: "container_port" },
  { name: "Nacala", code: "MZMNC", unlocode: "MZMNC", country: "Mozambique", countryCode: "MZ", region: "Africa", lat: -14.5639, lng: 40.6781, timezone: "Africa/Maputo", type: "container_port" },
  { name: "Toamasina", code: "MGTMM", unlocode: "MGTMM", country: "Madagascar", countryCode: "MG", region: "Africa", lat: -18.1443, lng: 49.3958, timezone: "Indian/Antananarivo", type: "container_port" },
  { name: "Port Sudan", code: "SDPZU", unlocode: "SDPZU", country: "Sudan", countryCode: "SD", region: "Africa", lat: 19.6158, lng: 37.2164, timezone: "Africa/Khartoum", type: "container_port" },
  { name: "Berbera", code: "SOBBO", unlocode: "SOBBO", country: "Somalia", countryCode: "SO", region: "Africa", lat: 10.4394, lng: 45.0296, timezone: "Africa/Mogadishu", type: "container_port" },
  { name: "Massawa", code: "ERMSW", unlocode: "ERMSW", country: "Eritrea", countryCode: "ER", region: "Africa", lat: 15.6072, lng: 39.4748, timezone: "Africa/Asmara", type: "container_port" },
  { name: "Manzanillo (Mexico)", code: "MXZLO", unlocode: "MXZLO", country: "Mexico", countryCode: "MX", region: "North America", lat: 19.0531, lng: -104.3186, timezone: "America/Mexico_City", type: "container_port" },
  { name: "Lazaro Cardenas", code: "MXLZC", unlocode: "MXLZC", country: "Mexico", countryCode: "MX", region: "North America", lat: 17.9569, lng: -102.1996, timezone: "America/Mexico_City", type: "container_port" },
  { name: "Veracruz", code: "MXVER", unlocode: "MXVER", country: "Mexico", countryCode: "MX", region: "North America", lat: 19.1738, lng: -96.1342, timezone: "America/Mexico_City", type: "container_port" },
  { name: "Altamira", code: "MXATM", unlocode: "MXATM", country: "Mexico", countryCode: "MX", region: "North America", lat: 22.4044, lng: -97.9431, timezone: "America/Mexico_City", type: "container_port" },
  { name: "Colon", code: "PACFZ", unlocode: "PACFZ", country: "Panama", countryCode: "PA", region: "Central America", lat: 9.3547, lng: -79.9008, timezone: "America/Panama", type: "container_port" },
  { name: "Balboa", code: "PABLB", unlocode: "PABLB", country: "Panama", countryCode: "PA", region: "Central America", lat: 8.9562, lng: -79.5668, timezone: "America/Panama", type: "container_port" },
  { name: "Limon", code: "CRLIO", unlocode: "CRLIO", country: "Costa Rica", countryCode: "CR", region: "Central America", lat: 9.9907, lng: -83.0359, timezone: "America/Costa_Rica", type: "container_port" },
  { name: "Puerto Cortes", code: "HNPCR", unlocode: "HNPCR", country: "Honduras", countryCode: "HN", region: "Central America", lat: 15.8333, lng: -87.9500, timezone: "America/Tegucigalpa", type: "container_port" },
  { name: "Santo Tomas de Castilla", code: "GTSTC", unlocode: "GTSTC", country: "Guatemala", countryCode: "GT", region: "Central America", lat: 15.6994, lng: -88.6170, timezone: "America/Guatemala", type: "container_port" },
  { name: "Kingston (Jamaica)", code: "JMKIN", unlocode: "JMKIN", country: "Jamaica", countryCode: "JM", region: "Caribbean", lat: 17.9714, lng: -76.7936, timezone: "America/Jamaica", type: "container_port" },
  { name: "Freeport (Bahamas)", code: "BSFPO", unlocode: "BSFPO", country: "Bahamas", countryCode: "BS", region: "Caribbean", lat: 26.5333, lng: -78.7000, timezone: "America/Nassau", type: "container_port" },
  { name: "San Juan", code: "PRSJU", unlocode: "PRSJU", country: "Puerto Rico", countryCode: "PR", region: "Caribbean", lat: 18.4655, lng: -66.1057, timezone: "America/Puerto_Rico", type: "container_port" },
  { name: "Caucedo", code: "DOCAU", unlocode: "DOCAU", country: "Dominican Republic", countryCode: "DO", region: "Caribbean", lat: 18.4308, lng: -69.6269, timezone: "America/Santo_Domingo", type: "container_port" },
  { name: "Havana", code: "CUHAV", unlocode: "CUHAV", country: "Cuba", countryCode: "CU", region: "Caribbean", lat: 23.1136, lng: -82.3666, timezone: "America/Havana", type: "container_port" },
  { name: "Mariel", code: "CUMAR", unlocode: "CUMAR", country: "Cuba", countryCode: "CU", region: "Caribbean", lat: 22.9961, lng: -82.7503, timezone: "America/Havana", type: "container_port" },
  { name: "Point Lisas", code: "TTPLS", unlocode: "TTPLS", country: "Trinidad & Tobago", countryCode: "TT", region: "Caribbean", lat: 10.4197, lng: -61.4772, timezone: "America/Port_of_Spain", type: "container_port" },
  { name: "Willemstad", code: "CWWIL", unlocode: "CWWIL", country: "Curacao", countryCode: "CW", region: "Caribbean", lat: 12.1091, lng: -68.9299, timezone: "America/Curacao", type: "container_port" },
  { name: "Oranjestad", code: "AWORA", unlocode: "AWORA", country: "Aruba", countryCode: "AW", region: "Caribbean", lat: 12.5092, lng: -70.0086, timezone: "America/Aruba", type: "container_port" },
  { name: "Suva", code: "FJSUV", unlocode: "FJSUV", country: "Fiji", countryCode: "FJ", region: "Oceania", lat: -18.1416, lng: 178.4419, timezone: "Pacific/Fiji", type: "container_port" },
  { name: "Noumea", code: "NCNOU", unlocode: "NCNOU", country: "New Caledonia", countryCode: "NC", region: "Oceania", lat: -22.2735, lng: 166.4580, timezone: "Pacific/Noumea", type: "container_port" },
  { name: "Guam", code: "GUPGU", unlocode: "GUPGU", country: "Guam", countryCode: "GU", region: "Oceania", lat: 13.4443, lng: 144.7937, timezone: "Pacific/Guam", type: "container_port" },
  { name: "Honolulu", code: "USHNL", unlocode: "USHNL", country: "United States", countryCode: "US", region: "Oceania", lat: 21.3069, lng: -157.8583, timezone: "Pacific/Honolulu", type: "container_port" },
  { name: "Papeete", code: "PFPPT", unlocode: "PFPPT", country: "French Polynesia", countryCode: "PF", region: "Oceania", lat: -17.5516, lng: -149.5585, timezone: "Pacific/Tahiti", type: "container_port" },
  { name: "Apia", code: "WSAPW", unlocode: "WSAPW", country: "Samoa", countryCode: "WS", region: "Oceania", lat: -13.8333, lng: -171.7500, timezone: "Pacific/Apia", type: "container_port" },
  { name: "Port Moresby", code: "PGPOM", unlocode: "PGPOM", country: "Papua New Guinea", countryCode: "PG", region: "Oceania", lat: -9.4438, lng: 147.1803, timezone: "Pacific/Port_Moresby", type: "container_port" },
  { name: "Honiara", code: "SBHIR", unlocode: "SBHIR", country: "Solomon Islands", countryCode: "SB", region: "Oceania", lat: -9.4280, lng: 159.9497, timezone: "Pacific/Guadalcanal", type: "container_port" },
  { name: "Port Vila", code: "VUVLI", unlocode: "VUVLI", country: "Vanuatu", countryCode: "VU", region: "Oceania", lat: -17.7333, lng: 168.3167, timezone: "Pacific/Efate", type: "container_port" },
  { name: "Richards Bay", code: "ZARCB", unlocode: "ZARCB", country: "South Africa", countryCode: "ZA", region: "Africa", lat: -28.8006, lng: 32.0383, timezone: "Africa/Johannesburg", type: "dry_bulk" },
  { name: "Saldanha Bay", code: "ZASDB", unlocode: "ZASDB", country: "South Africa", countryCode: "ZA", region: "Africa", lat: -33.0206, lng: 17.9489, timezone: "Africa/Johannesburg", type: "dry_bulk" },
  { name: "Gladstone", code: "AUGLT", unlocode: "AUGLT", country: "Australia", countryCode: "AU", region: "Oceania", lat: -23.8427, lng: 151.2553, timezone: "Australia/Brisbane", type: "dry_bulk" },
  { name: "Tubarao", code: "BRTUB", unlocode: "BRTUB", country: "Brazil", countryCode: "BR", region: "South America", lat: -20.2769, lng: -40.2867, timezone: "America/Sao_Paulo", type: "dry_bulk" },
  { name: "Itaqui", code: "BRITQ", unlocode: "BRITQ", country: "Brazil", countryCode: "BR", region: "South America", lat: -2.5675, lng: -44.3600, timezone: "America/Sao_Paulo", type: "dry_bulk" },
  { name: "Sept-Iles", code: "CASET", unlocode: "CASET", country: "Canada", countryCode: "CA", region: "North America", lat: 50.2167, lng: -66.3833, timezone: "America/Toronto", type: "dry_bulk" },
  { name: "Thunder Bay", code: "CATHB", unlocode: "CATHB", country: "Canada", countryCode: "CA", region: "North America", lat: 48.3809, lng: -89.2477, timezone: "America/Toronto", type: "dry_bulk" },
  { name: "Duluth", code: "USDLH", unlocode: "USDLH", country: "United States", countryCode: "US", region: "North America", lat: 46.7867, lng: -92.1005, timezone: "America/Chicago", type: "dry_bulk" },
  { name: "Two Harbors", code: "USTWH", unlocode: "USTWH", country: "United States", countryCode: "US", region: "North America", lat: 47.0236, lng: -91.6719, timezone: "America/Chicago", type: "dry_bulk" },
  { name: "Praia Mole", code: "BRPMW", unlocode: "BRPMW", country: "Brazil", countryCode: "BR", region: "South America", lat: -20.2936, lng: -40.2489, timezone: "America/Sao_Paulo", type: "dry_bulk" },
  { name: "Qinhuangdao", code: "CNQHD", unlocode: "CNQHD", country: "China", countryCode: "CN", region: "Asia", lat: 39.9354, lng: 119.6011, timezone: "Asia/Shanghai", type: "dry_bulk" },
  { name: "Tangshan", code: "CNTSN", unlocode: "CNTSN", country: "China", countryCode: "CN", region: "Asia", lat: 39.1308, lng: 118.1800, timezone: "Asia/Shanghai", type: "dry_bulk" }
];

export async function seedGlobalPorts(): Promise<void> {
  console.log(`Seeding ${globalPorts.length} global ports...`);
  
  const existingPorts = await db.select({ code: ports.code }).from(ports);
  const existingCodes = new Set(existingPorts.map(p => p.code));
  
  let inserted = 0;
  let skipped = 0;
  
  for (const port of globalPorts) {
    if (existingCodes.has(port.code)) {
      skipped++;
      continue;
    }
    
    try {
      await db.insert(ports).values({
        name: port.name,
        code: port.code,
        unlocode: port.unlocode,
        country: port.country,
        countryCode: port.countryCode,
        region: port.region,
        latitude: port.lat.toString(),
        longitude: port.lng.toString(),
        timezone: port.timezone,
        type: port.type,
        geofenceRadiusKm: "3.0",
        operationalStatus: "active"
      });
      inserted++;
    } catch (err) {
      console.error(`Failed to insert port ${port.code}:`, err);
    }
  }
  
  console.log(`Ports seeded: ${inserted} inserted, ${skipped} skipped (already exist)`);
}

export async function getPortCount(): Promise<number> {
  const result = await db.select({ count: sql<number>`count(*)` }).from(ports);
  return Number(result[0]?.count || 0);
}

// Port call seeding for arrivals/departures/dwell time statistics
export async function seedPortCalls(): Promise<void> {
  console.log('Seeding port call data...');
  
  // Get vessels and core ports (both 3-char and 5-char codes)
  const allVessels = await db.select().from(vessels);
  const corePorts = await db.select().from(ports).where(
    sql`code IN ('FJR', 'RTM', 'SIN', 'NLRTM', 'SGSIN', 'AEFJR')`
  );
  
  if (allVessels.length === 0 || corePorts.length === 0) {
    console.log('No vessels or core ports found, skipping port call seeding');
    return;
  }
  
  // Check if port calls already exist
  const existingCalls = await db.select({ count: sql<number>`count(*)` }).from(portCalls);
  const existingCount = Number(existingCalls[0]?.count || 0);
  
  if (existingCount > 0) {
    console.log(`Port calls already exist (${existingCount}), skipping seeding`);
    return;
  }
  
  const now = new Date();
  let inserted = 0;
  
  // Generate port calls for the last 7 days
  for (const vessel of allVessels) {
    // Each vessel makes 2-4 port calls over the last 7 days
    const numCalls = 2 + Math.floor(Math.random() * 3);
    
    for (let i = 0; i < numCalls; i++) {
      const port = corePorts[Math.floor(Math.random() * corePorts.length)];
      
      // Random time within the last 7 days
      const daysAgo = Math.random() * 7;
      const arrivalTime = new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000);
      
      // Dwell time between 4-72 hours
      const dwellHours = 4 + Math.random() * 68;
      const departureTime = new Date(arrivalTime.getTime() + dwellHours * 60 * 60 * 1000);
      
      // If departure is in the future, mark as still in port
      const isCompleted = departureTime < now;
      
      try {
        await db.insert(portCalls).values({
          vesselId: vessel.id,
          portId: port.id,
          callType: 'arrival',
          status: isCompleted ? 'completed' : 'in_port',
          arrivalTime: arrivalTime,
          departureTime: isCompleted ? departureTime : null,
          waitTimeHours: (Math.random() * 4).toFixed(2),
          berthTimeHours: isCompleted ? dwellHours.toFixed(2) : null,
          purpose: ['loading', 'discharging', 'bunkering', 'crew_change'][Math.floor(Math.random() * 4)]
        });
        inserted++;
      } catch (err) {
        console.error(`Failed to insert port call:`, err);
      }
    }
  }
  
  console.log(`Port calls seeded: ${inserted} records created`);
}

export async function getPortCallCount(): Promise<number> {
  const result = await db.select({ count: sql<number>`count(*)` }).from(portCalls);
  return Number(result[0]?.count || 0);
}
