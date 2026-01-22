import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useQuery } from "@tanstack/react-query";
import { Plane, Menu, Settings, Triangle } from "lucide-react";
import { Link } from "wouter";

export default function FlightScope() {
  // Mock flight data
  const flightData = [
    { flight: 'Airlstar', airline: 'Airline', departure: 'FR / LH', arrival: 'ATH', status: 'En Route' },
    { flight: 'Airstrane', airline: 'Airstar', departure: 'FST / AA', arrival: 'AAO', status: 'En Route' },
    { flight: 'Bodnioritori', airline: 'Airlines', departure: 'ATH / RO', arrival: 'EYU', status: 'En Route' }
  ];

  const topAirlines = [
    { name: 'Emirates', routes: 245 },
    { name: 'Qatar Airways', routes: 189 },
    { name: 'Lufthansa', routes: 156 },
    { name: 'British Airways', routes: 134 }
  ];

  const flightsByRegion = {
    'North America': 35,
    'Europe': 28,
    'Asia': 25,
    'Other': 12
  };

  return (
    <div className="min-h-screen bg-[#0A0B1E] text-white">
      {/* Header */}
      <header className="flex items-center justify-between p-6 border-b border-gray-800">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-menu">
            <Menu className="w-5 h-5" />
          </Button>
          <h1 className="text-2xl font-bold">FLIGHT TRACKING</h1>
        </div>
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-alerts">
            <Triangle className="w-5 h-5" />
          </Button>
        </div>
      </header>

      <div className="grid grid-cols-12 gap-6 p-6 h-[calc(100vh-88px)]">
        {/* Left Sidebar */}
        <div className="col-span-1 space-y-6">
          <nav className="space-y-4">
            <Link href="/dashboard/flightscope">
              <Button variant="ghost" size="icon" className="w-full text-blue-400 bg-blue-400/10" data-testid="button-nav-plane">
                <Plane className="w-5 h-5" />
              </Button>
            </Link>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-warning">
              ⚠️
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-chart">
              📊
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-arrow">
              ↗️
            </Button>
          </nav>
          <div className="mt-auto">
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-settings">
              <Settings className="w-5 h-5" />
            </Button>
          </div>
        </div>

        {/* Main Map Area */}
        <div className="col-span-7 relative">
          <Card className="h-full bg-gray-900/50 border-gray-700">
            <CardContent className="p-0 h-full relative overflow-hidden">
              {/* World Map Background */}
              <div className="absolute inset-0 bg-gradient-to-br from-blue-900/20 to-blue-800/10">
                <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KICA8cGF0aCBkPSJNMTAgMTBMMjAgMjBNMjAgMTBMMTAgMjAiIHN0cm9rZT0iIzMzMzMzMyIgc3Ryb2tlLXdpZHRoPSIwLjUiLz4KPC9zdmc+')] opacity-10"></div>
              </div>
              
              {/* Flight Routes */}
              <div className="absolute inset-0">
                {/* Flight paths */}
                <svg className="w-full h-full">
                  <path 
                    d="M100 200 Q300 100 500 180" 
                    stroke="#F59E0B" 
                    strokeWidth="2" 
                    fill="none" 
                    opacity="0.8"
                    data-testid="flight-route-1"
                  />
                  <path 
                    d="M150 300 Q400 200 650 250" 
                    stroke="#F59E0B" 
                    strokeWidth="2" 
                    fill="none" 
                    opacity="0.8"
                    data-testid="flight-route-2"
                  />
                  <path 
                    d="M200 150 Q450 80 700 160" 
                    stroke="#F59E0B" 
                    strokeWidth="2" 
                    fill="none" 
                    opacity="0.8"
                    data-testid="flight-route-3"
                  />
                </svg>

                {/* Flight Icons */}
                {Array.from({ length: 15 }, (_, index) => (
                  <div
                    key={index}
                    className="absolute text-orange-400"
                    style={{
                      left: `${20 + (index * 4) % 60}%`,
                      top: `${25 + (index * 3) % 50}%`,
                      transform: `rotate(${index * 24}deg)`
                    }}
                    data-testid={`flight-icon-${index}`}
                  >
                    ✈️
                  </div>
                ))}
              </div>

              {/* Map Region Labels */}
              <div className="absolute top-8 left-8 text-sm text-gray-400">
                <div>ATLANTIC</div>
                <div>OCEAN</div>
              </div>
              <div className="absolute top-20 right-20 text-sm text-gray-400">
                <div>PACIFIC</div>
                <div>OCEAN</div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Right Panel */}
        <div className="col-span-4 space-y-6">
          {/* Key Metrics */}
          <div className="grid grid-cols-1 gap-4">
            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4 flex items-center gap-4">
                <Plane className="w-8 h-8 text-orange-400" />
                <div>
                  <div className="text-sm text-gray-400">Total Flights</div>
                  <div className="text-3xl font-bold text-white" data-testid="text-total-flights">5,820</div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4 flex items-center gap-4">
                <div className="w-8 h-8 text-blue-400 flex items-center justify-center">📊</div>
                <div>
                  <div className="text-sm text-gray-400">Altitude</div>
                  <div className="text-3xl font-bold text-white" data-testid="text-altitude">32,450 ft</div>
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4 flex items-center gap-4">
                <div className="w-8 h-8 text-green-400 flex items-center justify-center">⚡</div>
                <div>
                  <div className="text-sm text-gray-400">Average Speed</div>
                  <div className="text-3xl font-bold text-white" data-testid="text-speed">475 kt</div>
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Flights by Region Chart */}
          <Card className="bg-gray-900/50 border-gray-700">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Flights by Region</CardTitle>
            </CardHeader>
            <CardContent className="flex items-center justify-center">
              <div className="relative w-32 h-32">
                <svg className="w-full h-full">
                  <circle cx="64" cy="64" r="50" fill="none" stroke="#374151" strokeWidth="8" />
                  <circle 
                    cx="64" cy="64" r="50" fill="none" stroke="#F59E0B" strokeWidth="8"
                    strokeDasharray={`${2 * Math.PI * 50 * 0.35} ${2 * Math.PI * 50}`}
                    strokeLinecap="round"
                    transform="rotate(-90 64 64)"
                    data-testid="region-chart-segment-1"
                  />
                  <circle 
                    cx="64" cy="64" r="50" fill="none" stroke="#3B82F6" strokeWidth="8"
                    strokeDasharray={`${2 * Math.PI * 50 * 0.28} ${2 * Math.PI * 50}`}
                    strokeDashoffset={`${-2 * Math.PI * 50 * 0.35}`}
                    strokeLinecap="round"
                    transform="rotate(-90 64 64)"
                    data-testid="region-chart-segment-2"
                  />
                </svg>
              </div>
              <div className="ml-6 space-y-2 text-sm">
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-orange-500 rounded-full"></div>
                  <span className="text-gray-300">North America</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                  <span className="text-gray-300">Europe</span>
                </div>
                <div className="flex items-center gap-2">
                  <div className="w-3 h-3 bg-purple-500 rounded-full"></div>
                  <span className="text-gray-300">Asia</span>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Top Airlines */}
          <Card className="bg-gray-900/50 border-gray-700">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Top Airlines</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {topAirlines.map((airline, index) => (
                <div key={airline.name} className="flex justify-between items-center text-sm">
                  <span className="text-gray-300" data-testid={`airline-name-${index}`}>
                    {airline.name}
                  </span>
                  <span className="text-white font-medium" data-testid={`airline-routes-${index}`}>
                    {airline.routes}
                  </span>
                </div>
              ))}
            </CardContent>
          </Card>
        </div>

        {/* Bottom Flight Table */}
        <div className="col-span-11 col-start-2">
          <Card className="bg-gray-900/50 border-gray-700">
            <CardContent className="p-0">
              <div className="overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-700">
                      <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-flight">Flight</th>
                      <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-airline">Airline</th>
                      <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-departure">Departure</th>
                      <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-arrival">Arrival</th>
                      <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-status">Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {flightData.map((flight, index) => (
                      <tr key={index} className="border-b border-gray-800">
                        <td className="p-4 text-white font-medium" data-testid={`table-flight-${index}`}>
                          {flight.flight}
                        </td>
                        <td className="p-4 text-gray-300" data-testid={`table-airline-${index}`}>
                          {flight.airline}
                        </td>
                        <td className="p-4 text-white" data-testid={`table-departure-${index}`}>
                          {flight.departure}
                        </td>
                        <td className="p-4 text-white" data-testid={`table-arrival-${index}`}>
                          {flight.arrival}
                        </td>
                        <td className="p-4 text-orange-400" data-testid={`table-status-${index}`}>
                          {flight.status}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}