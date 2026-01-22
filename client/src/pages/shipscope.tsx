import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useQuery } from "@tanstack/react-query";
import { Ship, Menu, Settings, Triangle } from "lucide-react";
import { Link } from "wouter";

interface Vessel {
  id: string;
  name: string;
  type: string;
  speed?: number;
}

export default function ShipScope() {
  const { data: vessels = [] } = useQuery<Vessel[]>({
    queryKey: ['/api/vessels'],
    refetchInterval: 30000,
  });

  const { data: portStats } = useQuery({
    queryKey: ['/api/ports/fujairah/stats'],
    refetchInterval: 60000,
  });

  // Mock data for vessel types
  const vesselsByType = [
    { type: 'Tanker', count: 23 },
    { type: 'Cargo', count: 18 },
    { type: 'Container', count: 15 },
    { type: 'Passenger', count: 8 }
  ];

  const topVessels = vessels.slice(0, 5);

  return (
    <div className="min-h-screen bg-[#0A0B1E] text-white">
      {/* Header */}
      <header className="flex items-center justify-between p-6 border-b border-gray-800">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-menu">
            <Menu className="w-5 h-5" />
          </Button>
          <h1 className="text-2xl font-bold">Maritime Intelligence</h1>
        </div>
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-location">
            📍 Sen
          </Button>
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-alerts">
            <Triangle className="w-5 h-5" />
          </Button>
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-warning">
            <Triangle className="w-5 h-5" />
          </Button>
        </div>
      </header>

      <div className="grid grid-cols-12 gap-6 p-6 h-[calc(100vh-88px)]">
        {/* Left Sidebar */}
        <div className="col-span-1 space-y-6">
          <nav className="space-y-4">
            <Link href="/dashboard/shipscope">
              <Button variant="ghost" size="icon" className="w-full text-blue-400 bg-blue-400/10" data-testid="button-nav-ship">
                <Ship className="w-5 h-5" />
              </Button>
            </Link>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-3d">
              📦
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-chart">
              📊
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-refresh">
              🔄
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-alert">
              ⚠️
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
              
              {/* Vessel Dots */}
              <div className="absolute inset-0">
                {vessels.slice(0, 20).map((vessel, index) => (
                  <div
                    key={vessel.id}
                    className="absolute w-2 h-2 bg-blue-400 rounded-full shadow-lg shadow-blue-400/50"
                    style={{
                      left: `${20 + (index * 3) % 60}%`,
                      top: `${30 + (index * 2) % 40}%`,
                    }}
                    data-testid={`vessel-dot-${index}`}
                  />
                ))}
              </div>

              {/* Map Labels */}
              <div className="absolute top-8 left-8 text-sm text-gray-400">
                Total Vessels
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Right Panel */}
        <div className="col-span-4 space-y-6">
          {/* Key Metrics */}
          <div className="grid grid-cols-1 gap-4">
            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4">
                <div className="text-sm text-gray-400 mb-1">TOTAL VESSELS</div>
                <div className="text-3xl font-bold text-white" data-testid="text-total-vessels">
                  {vessels.length.toLocaleString()}
                </div>
              </CardContent>
            </Card>

            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4">
                <div className="text-sm text-gray-400 mb-1">NUMBER OF PORTS</div>
                <div className="text-3xl font-bold text-white" data-testid="text-ports">3,452</div>
              </CardContent>
            </Card>

            <Card className="bg-gray-900/50 border-gray-700">
              <CardContent className="p-4">
                <div className="text-sm text-gray-400 mb-1">AVERAGE SPEED (KTS)</div>
                <div className="text-3xl font-bold text-white" data-testid="text-speed">12.8</div>
              </CardContent>
            </Card>
          </div>

          {/* Top Vessels */}
          <Card className="bg-gray-900/50 border-gray-700">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Top Vessels</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {topVessels.map((vessel, index) => (
                <div key={vessel.id} className="flex justify-between items-center text-sm">
                  <div>
                    <div className="text-white font-medium" data-testid={`vessel-name-${index}`}>
                      {vessel.name}
                    </div>
                    <div className="text-gray-400">{vessel.type}</div>
                  </div>
                  <div className="text-blue-400">Underway</div>
                </div>
              ))}
            </CardContent>
          </Card>
        </div>

        {/* Bottom Charts */}
        <div className="col-span-4 col-start-2">
          <Card className="bg-gray-900/50 border-gray-700 h-full">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Vessels by Type</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                {vesselsByType.map((item, index) => (
                  <div key={item.type} className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-gray-300">{item.type}</span>
                      <span className="text-white font-medium">{item.count}</span>
                    </div>
                    <div className="h-2 bg-gray-700 rounded-full">
                      <div 
                        className="h-full bg-blue-400 rounded-full"
                        style={{ width: `${(item.count / 25) * 100}%` }}
                        data-testid={`chart-bar-${index}`}
                      />
                    </div>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </div>

        <div className="col-span-4">
          <Card className="bg-gray-900/50 border-gray-700 h-full">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Port Congestion</CardTitle>
            </CardHeader>
            <CardContent className="flex items-center justify-center">
              <div className="relative w-32 h-32">
                <svg className="w-full h-full transform -rotate-90">
                  <circle
                    cx="64"
                    cy="64"
                    r="50"
                    fill="none"
                    stroke="#374151"
                    strokeWidth="8"
                  />
                  <circle
                    cx="64"
                    cy="64"
                    r="50"
                    fill="none"
                    stroke="#3B82F6"
                    strokeWidth="8"
                    strokeDasharray={`${2 * Math.PI * 50 * 0.65} ${2 * Math.PI * 50}`}
                    strokeLinecap="round"
                    data-testid="congestion-chart"
                  />
                </svg>
                <div className="absolute inset-0 flex items-center justify-center">
                  <span className="text-2xl font-bold text-white">65%</span>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Vessels Table */}
        <div className="col-span-8 col-start-2">
          <Card className="bg-gray-900/50 border-gray-700">
            <CardHeader className="pb-3">
              <CardTitle className="text-lg text-white">Top Vessels</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="overflow-hidden">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-700">
                      <th className="text-left text-gray-400 font-medium pb-3" data-testid="table-header-name">Name</th>
                      <th className="text-left text-gray-400 font-medium pb-3" data-testid="table-header-type">Type</th>
                      <th className="text-left text-gray-400 font-medium pb-3" data-testid="table-header-status">Status</th>
                      <th className="text-left text-gray-400 font-medium pb-3" data-testid="table-header-speed">Speed</th>
                      <th className="text-left text-gray-400 font-medium pb-3" data-testid="table-header-speed-kts">Speed (kts)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topVessels.map((vessel, index) => (
                      <tr key={vessel.id} className="border-b border-gray-800">
                        <td className="py-3 text-white font-medium" data-testid={`table-vessel-name-${index}`}>
                          {vessel.name}
                        </td>
                        <td className="py-3 text-gray-300" data-testid={`table-vessel-type-${index}`}>
                          {vessel.type}
                        </td>
                        <td className="py-3 text-blue-400" data-testid={`table-vessel-status-${index}`}>
                          Underway
                        </td>
                        <td className="py-3 text-white" data-testid={`table-vessel-speed-${index}`}>
                          {vessel.speed || '12.5'}
                        </td>
                        <td className="py-3 text-white" data-testid={`table-vessel-speed-kts-${index}`}>
                          {vessel.speed || '12.5'}
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