import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DataFreshnessIndicator } from "@/components/data-freshness-indicator";
import { useQuery } from "@tanstack/react-query";
import { BarChart3, Menu, Settings, PieChart } from "lucide-react";
import { Link } from "wouter";

export default function TankScope() {
  const { data: predictions = [], dataUpdatedAt } = useQuery({
    queryKey: ['/api/predictions'],
    refetchInterval: 60000,
  });

  // Mock commodity data
  const commodityVolumes = [
    { name: 'LNG', volume: 350, color: '#3B82F6' },
    { name: 'Crude', volume: 280, color: '#3B82F6' },
    { name: 'Iron Ore', volume: 180, color: '#3B82F6' },
    { name: 'Oil Products', volume: 150, color: '#3B82F6' },
    { name: 'Others', volume: 100, color: '#3B82F6' }
  ];

  const marketData = [
    { market: 'Physical Commodities', value: '129.5 M' },
    { market: 'Power & Renewables', value: '87.2 M' },
    { market: 'Freight', value: '522.8 M' },
    { market: 'Finance', value: '$7541 M' }
  ];

  const regionData = [
    { product: 'LNG', region: 'Asia', volume: '45.2 M', change: 2.4, trend: 'up' },
    { product: 'Crude', region: 'North America', volume: '27.2 M', change: 1.5, trend: 'down' },
    { product: 'Iron Ore', region: 'Europe', volume: '21.4 M', change: 1.5, trend: 'down' },
    { product: 'Oil Products', region: 'South America', volume: '52.28 M', change: 9.4, trend: 'up' },
    { product: 'Chemicals', region: 'Europe', volume: '29.6 M', change: 1.2, trend: 'up' },
    { product: 'Chemicals', region: 'Europe', volume: '29.6 M', change: 3.2, trend: 'up' }
  ];

  return (
    <div className="min-h-screen bg-[#0A0B1E] text-white">
      {/* Header */}
      <header className="flex items-center justify-between p-6 border-b border-gray-800">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" className="text-gray-400" data-testid="button-menu">
            <Menu className="w-5 h-5" />
          </Button>
          <h1 className="text-2xl font-bold">TankScope Analytics</h1>
        </div>
        <DataFreshnessIndicator 
          lastUpdate={dataUpdatedAt ? new Date(dataUpdatedAt) : undefined}
          streamName="Market Data"
          showLabel={true}
        />
      </header>

      <div className="grid grid-cols-12 gap-6 p-6">
        {/* Left Sidebar */}
        <div className="col-span-1 space-y-6">
          <nav className="space-y-4">
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-chart">
              <BarChart3 className="w-5 h-5" />
            </Button>
            <Link href="/dashboard/tankscope">
              <Button variant="ghost" size="icon" className="w-full text-blue-400 bg-blue-400/10" data-testid="button-nav-pie">
                <PieChart className="w-5 h-5" />
              </Button>
            </Link>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-clock">
              🕒
            </Button>
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-nav-globe">
              🌍
            </Button>
          </nav>
          <div className="mt-auto">
            <Button variant="ghost" size="icon" className="w-full text-gray-400" data-testid="button-settings">
              <Settings className="w-5 h-5" />
            </Button>
          </div>
        </div>

        {/* Main Content */}
        <div className="col-span-11">
          <div className="grid grid-cols-12 gap-6">
            {/* Commodity Volumes Chart */}
            <div className="col-span-4">
              <Card className="bg-gray-900/50 border-gray-700 h-full">
                <CardHeader>
                  <CardTitle className="text-white">Commodity Volumes by Region</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-4">
                    {commodityVolumes.map((item, index) => (
                      <div key={item.name} className="space-y-2">
                        <div className="flex justify-between text-sm">
                          <span className="text-gray-300">{item.name}</span>
                          <span className="text-white">{item.volume}</span>
                        </div>
                        <div className="h-3 bg-gray-700 rounded-full">
                          <div 
                            className="h-full bg-blue-500 rounded-full"
                            style={{ width: `${(item.volume / 350) * 100}%` }}
                            data-testid={`commodity-bar-${index}`}
                          />
                        </div>
                      </div>
                    ))}
                  </div>
                  <div className="flex justify-between text-xs text-gray-400 mt-4">
                    <span>0</span>
                    <span>50</span>
                    <span>100</span>
                    <span>150</span>
                    <span>350</span>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Market Share */}
            <div className="col-span-4">
              <Card className="bg-gray-900/50 border-gray-700 h-full">
                <CardHeader>
                  <CardTitle className="text-white">Market Share</CardTitle>
                </CardHeader>
                <CardContent className="flex items-center justify-center">
                  <div className="relative w-48 h-48">
                    <svg className="w-full h-full transform -rotate-90">
                      <circle
                        cx="96"
                        cy="96"
                        r="70"
                        fill="none"
                        stroke="#374151"
                        strokeWidth="16"
                      />
                      <circle
                        cx="96"
                        cy="96"
                        r="70"
                        fill="none"
                        stroke="#3B82F6"
                        strokeWidth="16"
                        strokeDasharray={`${2 * Math.PI * 70 * 0.34} ${2 * Math.PI * 70}`}
                        strokeLinecap="round"
                        data-testid="market-share-chart"
                      />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <span className="text-4xl font-bold text-white">34%</span>
                    </div>
                  </div>
                  <div className="ml-8 space-y-2">
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
                      <span className="text-sm text-gray-300">North America</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 bg-purple-500 rounded-full"></div>
                      <span className="text-sm text-gray-300">Europe</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 bg-cyan-500 rounded-full"></div>
                      <span className="text-sm text-gray-300">Asia</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="w-3 h-3 bg-gray-500 rounded-full"></div>
                      <span className="text-sm text-gray-300">Other</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Markets */}
            <div className="col-span-4">
              <Card className="bg-gray-900/50 border-gray-700 h-full">
                <CardHeader>
                  <CardTitle className="text-white">Markets</CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {marketData.map((market, index) => (
                    <div key={market.market} className="flex justify-between items-center">
                      <span className="text-gray-300 text-sm" data-testid={`market-name-${index}`}>
                        {market.market}
                      </span>
                      <span className="text-white font-medium" data-testid={`market-value-${index}`}>
                        {market.value}
                      </span>
                    </div>
                  ))}
                </CardContent>
              </Card>
            </div>

            {/* Regional Distribution Map */}
            <div className="col-span-4 row-span-2">
              <Card className="bg-gray-900/50 border-gray-700 h-full">
                <CardHeader>
                  <CardTitle className="text-white">Regional Distribution</CardTitle>
                </CardHeader>
                <CardContent className="flex items-center justify-center h-full">
                  <div className="relative w-full h-48 bg-gradient-to-br from-blue-900/20 to-blue-800/10 rounded-lg overflow-hidden">
                    <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTAwIiBoZWlnaHQ9IjEwMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KICA8cGF0aCBkPSJNMTAgMTBMMjAgMjBNMjAgMTBMMTAgMjAiIHN0cm9rZT0iIzMzMzMzMyIgc3Ryb2tlLXdpZHRoPSIwLjUiLz4KPC9zdmc+')] opacity-10"></div>
                    <div className="absolute inset-0 flex items-center justify-center">
                      <span className="text-gray-400">World Map</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Data Analysis */}
            <div className="col-span-4 row-span-2">
              <Card className="bg-gray-900/50 border-gray-700 h-full">
                <CardHeader>
                  <CardTitle className="text-white">🔍 Data Tale</CardTitle>
                </CardHeader>
                <CardContent className="flex items-center justify-center h-full">
                  <div className="text-center text-gray-400">
                    <div className="w-12 h-12 mx-auto mb-4 bg-gray-700 rounded-lg flex items-center justify-center">
                      📊
                    </div>
                    <p className="text-sm">Analytics Dashboard</p>
                  </div>
                </CardContent>
              </Card>
            </div>

            {/* Data Table */}
            <div className="col-span-8">
              <Card className="bg-gray-900/50 border-gray-700">
                <CardContent className="p-0">
                  <div className="overflow-hidden">
                    <table className="w-full text-sm">
                      <thead>
                        <tr className="border-b border-gray-700">
                          <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-product">Product</th>
                          <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-region">Region</th>
                          <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-volume">Volume</th>
                          <th className="text-left text-gray-400 font-medium p-4" data-testid="table-header-change">Change</th>
                        </tr>
                      </thead>
                      <tbody>
                        {regionData.map((item, index) => (
                          <tr key={index} className="border-b border-gray-800">
                            <td className="p-4 text-white font-medium" data-testid={`table-product-${index}`}>
                              {item.product}
                            </td>
                            <td className="p-4 text-gray-300" data-testid={`table-region-${index}`}>
                              {item.region}
                            </td>
                            <td className="p-4 text-white" data-testid={`table-volume-${index}`}>
                              {item.volume}
                            </td>
                            <td className={`p-4 ${item.trend === 'up' ? 'text-green-400' : 'text-red-400'}`} data-testid={`table-change-${index}`}>
                              {item.trend === 'up' ? '▲' : '▼'} {item.change}%
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
      </div>
    </div>
  );
}