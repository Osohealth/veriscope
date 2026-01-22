import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { TrendingUp, TrendingDown, Minus, AlertTriangle } from "lucide-react";
import { useDelayAdjustedPredictions } from "@/hooks/use-delay-adjusted-predictions";
import { useRotterdamData } from "@/hooks/use-rotterdam-data";

interface CommodityPriceChartProps {
  dashboardType: string;
  region?: string;
  selectedPort?: string;
  selectedMonth?: string;
}

export default function CommodityPriceChart({ dashboardType, region = "global", selectedPort, selectedMonth }: CommodityPriceChartProps) {
  // Fetch delay-adjusted predictions for Brent crude when Rotterdam is selected
  const { data: delayAdjustedData } = useDelayAdjustedPredictions(
    selectedPort === 'rotterdam' ? 'rotterdam' : undefined,
    'BRENT'
  );
  
  // Fetch Rotterdam data when Rotterdam is selected (with month filtering)
  const { data: rotterdamData } = useRotterdamData(selectedMonth, selectedPort === 'rotterdam');
  
  // Using mock data directly for reliable UI performance
  const getCommodityData = () => {
    switch (dashboardType) {
      case 'crude-oil':
        // Use Rotterdam CSV data if available and Rotterdam is selected
        if (selectedPort === 'rotterdam' && rotterdamData?.stats) {
          const brentPrice = rotterdamData.stats.avgBrentPrice;
          const rotterdamPrice = rotterdamData.stats.avgRotterdamPrice;
          const spread = rotterdamData.stats.avgSpread;
          const brentChange = spread;
          const brentChangePercent = (brentChange / brentPrice) * 100;
          
          return [
            { 
              name: 'Brent Crude', 
              price: brentPrice, 
              change: brentChange, 
              changePercent: brentChangePercent, 
              unit: '$/bbl',
              delayAdjusted: false,
              delayImpact: 0
            },
            { 
              name: 'Rotterdam Crude', 
              price: rotterdamPrice, 
              change: spread, 
              changePercent: (spread / brentPrice) * 100, 
              unit: '$/bbl' 
            },
            { name: 'Local Spread', price: spread, change: 0, changePercent: 0, unit: '$/bbl' },
          ];
        }
        
        // Use delay-adjusted price for Brent if available
        const brentPrice = delayAdjustedData?.adjustedPrediction 
          ? parseFloat(delayAdjustedData.adjustedPrediction.predictedPrice)
          : 75.24;
        const brentBasePrice = delayAdjustedData?.basePrediction 
          ? parseFloat(delayAdjustedData.basePrediction.currentPrice)
          : 74.01;
        const brentChange = brentPrice - brentBasePrice;
        const brentChangePercent = (brentChange / brentBasePrice) * 100;
        
        return [
          { 
            name: 'Brent Crude', 
            price: brentPrice, 
            change: brentChange, 
            changePercent: brentChangePercent, 
            unit: '$/bbl',
            delayAdjusted: delayAdjustedData?.delayAdjusted || false,
            delayImpact: delayAdjustedData?.delayImpact ? parseFloat(delayAdjustedData.delayImpact.priceImpact) : 0
          },
          { name: 'WTI Crude', price: 72.89, change: 0.98, changePercent: 1.36, unit: '$/bbl' },
          { name: 'Dubai Crude', price: 74.12, change: -0.45, changePercent: -0.60, unit: '$/bbl' },
        ];
      case 'refined-products':
        return [
          { name: 'Gasoline', price: 2.28, change: 0.12, changePercent: 5.56, unit: '$/gal' },
          { name: 'Diesel', price: 2.45, change: -0.05, changePercent: -2.00, unit: '$/gal' },
          { name: 'Jet Fuel', price: 2.67, change: 0.08, changePercent: 3.09, unit: '$/gal' },
        ];
      case 'lng':
        return [
          { name: 'JKM (LNG)', price: 12.45, change: 0.67, changePercent: 5.69, unit: '$/MMBtu' },
          { name: 'TTF (Gas)', price: 11.89, change: -0.23, changePercent: -1.90, unit: '$/MMBtu' },
          { name: 'Henry Hub', price: 9.34, change: 0.45, changePercent: 5.07, unit: '$/MMBtu' },
        ];
      default:
        return [
          { name: 'Brent Crude', price: 75.24, change: 1.23, changePercent: 1.66, unit: '$/bbl' },
          { name: 'WTI Crude', price: 72.89, change: 0.98, changePercent: 1.36, unit: '$/bbl' },
        ];
    }
  };

  const commodityData = getCommodityData();

  const getTrendIcon = (change: number) => {
    if (change > 0) return <TrendingUp className="w-4 h-4 text-green-500" />;
    if (change < 0) return <TrendingDown className="w-4 h-4 text-red-500" />;
    return <Minus className="w-4 h-4 text-muted-foreground" />;
  };

  const getTrendColor = (change: number) => {
    if (change > 0) return 'text-green-500';
    if (change < 0) return 'text-red-500';
    return 'text-muted-foreground';
  };

  const getDashboardTitle = () => {
    switch (dashboardType) {
      case 'crude-oil': return 'Crude Oil Prices';
      case 'refined-products': return 'Refined Products Prices';
      case 'lng': return 'LNG & Gas Prices';
      case 'maritime': return 'Freight Rates';
      case 'trade-flows': return 'Trade Flow Indicators';
      case 'market-analytics': return 'Market Indicators';
      default: return 'Commodity Prices';
    }
  };

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-sm font-medium" data-testid="text-commodity-prices-title">
          {getDashboardTitle()}
        </CardTitle>
        <Badge variant="outline" className="w-fit text-xs">
          Live {region.charAt(0).toUpperCase() + region.slice(1)}
        </Badge>
      </CardHeader>
      <CardContent className="space-y-3">
        {commodityData.map((commodity, index) => (
          <div 
            key={commodity.name} 
            className="flex justify-between items-center py-2 border-b border-border last:border-0"
            data-testid={`commodity-price-${index}`}
          >
            <div className="space-y-1">
              <div className="flex items-center gap-2">
                <div className="text-sm font-medium text-foreground">
                  {commodity.name}
                </div>
                {(commodity as any).delayAdjusted && (
                  <Badge variant="outline" className="text-xs px-1.5 py-0 bg-amber-500/10 text-amber-500 border-amber-500/30">
                    <AlertTriangle className="w-3 h-3 mr-1" />
                    Delay Adjusted
                  </Badge>
                )}
              </div>
              <div className="text-xs text-muted-foreground">
                {commodity.unit}
                {(commodity as any).delayAdjusted && (commodity as any).delayImpact > 0 && (
                  <span className="ml-1.5 text-amber-500">
                    (+${((commodity as any).delayImpact).toFixed(2)} delay impact)
                  </span>
                )}
              </div>
            </div>
            <div className="text-right space-y-1">
              <div className="text-sm font-mono font-medium">
                {commodity.price.toFixed(2)}
              </div>
              <div className={`flex items-center space-x-1 text-xs ${getTrendColor(commodity.change)}`}>
                {getTrendIcon(commodity.change)}
                <span className="font-mono">
                  {commodity.change > 0 ? '+' : ''}{commodity.change.toFixed(2)}
                </span>
                <span className="font-mono">
                  ({commodity.changePercent > 0 ? '+' : ''}{commodity.changePercent.toFixed(1)}%)
                </span>
              </div>
            </div>
          </div>
        ))}
      </CardContent>
    </Card>
  );
}