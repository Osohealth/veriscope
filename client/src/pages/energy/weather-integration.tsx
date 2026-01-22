import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Link } from "wouter";
import { ArrowLeft, Cloud, Wind, Droplets, Thermometer, Eye } from "lucide-react";
import { LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from "recharts";

export default function WeatherIntegration() {
  const temperatureData = [
    { hour: "00:00", temp: 8, dewpoint: 4, windchill: 5 },
    { hour: "04:00", temp: 6, dewpoint: 2, windchill: 2 },
    { hour: "08:00", temp: 10, dewpoint: 3, windchill: 7 },
    { hour: "12:00", temp: 15, dewpoint: 6, windchill: 12 },
    { hour: "16:00", temp: 14, dewpoint: 5, windchill: 11 },
    { hour: "20:00", temp: 10, dewpoint: 4, windchill: 7 },
    { hour: "24:00", temp: 8, dewpoint: 3, windchill: 5 },
  ];

  const windSpeedData = [
    { hour: "00:00", speed: 8.5, gusts: 12, direction: 230 },
    { hour: "04:00", speed: 9.2, gusts: 14, direction: 235 },
    { hour: "08:00", speed: 7.8, gusts: 11, direction: 240 },
    { hour: "12:00", speed: 6.5, gusts: 9, direction: 245 },
    { hour: "16:00", speed: 7.1, gusts: 10, direction: 242 },
    { hour: "20:00", speed: 8.9, gusts: 13, direction: 238 },
    { hour: "24:00", speed: 9.5, gusts: 15, direction: 232 },
  ];

  const precipitationData = [
    { hour: "00:00", rain: 0.2, probability: 15 },
    { hour: "04:00", rain: 0.5, probability: 25 },
    { hour: "08:00", rain: 0.1, probability: 10 },
    { hour: "12:00", rain: 0, probability: 5 },
    { hour: "16:00", rain: 0, probability: 3 },
    { hour: "20:00", rain: 0.3, probability: 20 },
    { hour: "24:00", rain: 0.8, probability: 35 },
  ];

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/energy">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Energy
              </Button>
            </Link>
            <div>
              <h1 className="text-2xl font-bold flex items-center gap-2" data-testid="text-page-title">
                <Cloud className="w-6 h-6 text-primary" />
                Weather Integration
              </h1>
              <p className="text-sm text-muted-foreground">Temperature, wind, and precipitation impact models</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Current Weather Summary */}
        <section className="grid grid-cols-1 md:grid-cols-5 gap-4">
          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Thermometer className="w-5 h-5 text-primary" />
                Temperature
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">14°C</div>
              <p className="text-xs text-muted-foreground mt-2">Current</p>
              <Badge variant="outline" className="mt-2">
                High: 16°C
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Wind className="w-5 h-5 text-primary" />
                Wind Speed
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">7.1 m/s</div>
              <p className="text-xs text-muted-foreground mt-2">Current</p>
              <Badge variant="outline" className="mt-2">
                Gusts: 10 m/s
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Droplets className="w-5 h-5 text-primary" />
                Humidity
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">65%</div>
              <p className="text-xs text-muted-foreground mt-2">Relative</p>
              <Badge variant="outline" className="mt-2">
                Dewpoint: 5°C
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Cloud className="w-5 h-5 text-primary" />
                Cloud Cover
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">35%</div>
              <p className="text-xs text-muted-foreground mt-2">Current</p>
              <Badge variant="outline" className="mt-2">
                Mostly clear
              </Badge>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-base flex items-center gap-2">
                <Eye className="w-5 h-5 text-primary" />
                Visibility
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-3xl font-bold">18 km</div>
              <p className="text-xs text-muted-foreground mt-2">Current</p>
              <Badge variant="outline" className="mt-2">
                Excellent
              </Badge>
            </CardContent>
          </Card>
        </section>

        {/* Temperature Trends */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Temperature & Wind Chill Forecast</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <LineChart data={temperatureData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Line type="monotone" dataKey="temp" stroke="#ff9800" strokeWidth={2} name="Temperature (°C)" />
                  <Line type="monotone" dataKey="windchill" stroke="#2196f3" strokeWidth={2} name="Wind Chill (°C)" />
                  <Line type="monotone" dataKey="dewpoint" stroke="#4caf50" strokeWidth={2} name="Dewpoint (°C)" />
                </LineChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Wind Analysis */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Wind Speed & Gust Forecast</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={windSpeedData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="speed" fill="#00e5ff" stroke="#00e5ff" name="Wind Speed (m/s)" />
                  <Area type="monotone" dataKey="gusts" fill="#ff6b6b" stroke="#ff6b6b" name="Wind Gusts (m/s)" opacity={0.6} />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Precipitation */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>Precipitation & Rain Probability</CardTitle>
            </CardHeader>
            <CardContent>
              <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={precipitationData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="hour" />
                  <YAxis />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="rain" fill="#4caf50" stroke="#4caf50" name="Rainfall (mm)" />
                  <Area type="monotone" dataKey="probability" fill="#2196f3" stroke="#2196f3" name="Probability (%)" opacity={0.6} />
                </AreaChart>
              </ResponsiveContainer>
            </CardContent>
          </Card>
        </section>

        {/* Impact on Energy Systems */}
        <section className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Temperature Impact</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <p className="text-sm font-medium">Heating Demand</p>
                <div className="w-full h-2 bg-secondary rounded-full">
                  <div className="h-full bg-orange-500" style={{ width: "35%" }} />
                </div>
                <p className="text-xs text-muted-foreground">Moderate heating load</p>
              </div>
              <div className="p-2 bg-secondary/50 rounded text-xs text-muted-foreground">
                Each 1°C decrease adds ~500 MW to demand
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Wind Impact</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <p className="text-sm font-medium">Wind Generation</p>
                <div className="w-full h-2 bg-secondary rounded-full">
                  <div className="h-full bg-blue-500" style={{ width: "64%" }} />
                </div>
                <p className="text-xs text-muted-foreground">High wind conditions</p>
              </div>
              <div className="p-2 bg-secondary/50 rounded text-xs text-muted-foreground">
                Current winds support strong wind output
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base">Solar Impact</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="space-y-2">
                <p className="text-sm font-medium">Solar Generation</p>
                <div className="w-full h-2 bg-secondary rounded-full">
                  <div className="h-full bg-yellow-500" style={{ width: "48%" }} />
                </div>
                <p className="text-xs text-muted-foreground">Moderate cloud cover</p>
              </div>
              <div className="p-2 bg-secondary/50 rounded text-xs text-muted-foreground">
                35% cloud cover reducing solar output
              </div>
            </CardContent>
          </Card>
        </section>

        {/* 7-Day Forecast Impact */}
        <section>
          <Card>
            <CardHeader>
              <CardTitle>7-Day Weather Impact Summary</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="grid grid-cols-1 md:grid-cols-7 gap-2">
                {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((day, i) => (
                  <div key={day} className="p-2 bg-secondary/50 rounded text-center">
                    <p className="text-xs font-semibold">{day}</p>
                    <p className="text-lg font-bold mt-1">{12 + (i % 3)}°C</p>
                    <p className="text-xs text-muted-foreground mt-1">{8 + (i % 4)} m/s</p>
                  </div>
                ))}
              </div>
              <p className="text-sm text-muted-foreground pt-2 border-t">
                Expected conditions: Stable temperatures with moderate wind patterns. No significant weather events forecast.
              </p>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
