import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Link } from "wouter";
import { ArrowLeft, Container, Ship, TrendingUp, Package, BarChart3 } from "lucide-react";
import type { ContainerOperation, Port, Vessel } from "@shared/schema";

interface EnrichedContainerOperation extends ContainerOperation {
  port?: Port;
  vessel?: Vessel;
}

export default function ContainerIntelligence() {
  const [selectedPort, setSelectedPort] = useState<string>("all");
  const [selectedOperationType, setSelectedOperationType] = useState<string>("all");

  // Fetch ports
  const { data: ports = [] } = useQuery<Port[]>({
    queryKey: ['/api/ports']
  });

  // Fetch vessels
  const { data: vessels = [] } = useQuery<Vessel[]>({
    queryKey: ['/api/vessels']
  });

  // Fetch container operations with port filtering
  const operationsUrl = selectedPort !== "all" 
    ? `/api/container-operations?portId=${selectedPort}` 
    : '/api/container-operations';
  
  const { data: operations = [], isLoading } = useQuery<ContainerOperation[]>({
    queryKey: [operationsUrl]
  });

  // Enrich operations with port and vessel data
  const enrichedOperations: EnrichedContainerOperation[] = operations.map(op => ({
    ...op,
    port: ports.find(p => p.id === op.portId),
    vessel: vessels.find(v => v.id === op.vesselId)
  }));

  // Filter by operation type
  const filteredOperations = selectedOperationType === "all" 
    ? enrichedOperations 
    : enrichedOperations.filter(op => op.operationType === selectedOperationType);

  // Calculate statistics
  const stats = {
    totalOperations: filteredOperations.length,
    totalTEU: filteredOperations.reduce((sum, op) => sum + (op.teuCount || 0), 0),
    totalFEU: filteredOperations.reduce((sum, op) => sum + (op.feuCount || 0), 0),
    loadOperations: filteredOperations.filter(op => op.operationType === 'load').length,
    dischargeOperations: filteredOperations.filter(op => op.operationType === 'discharge').length,
    transshipmentOperations: filteredOperations.filter(op => op.operationType === 'transshipment').length
  };

  const getOperationColor = (type: string): "default" | "secondary" | "outline" => {
    switch (type.toLowerCase()) {
      case 'load': return "default";
      case 'discharge': return "secondary";
      case 'transshipment': return "outline";
      default: return "secondary";
    }
  };

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="bg-card border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link href="/maritime">
              <Button variant="ghost" size="sm" data-testid="button-back">
                <ArrowLeft className="w-4 h-4 mr-2" />
                Back to Maritime
              </Button>
            </Link>
            <div>
              <div className="flex items-center gap-2">
                <Container className="w-6 h-6 text-primary" />
                <h1 className="text-2xl font-bold" data-testid="text-page-title">Container Intelligence</h1>
              </div>
              <p className="text-sm text-muted-foreground">TEU tracking and port container operations</p>
            </div>
          </div>
        </div>
      </header>

      <div className="p-6 space-y-6">
        {/* Statistics Cards */}
        <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-6 gap-4">
          <Card data-testid="card-total-operations">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total Operations</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold" data-testid="text-total-operations">{stats.totalOperations}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-total-teu">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total TEU</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-blue-500" data-testid="text-total-teu">
                {stats.totalTEU.toLocaleString()}
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-total-feu">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Total FEU</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-green-500" data-testid="text-total-feu">
                {stats.totalFEU.toLocaleString()}
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-load-ops">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Load Ops</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-cyan-500" data-testid="text-load-ops">{stats.loadOperations}</div>
            </CardContent>
          </Card>

          <Card data-testid="card-discharge-ops">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Discharge Ops</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-orange-500" data-testid="text-discharge-ops">
                {stats.dischargeOperations}
              </div>
            </CardContent>
          </Card>

          <Card data-testid="card-transship-ops">
            <CardHeader className="pb-2">
              <CardTitle className="text-sm font-medium text-muted-foreground">Transshipment</CardTitle>
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-purple-500" data-testid="text-transship-ops">
                {stats.transshipmentOperations}
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Filters and Content */}
        <Tabs defaultValue="operations" className="space-y-4">
          <div className="flex items-center justify-between">
            <TabsList>
              <TabsTrigger value="operations" data-testid="tab-operations">Operations</TabsTrigger>
              <TabsTrigger value="analytics" data-testid="tab-analytics">Analytics</TabsTrigger>
            </TabsList>

            <div className="flex items-center gap-3">
              <Select value={selectedOperationType} onValueChange={setSelectedOperationType}>
                <SelectTrigger className="w-40" data-testid="select-operation-type">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Types</SelectItem>
                  <SelectItem value="load">Load</SelectItem>
                  <SelectItem value="discharge">Discharge</SelectItem>
                  <SelectItem value="transshipment">Transshipment</SelectItem>
                </SelectContent>
              </Select>

              <Select value={selectedPort} onValueChange={setSelectedPort}>
                <SelectTrigger className="w-48" data-testid="select-port-filter">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All Ports</SelectItem>
                  {ports.map(port => (
                    <SelectItem key={port.id} value={port.id}>{port.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <TabsContent value="operations" className="space-y-4">
            {isLoading ? (
              <div className="space-y-3">
                {[...Array(5)].map((_, i) => (
                  <Skeleton key={i} className="h-32 w-full" />
                ))}
              </div>
            ) : filteredOperations.length === 0 ? (
              <Card>
                <CardContent className="py-12 text-center text-muted-foreground">
                  <Container className="w-12 h-12 mx-auto mb-3 opacity-50" />
                  <p>No container operations found</p>
                  <p className="text-sm mt-1">Container operation data will appear here</p>
                </CardContent>
              </Card>
            ) : (
              <div className="space-y-3">
                {filteredOperations.map((operation) => (
                  <Card 
                    key={operation.id}
                    className="hover:border-primary transition-colors"
                    data-testid={`card-operation-${operation.id}`}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-start justify-between">
                        <div className="flex-1 space-y-3">
                          <div className="flex items-center gap-3">
                            <Ship className="w-5 h-5 text-primary" />
                            <div className="flex-1">
                              <div className="flex items-center gap-3 mb-1">
                                <h3 className="font-semibold" data-testid={`text-vessel-${operation.id}`}>
                                  {operation.vessel?.name || `Vessel ${operation.vesselId}`}
                                </h3>
                                <Badge variant={getOperationColor(operation.operationType)} data-testid={`badge-type-${operation.id}`}>
                                  {operation.operationType}
                                </Badge>
                              </div>
                              <p className="text-sm text-muted-foreground">
                                {operation.port?.name || `Port ${operation.portId}`}
                              </p>
                            </div>
                          </div>

                          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                            <div>
                              <p className="text-xs text-muted-foreground mb-1">TEU Count</p>
                              <div className="flex items-center gap-2">
                                <Package className="w-4 h-4 text-blue-500" />
                                <span className="text-lg font-bold text-blue-500" data-testid={`text-teu-${operation.id}`}>
                                  {operation.teuCount}
                                </span>
                              </div>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">FEU Count</p>
                              <div className="flex items-center gap-2">
                                <Package className="w-4 h-4 text-green-500" />
                                <span className="text-lg font-bold text-green-500" data-testid={`text-feu-${operation.id}`}>
                                  {operation.feuCount}
                                </span>
                              </div>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Operation Date</p>
                              <span className="text-sm" data-testid={`text-date-${operation.id}`}>
                                {operation.operationDate ? new Date(operation.operationDate).toLocaleDateString() : 'N/A'}
                              </span>
                            </div>

                            <div>
                              <p className="text-xs text-muted-foreground mb-1">Shipping Line</p>
                              <span className="text-sm" data-testid={`text-shipping-line-${operation.id}`}>
                                {operation.shippingLine || 'N/A'}
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </TabsContent>

          <TabsContent value="analytics" className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Operation Distribution</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Load Operations</span>
                      <span className="text-sm font-medium">{stats.loadOperations}</span>
                    </div>
                    <div className="bg-secondary h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-cyan-500"
                        style={{ width: `${(stats.loadOperations / Math.max(stats.totalOperations, 1)) * 100}%` }}
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Discharge Operations</span>
                      <span className="text-sm font-medium">{stats.dischargeOperations}</span>
                    </div>
                    <div className="bg-secondary h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-orange-500"
                        style={{ width: `${(stats.dischargeOperations / Math.max(stats.totalOperations, 1)) * 100}%` }}
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Transshipment</span>
                      <span className="text-sm font-medium">{stats.transshipmentOperations}</span>
                    </div>
                    <div className="bg-secondary h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-purple-500"
                        style={{ width: `${(stats.transshipmentOperations / Math.max(stats.totalOperations, 1)) * 100}%` }}
                      />
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card>
                <CardHeader>
                  <CardTitle className="text-base">TEU vs FEU</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Twenty-foot Equivalent (TEU)</span>
                      <span className="text-sm font-medium text-blue-500">{stats.totalTEU}</span>
                    </div>
                    <div className="bg-secondary h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-blue-500"
                        style={{ width: `${(stats.totalTEU / Math.max(stats.totalTEU + stats.totalFEU, 1)) * 100}%` }}
                      />
                    </div>
                  </div>

                  <div className="space-y-2">
                    <div className="flex items-center justify-between">
                      <span className="text-sm">Forty-foot Equivalent (FEU)</span>
                      <span className="text-sm font-medium text-green-500">{stats.totalFEU}</span>
                    </div>
                    <div className="bg-secondary h-2 rounded-full overflow-hidden">
                      <div 
                        className="h-full bg-green-500"
                        style={{ width: `${(stats.totalFEU / Math.max(stats.totalTEU + stats.totalFEU, 1)) * 100}%` }}
                      />
                    </div>
                  </div>

                  <div className="pt-3 border-t border-border">
                    <div className="flex items-center justify-between">
                      <span className="text-sm font-medium">Total Capacity (TEU Equivalent)</span>
                      <span className="text-lg font-bold">{(stats.totalTEU + stats.totalFEU * 2).toLocaleString()}</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </div>

            <Card>
              <CardHeader>
                <CardTitle className="text-base">Insights</CardTitle>
              </CardHeader>
              <CardContent>
                <ul className="space-y-2 text-sm">
                  <li className="flex items-start gap-2">
                    <BarChart3 className="w-4 h-4 text-primary mt-0.5" />
                    <span>{stats.totalOperations} total container operations tracked</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <TrendingUp className="w-4 h-4 text-blue-500 mt-0.5" />
                    <span>Average of {(stats.totalTEU / Math.max(stats.totalOperations, 1)).toFixed(1)} TEU per operation</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <Container className="w-4 h-4 text-green-500 mt-0.5" />
                    <span>Total capacity equivalent to {(stats.totalTEU + stats.totalFEU * 2).toLocaleString()} TEU</span>
                  </li>
                </ul>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
}
