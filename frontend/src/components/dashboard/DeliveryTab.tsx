import { useState, useEffect } from "react";
import { Truck, Clock, CheckCircle, AlertCircle } from "lucide-react";
import KPICard from "./KPICard";
import { GlassChart, GlassBarChart } from "./Charts";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { fetchDeliveryMetrics, type DeliveryMetric } from "@/data/api";
import { cn } from "@/lib/utils";

const DeliveryTab = () => {
  const [metrics, setMetrics] = useState<DeliveryMetric[]>([]);

  // Initial data fetch
  useEffect(() => {
    fetchDeliveryMetrics().then(setMetrics).catch(console.error);
  }, []);

  // Auto-refresh when stream is running
  useEffect(() => {
    const refreshData = async () => {
      try {
        const statusRes = await fetch("/api/stream/status");
        if (statusRes.ok) {
          const status = await statusRes.json();
          if (status.status === "running") {
            fetchDeliveryMetrics().then(setMetrics).catch(console.error);
          }
        }
      } catch (err) {
        console.error("Failed to check stream status:", err);
      }
    };

    const interval = setInterval(refreshData, 10000);
    return () => clearInterval(interval);
  }, []);

  const totalShipments = metrics.reduce((s, m) => s + m.shipment_count, 0);
  const avgDelivery = metrics.length
    ? (metrics.reduce((s, m) => s + m.avg_delivery_days * m.shipment_count, 0) / (totalShipments || 1)).toFixed(1)
    : "...";
  const totalFast = metrics.reduce((s, m) => s + (m.fast_deliveries || 0), 0);
  const totalDelayed = metrics.reduce((s, m) => s + (m.delayed_deliveries || 0), 0);
  const onTimePct = totalShipments
    ? (((totalShipments - totalDelayed) / totalShipments) * 100).toFixed(1)
    : "...";

  // Delivery bottlenecks by region 
  const regionMap: Record<string, { delayed: number; total: number }> = {};
  metrics.forEach((m) => {
    if (!regionMap[m.origin_region]) regionMap[m.origin_region] = { delayed: 0, total: 0 };
    regionMap[m.origin_region].delayed += m.delayed_deliveries || 0;
    regionMap[m.origin_region].total += m.shipment_count;
  });
  const bottlenecks = Object.entries(regionMap).map(([region, d]) => ({
    region,
    pct: d.total ? +((d.delayed / d.total) * 100).toFixed(1) : 0,
  }));

  // Carrier performance chart
  const carrierMap: Record<string, { total: number; avgDays: number; count: number }> = {};
  metrics.forEach((m) => {
    if (!carrierMap[m.carrier]) carrierMap[m.carrier] = { total: 0, avgDays: 0, count: 0 };
    carrierMap[m.carrier].avgDays += m.avg_delivery_days * m.shipment_count;
    carrierMap[m.carrier].total += m.shipment_count;
    carrierMap[m.carrier].count++;
  });
  const carrierPerf = Object.entries(carrierMap).map(([carrier, d]) => ({
    carrier,
    avgDays: +(d.avgDays / (d.total || 1)).toFixed(1),
  }));

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Avg Delivery Time" value={`${avgDelivery} days`} icon={<Clock className="h-4 w-4" />} />
        <KPICard title="On-Time Delivery" value={`${onTimePct}%`} icon={<CheckCircle className="h-4 w-4" />} />
        <KPICard title="Total Shipments" value={totalShipments} icon={<Truck className="h-4 w-4" />} />
        <KPICard title="Delayed Orders" value={totalDelayed} icon={<AlertCircle className="h-4 w-4" />} />
      </div>

      <div className="grid lg:grid-cols-2 gap-6">
        <GlassChart title="Carrier Performance (Avg Days)">
          <GlassBarChart data={carrierPerf} dataKey="avgDays" xKey="carrier" color="hsl(175, 70%, 45%)" />
        </GlassChart>

        <GlassChart title="Delivery Bottlenecks by Region">
          <GlassBarChart data={bottlenecks} dataKey="pct" xKey="region" color="hsl(0, 84%, 60%)" />
        </GlassChart>
      </div>

      <GlassChart title="Shipment Details">
        <div className="overflow-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-border/50">
                <TableHead>Carrier</TableHead>
                <TableHead>Region</TableHead>
                <TableHead className="text-right">Shipments</TableHead>
                <TableHead className="text-right">Avg Days</TableHead>
                <TableHead className="text-right">Avg Cost</TableHead>
                <TableHead>Status</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {metrics.map((m, i) => (
                <TableRow key={i} className="border-border/30">
                  <TableCell className="font-medium text-sm">{m.carrier}</TableCell>
                  <TableCell className="text-sm">{m.origin_region}</TableCell>
                  <TableCell className="text-right">{m.shipment_count}</TableCell>
                  <TableCell className="text-right">{m.avg_delivery_days.toFixed(1)}</TableCell>
                  <TableCell className="text-right">${m.avg_shipping_cost.toFixed(2)}</TableCell>
                  <TableCell>
                    <span className={cn(
                      "px-2 py-1 rounded text-xs font-medium",
                      (m.delayed_deliveries || 0) > 0
                        ? "text-destructive bg-destructive/10"
                        : "text-glow-teal bg-glow-teal/10"
                    )}>
                      {(m.delayed_deliveries || 0) > 0 ? "Has Delays" : "On Track"}
                    </span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </GlassChart>
    </div>
  );
};

export default DeliveryTab;
