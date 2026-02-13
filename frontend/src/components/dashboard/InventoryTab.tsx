import { useState, useEffect } from "react";
import { Package, AlertTriangle, TrendingDown, RefreshCw } from "lucide-react";
import KPICard from "./KPICard";
import { GlassChart, GlassBarChart, GlassLineChart } from "./Charts";
import {
  fetchInventoryTurnover, fetchSeasonalTrends,
  type InventoryItem, type SeasonalTrend,
} from "@/data/api";

const InventoryTab = () => {
  const [inventory, setInventory] = useState<InventoryItem[]>([]);
  const [seasonal, setSeasonal] = useState<SeasonalTrend[]>([]);

  // Initial data fetch
  useEffect(() => {
    fetchInventoryTurnover().then(setInventory).catch(console.error);
    fetchSeasonalTrends().then(setSeasonal).catch(console.error);
  }, []);

  // Auto-refresh when stream is running
  useEffect(() => {
    const refreshData = async () => {
      try {
        const statusRes = await fetch("/api/stream/status");
        if (statusRes.ok) {
          const status = await statusRes.json();
          if (status.status === "running") {
            fetchInventoryTurnover().then(setInventory).catch(console.error);
            fetchSeasonalTrends().then(setSeasonal).catch(console.error);
          }
        }
      } catch (err) {
        console.error("Failed to check stream status:", err);
      }
    };

    const interval = setInterval(refreshData, 10000);
    return () => clearInterval(interval);
  }, []);

  // Compute KPIs from inventory data
  const avgTurnover = inventory.length
    ? (inventory.reduce((s, i) => s + i.turnover_ratio, 0) / inventory.length).toFixed(2)
    : "...";
  const lowStockCount = inventory.filter((i) => i.avg_stock < 30).length;
  const highTurnover = inventory.filter((i) => i.turnover_ratio > 0.8).length;
  const needsReorder = inventory.filter((i) => i.reorder_instances && i.reorder_instances > 0).length;

  // Stock by category chart
  const categoryStock: Record<string, { inStock: number; count: number }> = {};
  inventory.forEach((i) => {
    if (!categoryStock[i.category]) categoryStock[i.category] = { inStock: 0, count: 0 };
    categoryStock[i.category].inStock += i.avg_stock;
    categoryStock[i.category].count++;
  });
  const stockByCategory = Object.entries(categoryStock).map(([category, d]) => ({
    category,
    inStock: Math.round(d.inStock),
  }));

  // Seasonal demand by category for chart (pivot by month)
  const months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const categories = [...new Set(seasonal.map((s) => s.category))];
  const monthlyMap: Record<number, Record<string, number>> = {};
  seasonal.forEach((s) => {
    if (!monthlyMap[s.month]) monthlyMap[s.month] = {};
    monthlyMap[s.month][s.category] = s.units_sold;
  });
  const seasonalChartData = Object.entries(monthlyMap)
    .sort(([a], [b]) => Number(a) - Number(b))
    .map(([month, cats]) => ({
      month: months[Number(month) - 1] || `M${month}`,
      ...cats,
    }));

  const categoryColors = [
    "hsl(217, 91%, 60%)", "hsl(270, 60%, 55%)", "hsl(175, 70%, 45%)",
    "hsl(45, 90%, 55%)", "hsl(0, 84%, 60%)",
  ];

  // Reorder alerts (low-stock items)
  const reorderAlerts = inventory
    .filter((i) => i.avg_stock < 30)
    .sort((a, b) => a.avg_stock - b.avg_stock)
    .slice(0, 6);

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Avg Turnover" value={avgTurnover} icon={<RefreshCw className="h-4 w-4" />} />
        <KPICard title="Low Stock Items" value={lowStockCount} icon={<AlertTriangle className="h-4 w-4" />} />
        <KPICard title="High Turnover" value={`${highTurnover} items`} icon={<Package className="h-4 w-4" />} subtitle="> 0.8 ratio" />
        <KPICard title="Reorder Alerts" value={needsReorder} icon={<TrendingDown className="h-4 w-4" />} subtitle="items need action" />
      </div>

      <div className="grid lg:grid-cols-2 gap-6">
        <GlassChart title="Stock Levels by Category">
          <GlassBarChart data={stockByCategory} dataKey="inStock" xKey="category" color="hsl(175, 70%, 45%)" />
        </GlassChart>

        <GlassChart title="Seasonal Demand Trends">
          <GlassLineChart
            data={seasonalChartData}
            xKey="month"
            lines={categories.map((cat, i) => ({
              key: cat,
              color: categoryColors[i % categoryColors.length],
              name: cat,
            }))}
          />
        </GlassChart>
      </div>

      <GlassChart title="🚨 Low Stock Items">
        <div className="space-y-3">
          {reorderAlerts.length === 0 && (
            <div className="text-center text-muted-foreground text-sm py-4">All items adequately stocked</div>
          )}
          {reorderAlerts.map((a) => (
            <div key={a.product_name} className="flex items-center justify-between p-3 rounded-lg bg-destructive/5 border border-destructive/20">
              <div>
                <div className="font-medium text-sm">{a.product_name}</div>
                <div className="text-xs text-muted-foreground">{a.category}</div>
              </div>
              <div className="flex items-center gap-6 text-sm">
                <div className="text-right">
                  <div className="font-semibold text-destructive">{Math.round(a.avg_stock)} avg stock</div>
                  <div className="text-xs text-muted-foreground">Turnover: {a.turnover_ratio.toFixed(2)}</div>
                </div>
                <div className="text-xs px-2 py-1 rounded bg-destructive/10 text-destructive font-medium">
                  {a.units_sold} sold
                </div>
              </div>
            </div>
          ))}
        </div>
      </GlassChart>
    </div>
  );
};

export default InventoryTab;
