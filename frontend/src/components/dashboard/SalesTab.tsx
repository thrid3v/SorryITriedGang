import { useState, useEffect } from "react";
import { IndianRupee, ShoppingCart, TrendingUp, CreditCard } from "lucide-react";
import KPICard from "./KPICard";
import { GlassChart, GlassBarChart, GlassLineChart } from "./Charts";
import { Button } from "@/components/ui/button";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { cn } from "@/lib/utils";
import {
  fetchSummaryKPIs, fetchRevenueTimeseries, fetchCitySales, fetchTopProducts,
  type SummaryKPIs, type RevenuePoint, type CitySale, type TopProduct,
} from "@/data/api";

const formatCurrency = (n: number) => {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};

const SalesTab = () => {
  const [period, setPeriod] = useState<"daily" | "monthly">("daily");
  const [kpis, setKpis] = useState<SummaryKPIs | null>(null);
  const [revenue, setRevenue] = useState<RevenuePoint[]>([]);
  const [cities, setCities] = useState<CitySale[]>([]);
  const [products, setProducts] = useState<TopProduct[]>([]);

  // Initial data fetch
  useEffect(() => {
    fetchSummaryKPIs().then(setKpis).catch(console.error);
    fetchCitySales().then(setCities).catch(console.error);
    fetchTopProducts(10).then(setProducts).catch(console.error);
  }, []);

  // Fetch revenue when period changes
  useEffect(() => {
    fetchRevenueTimeseries(period).then(setRevenue).catch(console.error);
  }, [period]);

  // Auto-refresh when stream is running
  useEffect(() => {
    const refreshData = async () => {
      try {
        const statusRes = await fetch("/api/stream/status");
        if (statusRes.ok) {
          const status = await statusRes.json();
          if (status.status === "running") {
            // Refresh all data
            fetchSummaryKPIs().then(setKpis).catch(console.error);
            fetchRevenueTimeseries(period).then(setRevenue).catch(console.error);
            fetchCitySales().then(setCities).catch(console.error);
            fetchTopProducts(10).then(setProducts).catch(console.error);
          }
        }
      } catch (err) {
        console.error("Failed to check stream status:", err);
      }
    };

    const interval = setInterval(refreshData, 10000); // Refresh every 10 seconds
    return () => clearInterval(interval);
  }, [period]);

  const revenueData = revenue.map((r) => ({
    label: period === "daily"
      ? (r.full_date ? new Date(r.full_date).toLocaleDateString("en-US", { month: "short", day: "numeric" }) : "")
      : `Month ${r.month}`,
    revenue: r.revenue,
  }));

  const cityData = cities.map((c) => ({ city: c.city, revenue: c.total_revenue }));

  return (
    <div className="space-y-8">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
        <KPICard title="Total Revenue" value={kpis ? formatCurrency(kpis.total_revenue) : "..."} icon={<IndianRupee className="h-4 w-4" />} />
        <KPICard title="Total Orders" value={kpis ? kpis.total_orders.toLocaleString() : "..."} icon={<ShoppingCart className="h-4 w-4" />} />
        <KPICard title="Active Users" value={kpis ? kpis.active_users.toLocaleString() : "..."} icon={<TrendingUp className="h-4 w-4" />} />
        <KPICard title="Avg Order Value" value={kpis ? formatCurrency(kpis.total_revenue / kpis.total_orders) : "..."} icon={<CreditCard className="h-4 w-4" />} />
      </div>

      <div className="grid lg:grid-cols-2 gap-8">
        <GlassChart
          title="Revenue Trends"
          actions={
            <div className="flex gap-2">
              <Button 
                size="sm" 
                variant={period === "daily" ? "default" : "ghost"} 
                onClick={() => setPeriod("daily")} 
                className={cn(
                  "text-xs h-8 rounded-pill transition-all",
                  period === "daily" 
                    ? "bg-acid-lime text-deep-charcoal hover:bg-acid-lime/90 shadow-[0_0_15px_rgba(212,255,0,0.3)]" 
                    : "hover:bg-acid-lime/10 hover:text-acid-lime"
                )}
              >
                Daily
              </Button>
              <Button 
                size="sm" 
                variant={period === "monthly" ? "default" : "ghost"} 
                onClick={() => setPeriod("monthly")} 
                className={cn(
                  "text-xs h-8 rounded-pill transition-all",
                  period === "monthly" 
                    ? "bg-acid-lime text-deep-charcoal hover:bg-acid-lime/90 shadow-[0_0_15px_rgba(212,255,0,0.3)]" 
                    : "hover:bg-acid-lime/10 hover:text-acid-lime"
                )}
              >
                Monthly
              </Button>
            </div>
          }
        >
          <GlassLineChart data={revenueData} lines={[{ key: "revenue", color: "#D4FF00" }]} xKey="label" />
        </GlassChart>

        <GlassChart title="City-Wise Sales (Top 10)">
          <GlassBarChart data={cityData} dataKey="revenue" xKey="city" color="#D4FF00" />
        </GlassChart>
      </div>

      <GlassChart title="Top Selling Products">
        <div className="overflow-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-border/30">
                <TableHead>Product</TableHead>
                <TableHead>Category</TableHead>
                <TableHead className="text-right">Revenue</TableHead>
                <TableHead className="text-right">Units</TableHead>
                <TableHead className="text-right">Avg Price</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {products.map((p) => (
                <TableRow key={p.product_name} className="border-border/20">
                  <TableCell className="font-medium">{p.product_name}</TableCell>
                  <TableCell><span className="px-2.5 py-1 text-xs rounded-pill bg-acid-lime/10 text-acid-lime">{p.category}</span></TableCell>
                  <TableCell className="text-right">{formatCurrency(p.total_revenue)}</TableCell>
                  <TableCell className="text-right">{p.units_sold.toLocaleString()}</TableCell>
                  <TableCell className="text-right">${p.avg_sale_price.toFixed(2)}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </GlassChart>
    </div>
  );
};

export default SalesTab;
