import { useState, useEffect } from "react";
import { Users, Heart, UserPlus, Repeat } from "lucide-react";
import KPICard from "./KPICard";
import { GlassChart, GlassBarChart, GlassDonutChart } from "./Charts";
import {
  fetchCustomerSegmentation, fetchCLV, fetchMarketBasket,
  type CustomerSegment, type CLVRecord, type BasketPair,
} from "@/data/api";

const formatCurrency = (n: number) => {
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
};

const CustomerTab = () => {
  const [segments, setSegments] = useState<CustomerSegment[]>([]);
  const [clv, setCLV] = useState<CLVRecord[]>([]);
  const [basket, setBasket] = useState<BasketPair[]>([]);

  // Initial data fetch
  useEffect(() => {
    fetchCustomerSegmentation().then(setSegments).catch(console.error);
    fetchCLV().then(setCLV).catch(console.error);
    fetchMarketBasket().then(setBasket).catch(console.error);
  }, []);

  // Auto-refresh when stream is running
  useEffect(() => {
    const refreshData = async () => {
      try {
        const statusRes = await fetch("/api/stream/status");
        if (statusRes.ok) {
          const status = await statusRes.json();
          if (status.status === "running") {
            fetchCustomerSegmentation().then(setSegments).catch(console.error);
            fetchCLV().then(setCLV).catch(console.error);
            fetchMarketBasket().then(setBasket).catch(console.error);
          }
        }
      } catch (err) {
        console.error("Failed to check stream status:", err);
      }
    };

    const interval = setInterval(refreshData, 10000);
    return () => clearInterval(interval);
  }, []);

  const newSeg = segments.find((s) => s.customer_type === "New");
  const retSeg = segments.find((s) => s.customer_type === "Returning");
  const totalCustomers = segments.reduce((s, seg) => s + seg.customer_count, 0);
  const avgCLV = clv.length ? clv.reduce((s, c) => s + c.estimated_clv, 0) / clv.length : 0;

  const newPct = newSeg && retSeg ? Math.round((newSeg.order_count / (newSeg.order_count + retSeg.order_count)) * 100) : 0;
  const retPct = 100 - newPct;

  const donutData = [
    { name: "New Customers", value: newPct, fill: "hsl(217, 91%, 60%)" },
    { name: "Returning", value: retPct, fill: "hsl(175, 70%, 45%)" },
  ];

  // Top CLV customers for bar chart
  const topCLV = clv.slice(0, 8).map((c) => ({
    segment: c.customer_name.split(" ")[0],
    clv: Math.round(c.estimated_clv),
  }));

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Total Customers" value={totalCustomers.toLocaleString()} icon={<Users className="h-4 w-4" />} />
        <KPICard title="Avg CLV" value={formatCurrency(avgCLV)} icon={<Heart className="h-4 w-4" />} />
        <KPICard title="New : Returning" value={`${newPct}:${retPct}`} icon={<UserPlus className="h-4 w-4" />} />
        <KPICard
          title="Retention Revenue"
          value={retSeg ? formatCurrency(retSeg.total_revenue) : "..."}
          icon={<Repeat className="h-4 w-4" />}
          subtitle={retSeg ? `${retPct}% of total` : ""}
        />
      </div>

      <div className="grid lg:grid-cols-2 gap-6">
        <GlassChart title="New vs Returning Customers">
          <GlassDonutChart data={donutData} />
        </GlassChart>

        <GlassChart title="Top Customer Lifetime Value">
          <GlassBarChart data={topCLV} dataKey="clv" xKey="segment" color="hsl(270, 60%, 55%)" />
        </GlassChart>
      </div>

      <GlassChart title="🛒 Market Basket Analysis — Frequently Co-Purchased">
        <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-4">
          {basket.slice(0, 6).map((pair) => (
            <div key={`${pair.product_a}-${pair.product_b}`} className="glass-card-hover p-4 text-center">
              <div className="flex items-center justify-center gap-3 mb-3">
                <span className="px-3 py-1 rounded-full bg-primary/10 text-primary text-sm font-medium">{pair.product_a_name}</span>
                <span className="text-muted-foreground text-xs">+</span>
                <span className="px-3 py-1 rounded-full bg-secondary/10 text-secondary text-sm font-medium">{pair.product_b_name}</span>
              </div>
              <div className="text-xs text-muted-foreground">
                Bought together: <span className="text-foreground font-medium">{pair.times_bought_together} times</span>
              </div>
            </div>
          ))}
        </div>
      </GlassChart>
    </div>
  );
};

export default CustomerTab;
