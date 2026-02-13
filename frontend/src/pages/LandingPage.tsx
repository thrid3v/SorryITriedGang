import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { ArrowRight, Database, Shield, BarChart3, Workflow, Store, ShoppingCart, Warehouse, TrendingUp, Users, Truck, Activity } from "lucide-react";
import { useCountUp, formatCompact } from "@/hooks/useCountUp";

const features = [
  { icon: Workflow, title: "Data Ingestion", desc: "Batch & real-time pipelines from POS, e-commerce, and warehouse systems with auto-retry and schema evolution.", color: "text-glow-blue" },
  { icon: Database, title: "Transformation", desc: "Automated cleaning, deduplication, SCD tracking, and star-schema modeling for analytics-ready datasets.", color: "text-glow-purple" },
  { icon: Shield, title: "Storage & Security", desc: "Optimized Parquet/Delta storage with partitioning strategies and role-based access controls.", color: "text-glow-teal" },
  { icon: BarChart3, title: "Analytics & AI", desc: "Real-time KPIs, market basket analysis, CLV scoring, and demand forecasting dashboards.", color: "text-glow-blue" },
];

const archSteps = [
  { icon: Store, label: "POS Systems", sub: "50+ Stores" },
  { icon: ShoppingCart, label: "E-Commerce", sub: "Web & App" },
  { icon: Warehouse, label: "Warehouses", sub: "5 Regions" },
];

const kpis = [
  { label: "Stores Connected", value: 50, suffix: "+" },
  { label: "Monthly Transactions", value: 1000000, suffix: "+", format: true },
  { label: "Products Tracked", value: 25000, suffix: "+", format: true },
  { label: "Uptime SLA", value: 99.9, suffix: "%", decimal: true },
];

const KPICounter = ({ label, value, suffix, format, decimal }: { label: string; value: number; suffix: string; format?: boolean; decimal?: boolean }) => {
  const { count } = useCountUp(decimal ? Math.floor(value * 10) : value, 2500);
  const display = format ? formatCompact(count) : decimal ? (count / 10).toFixed(1) : count;
  return (
    <div className="glass-card p-6 text-center animate-fade-in">
      <div className="text-3xl md:text-4xl font-bold gradient-text mb-2">
        {display}{!format && suffix}
      </div>
      <div className="text-muted-foreground text-sm">{label}</div>
    </div>
  );
};

const LandingPage = () => {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-background overflow-hidden">
      {/* Ambient glow background */}
      <div className="fixed inset-0 pointer-events-none">
        <div className="absolute top-[-20%] left-[-10%] w-[500px] h-[500px] rounded-full bg-glow-blue/5 blur-[120px]" />
        <div className="absolute bottom-[-20%] right-[-10%] w-[600px] h-[600px] rounded-full bg-glow-purple/5 blur-[120px]" />
        <div className="absolute top-[40%] right-[20%] w-[300px] h-[300px] rounded-full bg-glow-teal/5 blur-[100px]" />
      </div>

      {/* Navigation */}
      <nav className="relative z-10 flex items-center justify-between px-6 md:px-12 py-4 border-b border-border/50">
        <div className="flex items-center gap-2">
          <Activity className="h-7 w-7 text-primary" />
          <span className="text-xl font-bold">RetailNexus</span>
        </div>
        <Button onClick={() => navigate("/dashboard")} variant="outline" className="border-primary/30 hover:bg-primary/10">
          Open Dashboard <ArrowRight className="ml-1 h-4 w-4" />
        </Button>
      </nav>

      {/* Hero */}
      <section className="relative z-10 flex flex-col items-center text-center px-6 pt-20 pb-16 max-w-5xl mx-auto">
        <div className="inline-flex items-center gap-2 glass-card px-4 py-1.5 text-xs text-muted-foreground mb-8 animate-fade-in">
          <span className="h-2 w-2 rounded-full bg-glow-teal animate-pulse_glow" />
          Smart Retail Intelligence Platform
        </div>
        <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold leading-tight mb-6 animate-fade-in-up" style={{ animationDelay: "0.1s" }}>
          Turn Retail Data Into{" "}
          <span className="gradient-text">Actionable Intelligence</span>
        </h1>
        <p className="text-lg md:text-xl text-muted-foreground max-w-2xl mb-10 animate-fade-in-up" style={{ animationDelay: "0.2s" }}>
          Unify POS, e-commerce, and warehouse data into a single hub. Get real-time analytics on sales, inventory, customer behavior, and logistics.
        </p>
        <div className="flex gap-4 animate-fade-in-up" style={{ animationDelay: "0.3s" }}>
          <Button size="lg" onClick={() => navigate("/dashboard")} className="bg-primary hover:bg-primary/90 glow-blue text-primary-foreground px-8">
            Explore Dashboard <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
          <Button size="lg" variant="outline" className="border-border hover:bg-muted">
            View Architecture
          </Button>
        </div>
      </section>

      {/* Features */}
      <section className="relative z-10 max-w-6xl mx-auto px-6 py-16">
        <h2 className="text-3xl font-bold text-center mb-4">The Four Pillars</h2>
        <p className="text-center text-muted-foreground mb-12 max-w-xl mx-auto">End-to-end data infrastructure powering intelligent retail decisions.</p>
        <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
          {features.map((f, i) => (
            <div key={f.title} className="glass-card-hover p-6 animate-fade-in-up" style={{ animationDelay: `${i * 0.1}s` }}>
              <f.icon className={`h-10 w-10 mb-4 ${f.color}`} />
              <h3 className="text-lg font-semibold mb-2">{f.title}</h3>
              <p className="text-sm text-muted-foreground leading-relaxed">{f.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Architecture Diagram */}
      <section className="relative z-10 max-w-5xl mx-auto px-6 py-16">
        <h2 className="text-3xl font-bold text-center mb-12">Data Architecture</h2>
        <div className="glass-card p-8 md:p-12">
          <div className="flex flex-col md:flex-row items-center justify-between gap-8">
            {/* Sources */}
            <div className="flex flex-col gap-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider text-center mb-2">Data Sources</div>
              {archSteps.map((s, i) => (
                <div key={s.label} className="glass-card-hover p-4 flex items-center gap-3 animate-slide-in-left" style={{ animationDelay: `${i * 0.15}s` }}>
                  <s.icon className="h-6 w-6 text-glow-blue" />
                  <div>
                    <div className="text-sm font-medium">{s.label}</div>
                    <div className="text-xs text-muted-foreground">{s.sub}</div>
                  </div>
                </div>
              ))}
            </div>

            {/* Arrow */}
            <div className="flex flex-col items-center gap-2">
              <div className="hidden md:block w-24 h-px bg-gradient-to-r from-glow-blue to-glow-purple" />
              <div className="md:hidden h-12 w-px bg-gradient-to-b from-glow-blue to-glow-purple" />
              <span className="text-xs text-muted-foreground">ETL Pipeline</span>
            </div>

            {/* Hub */}
            <div className="glass-card p-6 border-primary/30 glow-blue text-center animate-fade-in" style={{ animationDelay: "0.4s" }}>
              <Database className="h-10 w-10 text-primary mx-auto mb-3" />
              <div className="font-semibold">Retail Data Hub</div>
              <div className="text-xs text-muted-foreground mt-1">Star Schema • Delta Lake</div>
            </div>

            {/* Arrow */}
            <div className="flex flex-col items-center gap-2">
              <div className="hidden md:block w-24 h-px bg-gradient-to-r from-glow-purple to-glow-teal" />
              <div className="md:hidden h-12 w-px bg-gradient-to-b from-glow-purple to-glow-teal" />
              <span className="text-xs text-muted-foreground">Analytics</span>
            </div>

            {/* Outputs */}
            <div className="flex flex-col gap-4">
              <div className="text-xs text-muted-foreground uppercase tracking-wider text-center mb-2">Insights</div>
              {[
                { icon: TrendingUp, label: "Sales", color: "text-glow-blue" },
                { icon: Users, label: "Customers", color: "text-glow-purple" },
                { icon: Truck, label: "Logistics", color: "text-glow-teal" },
              ].map((o, i) => (
                <div key={o.label} className="glass-card-hover p-4 flex items-center gap-3 animate-fade-in" style={{ animationDelay: `${0.5 + i * 0.1}s` }}>
                  <o.icon className={`h-6 w-6 ${o.color}`} />
                  <span className="text-sm font-medium">{o.label}</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </section>

      {/* KPI Highlights */}
      <section className="relative z-10 max-w-5xl mx-auto px-6 py-16">
        <h2 className="text-3xl font-bold text-center mb-12">Platform at a Glance</h2>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
          {kpis.map((k) => (
            <KPICounter key={k.label} {...k} />
          ))}
        </div>
      </section>

      {/* Final CTA */}
      <section className="relative z-10 max-w-3xl mx-auto px-6 py-20 text-center">
        <div className="glass-card p-12 glow-purple">
          <h2 className="text-3xl font-bold mb-4">Ready to Explore?</h2>
          <p className="text-muted-foreground mb-8">Dive into the live analytics dashboard with realistic mock data from 50+ Indian retail stores.</p>
          <Button size="lg" onClick={() => navigate("/dashboard")} className="bg-primary hover:bg-primary/90 px-10">
            Launch Dashboard <ArrowRight className="ml-2 h-4 w-4" />
          </Button>
        </div>
      </section>

      <footer className="relative z-10 border-t border-border/50 py-6 text-center text-xs text-muted-foreground">
        RetailNexus © 2026 — Smart Retail Supply Chain & Customer Intelligence Platform
      </footer>
    </div>
  );
};

export default LandingPage;
