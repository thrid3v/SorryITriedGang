import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { TrendingUp, Package, Users, Truck, Shield, Settings, Activity, ArrowLeft } from "lucide-react";
import { cn } from "@/lib/utils";
import SalesTab from "@/components/dashboard/SalesTab";
import InventoryTab from "@/components/dashboard/InventoryTab";
import CustomerTab from "@/components/dashboard/CustomerTab";
import DeliveryTab from "@/components/dashboard/DeliveryTab";
import DataQualityTab from "@/components/dashboard/DataQualityTab";

const tabs = [
  { id: "sales", label: "Sales Analytics", icon: TrendingUp },
  { id: "inventory", label: "Inventory & Ops", icon: Package },
  { id: "customer", label: "Customer Intel", icon: Users },
  { id: "delivery", label: "Delivery & Logistics", icon: Truck },
  { id: "quality", label: "Data Quality", icon: Shield },
  { id: "settings", label: "Settings", icon: Settings },
];

const Dashboard = () => {
  const [activeTab, setActiveTab] = useState("sales");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const navigate = useNavigate();

  const renderTab = () => {
    switch (activeTab) {
      case "sales": return <SalesTab />;
      case "inventory": return <InventoryTab />;
      case "customer": return <CustomerTab />;
      case "delivery": return <DeliveryTab />;
      case "quality": return <DataQualityTab />;
      case "settings": return (
        <div className="glass-card p-12 text-center">
          <Settings className="h-12 w-12 text-muted-foreground mx-auto mb-4" />
          <h3 className="text-xl font-semibold mb-2">Settings</h3>
          <p className="text-muted-foreground">Configuration options coming soon.</p>
        </div>
      );
      default: return <SalesTab />;
    }
  };

  return (
    <div className="min-h-screen bg-background flex">
      {/* Ambient glow */}
      <div className="fixed inset-0 pointer-events-none">
        <div className="absolute top-[-10%] right-[-10%] w-[400px] h-[400px] rounded-full bg-glow-blue/3 blur-[100px]" />
        <div className="absolute bottom-[-10%] left-[-10%] w-[400px] h-[400px] rounded-full bg-glow-purple/3 blur-[100px]" />
      </div>

      {/* Sidebar */}
      <aside className={cn(
        "relative z-10 h-screen sticky top-0 border-r border-border/50 bg-[hsl(var(--sidebar-background))] transition-all duration-300 flex flex-col",
        sidebarCollapsed ? "w-16" : "w-60"
      )}>
        {/* Logo */}
        <div className="flex items-center gap-2 p-4 border-b border-border/50">
          <Activity className="h-6 w-6 text-primary shrink-0" />
          {!sidebarCollapsed && <span className="font-bold text-sm">RetailNexus</span>}
        </div>

        {/* Back to landing */}
        <button
          onClick={() => navigate("/")}
          className="flex items-center gap-2 px-4 py-2 text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          <ArrowLeft className="h-3.5 w-3.5 shrink-0" />
          {!sidebarCollapsed && <span>Back to Home</span>}
        </button>

        {/* Nav items */}
        <nav className="flex-1 p-2 space-y-1">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-all",
                activeTab === tab.id
                  ? "bg-primary/10 text-primary font-medium"
                  : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
              )}
            >
              <tab.icon className="h-4 w-4 shrink-0" />
              {!sidebarCollapsed && <span>{tab.label}</span>}
            </button>
          ))}
        </nav>

        {/* Collapse toggle */}
        <button
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          className="p-4 border-t border-border/50 text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          {sidebarCollapsed ? "→" : "← Collapse"}
        </button>
      </aside>

      {/* Main content */}
      <main className="flex-1 relative z-10">
        <header className="sticky top-0 z-20 backdrop-blur-xl bg-background/80 border-b border-border/50 px-6 py-4">
          <h1 className="text-xl font-bold">{tabs.find((t) => t.id === activeTab)?.label}</h1>
          <p className="text-xs text-muted-foreground mt-0.5">Real-time analytics from 50+ retail stores across India</p>
        </header>
        <div className="p-6">
          {renderTab()}
        </div>
      </main>
    </div>
  );
};

export default Dashboard;
