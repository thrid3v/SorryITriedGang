import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { TrendingUp, Package, Users, Truck, Shield, Settings, Activity, ArrowLeft, Play, Square, LogOut } from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import SalesTab from "@/components/dashboard/SalesTab";
import InventoryTab from "@/components/dashboard/InventoryTab";
import CustomerTab from "@/components/dashboard/CustomerTab";
import DeliveryTab from "@/components/dashboard/DeliveryTab";
import DataQualityTab from "@/components/dashboard/DataQualityTab";

const tabs = [
  { id: "sales", label: "Sales Analytics", icon: TrendingUp, roles: ["admin", "customer"] },
  { id: "inventory", label: "Inventory & Ops", icon: Package, roles: ["admin"] },
  { id: "customer", label: "Customer Intel", icon: Users, roles: ["admin"] },
  { id: "delivery", label: "Delivery & Logistics", icon: Truck, roles: ["admin"] },
  { id: "quality", label: "Data Quality", icon: Shield, roles: ["admin"] },
  { id: "settings", label: "Settings", icon: Settings, roles: ["admin", "customer"] },
];

interface StreamStatus {
  status: "running" | "stopped";
  started_at?: string;
  events_processed: number;
  generator_pid?: number;
  processor_pid?: number;
}

const Dashboard = () => {
  const [activeTab, setActiveTab] = useState("sales");
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [streamStatus, setStreamStatus] = useState<StreamStatus>({ status: "stopped", events_processed: 0 });
  const [isStreamLoading, setIsStreamLoading] = useState(false);
  const navigate = useNavigate();
  const { toast } = useToast();
  const { user, logout, isAdmin } = useAuth();

  // Filter tabs based on user role
  const visibleTabs = tabs.filter(tab => tab.roles.includes(user?.role || ""));

  // Poll stream status every 5 seconds
  useEffect(() => {
    const fetchStreamStatus = async () => {
      try {
        const res = await fetch("http://localhost:8000/api/stream/status");
        if (res.ok) {
          const data = await res.json();
          setStreamStatus(data);
        }
      } catch (err) {
        console.error("Failed to fetch stream status:", err);
      }
    };

    fetchStreamStatus();
    const interval = setInterval(fetchStreamStatus, 5000);
    return () => clearInterval(interval);
  }, []);

  const handleStartStream = async () => {
    setIsStreamLoading(true);
    try {
      const token = localStorage.getItem('auth_token');
      const res = await fetch("http://localhost:8000/api/stream/start", { 
        method: "POST",
        headers: token ? { 'Authorization': `Bearer ${token}` } : {}
      });
      if (res.ok) {
        const data = await res.json();
        toast({
          title: "Stream Started",
          description: `Generator PID: ${data.generator_pid}, Processor PID: ${data.processor_pid}`,
        });
        setStreamStatus({ status: "running", events_processed: 0 });
      } else {
        const error = await res.json();
        toast({
          title: "Failed to Start Stream",
          description: error.detail || "Unknown error",
          variant: "destructive",
        });
      }
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to start stream",
        variant: "destructive",
      });
    } finally {
      setIsStreamLoading(false);
    }
  };

  const handleStopStream = async () => {
    setIsStreamLoading(true);
    try {
      const token = localStorage.getItem('auth_token');
      const res = await fetch("http://localhost:8000/api/stream/stop", { 
        method: "POST",
        headers: token ? { 'Authorization': `Bearer ${token}` } : {}
      });
      if (res.ok) {
        toast({
          title: "Stream Stopped",
          description: "Real-time ingestion has been stopped",
        });
        setStreamStatus({ status: "stopped", events_processed: 0 });
      } else {
        const error = await res.json();
        toast({
          title: "Failed to Stop Stream",
          description: error.detail || "Unknown error",
          variant: "destructive",
        });
      }
    } catch (err) {
      toast({
        title: "Error",
        description: "Failed to stop stream",
        variant: "destructive",
      });
    } finally {
      setIsStreamLoading(false);
    }
  };

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
          {visibleTabs.map((tab) => (
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

        {/* Stream Control - Admin Only */}
        {isAdmin && (
          <div className="p-2 border-t border-border/50">
            {!sidebarCollapsed && (
              <div className="mb-2 px-2">
                <div className="text-xs text-muted-foreground mb-1">Real-time Stream</div>
                {streamStatus.status === "running" && (
                  <div className="text-xs text-glow-teal mb-2">
                    ● {streamStatus.events_processed} events
                  </div>
                )}
              </div>
            )}
            <Button
              onClick={streamStatus.status === "running" ? handleStopStream : handleStartStream}
              disabled={isStreamLoading}
              variant={streamStatus.status === "running" ? "destructive" : "default"}
              size="sm"
              className={cn("w-full", sidebarCollapsed && "px-2")}
            >
              {streamStatus.status === "running" ? (
                <>
                  <Square className="h-3.5 w-3.5 shrink-0" />
                  {!sidebarCollapsed && <span className="ml-2">Stop Stream</span>}
                </>
              ) : (
                <>
                  <Play className="h-3.5 w-3.5 shrink-0" />
                  {!sidebarCollapsed && <span className="ml-2">Start Stream</span>}
                </>
              )}
            </Button>
          </div>
        )}

        {/* Logout Button */}
        <div className="p-2 border-t border-border/50">
          <Button
            onClick={() => {
              logout();
              navigate("/login");
            }}
            variant="ghost"
            size="sm"
            className={cn("w-full text-muted-foreground hover:text-foreground", sidebarCollapsed && "px-2")}
          >
            <LogOut className="h-3.5 w-3.5 shrink-0" />
            {!sidebarCollapsed && <span className="ml-2">Logout</span>}
          </Button>
        </div>

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
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-xl font-bold">{tabs.find((t) => t.id === activeTab)?.label}</h1>
              <p className="text-xs text-muted-foreground mt-0.5">Real-time analytics from 50+ retail stores across India</p>
            </div>
            <div className="flex items-center gap-3">
              <div className="text-right">
                <div className="text-sm font-medium">{user?.username}</div>
                <div className="text-xs text-muted-foreground">
                  <span className={cn(
                    "inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium",
                    user?.role === "admin" ? "bg-primary/10 text-primary" : "bg-secondary/10 text-secondary"
                  )}>
                    {user?.role}
                  </span>
                </div>
              </div>
            </div>
          </div>
        </header>
        <div className="p-6">
          {renderTab()}
        </div>
      </main>
    </div>
  );
};

export default Dashboard;
