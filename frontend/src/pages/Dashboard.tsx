import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { TrendingUp, Package, Users, Shield, Activity, Bot, Play, Square, LogOut, Upload, RotateCcw } from "lucide-react";
import { useAuth } from "@/contexts/AuthContext";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { useToast } from "@/hooks/use-toast";
import { useQueryClient } from "@tanstack/react-query";
import { resetData } from "@/data/api";
import SalesTab from "@/components/dashboard/SalesTab";
import InventoryTab from "@/components/dashboard/InventoryTab";
import CustomerTab from "@/components/dashboard/CustomerTab";
import DataQualityTab from "@/components/dashboard/DataQualityTab";
import AskAnalyst from "@/pages/AskAnalyst";
import DataUploader from "@/components/dashboard/DataUploader";

const tabs = [
  { id: "sales", label: "Sales Analytics", icon: TrendingUp, roles: ["admin", "customer"] },
  { id: "ai-analyst", label: "AI Analyst", icon: Bot, roles: ["admin", "customer"] },
  { id: "inventory", label: "Inventory & Ops", icon: Package, roles: ["admin"] },
  { id: "customer", label: "Customer Intel", icon: Users, roles: ["admin"] },
  { id: "quality", label: "Data Quality", icon: Shield, roles: ["admin"] },
];

interface StreamStatus {
  status: "running" | "stopped";
  started_at?: string;
  events_in_buffer?: number;
}

const Dashboard = () => {
  const [activeTab, setActiveTab] = useState("sales");
  const [streamStatus, setStreamStatus] = useState<StreamStatus>({ status: "stopped" });
  const [isStreamLoading, setIsStreamLoading] = useState(false);
  const [showUploader, setShowUploader] = useState(false);
  const [isResetting, setIsResetting] = useState(false);
  const navigate = useNavigate();
  const { toast } = useToast();
  const { user, logout, isAdmin } = useAuth();
  const queryClient = useQueryClient();

  // Filter tabs based on user role
  const visibleTabs = tabs.filter(tab => tab.roles.includes(user?.role || ""));

  const roleHeader = { 'X-User-Role': user?.role || 'customer' };

  // Poll stream status every 5 seconds
  useEffect(() => {
    const fetchStreamStatus = async () => {
      try {
        const res = await fetch(`/api/stream/status`);
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
      const res = await fetch(`/api/stream/start`, {
        method: "POST",
        headers: roleHeader
      });
      if (res.ok) {
        const data = await res.json();
        toast({
          title: "Stream Started",
          description: `Generator PID: ${data.generator_pid}, Processor PID: ${data.processor_pid}`,
        });
        setStreamStatus({ status: "running" });
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
      const res = await fetch(`/api/stream/stop`, {
        method: "POST",
        headers: roleHeader
      });
      if (res.ok) {
        toast({
          title: "Stream Stopped",
          description: "Real-time ingestion has been stopped",
        });
        setStreamStatus({ status: "stopped" });
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

  const handleUploadComplete = useCallback(() => {
    // Invalidate all queries to refresh dashboard data
    queryClient.invalidateQueries();
    toast({
      title: "Data Refreshed",
      description: "Dashboard data has been updated with the new dataset",
    });
    setShowUploader(false);
  }, [queryClient, toast]);

  const handleReset = async () => {
    setIsResetting(true);
    try {
      await resetData();
      queryClient.invalidateQueries();
      toast({
        title: "Data Reset",
        description: "All data has been cleared. Upload a new dataset to begin.",
      });
    } catch (err: any) {
      toast({
        title: "Reset Failed",
        description: err.message || "Failed to reset data",
        variant: "destructive",
      });
    } finally {
      setIsResetting(false);
    }
  };

  const renderTab = () => {
    switch (activeTab) {
      case "sales": return <SalesTab />;
      case "ai-analyst": return <AskAnalyst />;
      case "inventory": return <InventoryTab />;
      case "customer": return <CustomerTab />;
      case "quality": return <DataQualityTab />;
      default: return <SalesTab />;
    }
  };

  return (
    <div className="min-h-screen bg-deep-charcoal flex">
      {/* Ambient glow */}
      <div className="fixed inset-0 pointer-events-none">
        <div className="absolute top-[-10%] right-[-10%] w-[400px] h-[400px] rounded-full bg-acid-lime/5 blur-[120px]" />
        <div className="absolute bottom-[-10%] left-[-10%] w-[400px] h-[400px] rounded-full bg-safety-orange/5 blur-[120px]" />
      </div>

      {/* Floating Dock Navigation */}
      <aside className="floating-dock w-16">
        {/* Logo */}
        <button
          onClick={() => navigate("/")}
          className="flex items-center justify-center w-10 h-10 rounded-full bg-acid-lime/10 hover:bg-acid-lime/20 transition-all group"
          title="RetailNexus"
        >
          <Activity className="h-5 w-5 text-acid-lime" />
        </button>

        {/* Divider */}
        <div className="w-full h-px bg-border/30 my-1" />

        {/* Nav icons */}
        <nav className="flex flex-col gap-2">
          {visibleTabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                "w-10 h-10 rounded-full flex items-center justify-center transition-all duration-200",
                activeTab === tab.id
                  ? "bg-acid-lime text-deep-charcoal shadow-[0_0_20px_rgba(212,255,0,0.4)]"
                  : "text-muted-foreground hover:text-acid-lime hover:bg-acid-lime/10"
              )}
              title={tab.label}
            >
              <tab.icon className="h-4 w-4" />
            </button>
          ))}
        </nav>

        {/* Upload & Stream Control - Admin Only */}
        {isAdmin() && (
          <>
            <div className="w-full h-px bg-border/30 my-1" />

            {/* Upload Button */}
            <button
              onClick={() => setShowUploader(!showUploader)}
              className={cn(
                "w-10 h-10 rounded-full flex items-center justify-center transition-all duration-200",
                showUploader
                  ? "bg-emerald-500 text-white shadow-[0_0_20px_rgba(16,185,129,0.4)]"
                  : "text-muted-foreground hover:text-emerald-400 hover:bg-emerald-500/10"
              )}
              title="Upload Dataset"
            >
              <Upload className="h-4 w-4" />
            </button>

            {/* Stream Toggle */}
            <button
              onClick={streamStatus.status === "running" ? handleStopStream : handleStartStream}
              disabled={isStreamLoading}
              className={cn(
                "w-10 h-10 rounded-full flex items-center justify-center transition-all duration-200",
                streamStatus.status === "running"
                  ? "bg-safety-orange text-white shadow-[0_0_20px_rgba(255,159,41,0.4)]"
                  : "text-muted-foreground hover:text-acid-lime hover:bg-acid-lime/10"
              )}
              title={streamStatus.status === "running" ? "Stop Stream" : "Start Stream"}
            >
              {streamStatus.status === "running" ? (
                <Square className="h-4 w-4" />
              ) : (
                <Play className="h-4 w-4" />
              )}
            </button>
          </>
        )}
      </aside>

      {/* Main content */}
      <main className="flex-1 relative z-10 ml-24">
        <header className="sticky top-0 z-20 backdrop-blur-xl bg-deep-charcoal/80 border-b border-border/30 px-8 py-5">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl font-bold gradient-text">{tabs.find((t) => t.id === activeTab)?.label}</h1>
              <p className="text-xs text-muted-foreground mt-1">Dynamic analytics powered by your uploaded data</p>
            </div>
            <div className="flex items-center gap-4">
              {/* Stream Status Indicator - Admin Only */}
              {isAdmin() && streamStatus.status === "running" && (
                <div className="flex items-center gap-2 px-3 py-1.5 rounded-pill bg-safety-orange/10 border border-safety-orange/20">
                  <div className="w-2 h-2 rounded-full bg-safety-orange animate-pulse" />
                  <span className="text-xs text-safety-orange font-medium">
                    {streamStatus.events_in_buffer || 0} events
                  </span>
                </div>
              )}

              {/* Reset Data Button - Admin Only */}
              {isAdmin() && (
                <Button
                  onClick={handleReset}
                  disabled={isResetting}
                  variant="ghost"
                  size="sm"
                  className="rounded-pill text-muted-foreground hover:text-red-400 hover:bg-red-500/10"
                  title="Reset all data"
                >
                  <RotateCcw className={cn("h-4 w-4 mr-2", isResetting && "animate-spin")} />
                  Reset
                </Button>
              )}
              
              {/* User Info */}
              <div className="text-right">
                <div className="text-sm font-semibold">{user?.displayName}</div>
                <div className="text-xs">
                  <span className={cn(
                    "inline-flex items-center px-2.5 py-0.5 rounded-pill text-xs font-bold",
                    user?.role === "admin" ? "bg-acid-lime/20 text-acid-lime" : "bg-safety-orange/20 text-safety-orange"
                  )}>
                    {user?.role}
                  </span>
                </div>
              </div>

              {/* Logout Button */}
              <Button
                onClick={() => {
                  logout();
                  navigate("/login");
                }}
                variant="ghost"
                size="sm"
                className="rounded-pill text-muted-foreground hover:text-foreground hover:bg-card-charcoal"
              >
                <LogOut className="h-4 w-4 mr-2" />
                Logout
              </Button>
            </div>
          </div>

          {/* Upload Panel - slides down when active */}
          {showUploader && isAdmin() && (
            <div className="mt-4 pt-4 border-t border-border/30">
              <DataUploader onUploadComplete={handleUploadComplete} />
            </div>
          )}
        </header>
        <div className="p-8">
          {renderTab()}
        </div>
      </main>
    </div>
  );
};

export default Dashboard;
