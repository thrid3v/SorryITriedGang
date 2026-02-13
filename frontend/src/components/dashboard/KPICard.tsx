import { ReactNode } from "react";
import { cn } from "@/lib/utils";

interface KPICardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  icon: ReactNode;
  trend?: number;
  className?: string;
}

const KPICard = ({ title, value, subtitle, icon, trend, className }: KPICardProps) => (
  <div className={cn("glass-card p-5 animate-fade-in", className)}>
    <div className="flex items-start justify-between mb-3">
      <span className="text-xs text-muted-foreground uppercase tracking-wider">{title}</span>
      <span className="text-muted-foreground">{icon}</span>
    </div>
    <div className="text-2xl font-bold mb-1">{value}</div>
    <div className="flex items-center gap-2">
      {subtitle && <span className="text-xs text-muted-foreground">{subtitle}</span>}
      {trend !== undefined && (
        <span className={cn("text-xs font-medium", trend >= 0 ? "text-glow-teal" : "text-destructive")}>
          {trend >= 0 ? "↑" : "↓"} {Math.abs(trend)}%
        </span>
      )}
    </div>
  </div>
);

export default KPICard;
