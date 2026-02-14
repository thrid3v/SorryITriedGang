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
  <div className={cn("bento-card p-6 animate-fade-in hover:shadow-[0_12px_40px_-12px_rgba(0,0,0,0.5)] transition-shadow", className)}>
    <div className="flex items-start justify-between mb-4">
      <span className="text-lg font-bold text-white">{title}</span>
      <span className="text-acid-lime">{icon}</span>
    </div>
    <div className="text-3xl font-bold mb-2">{value}</div>
    <div className="flex items-center gap-2">
      {subtitle && <span className="text-xs text-muted-foreground">{subtitle}</span>}
      {trend !== undefined && (
        <span className={cn("text-xs font-bold px-2 py-0.5 rounded-pill", trend >= 0 ? "bg-acid-lime/20 text-acid-lime" : "bg-safety-orange/20 text-safety-orange")}>
          {trend >= 0 ? "↑" : "↓"} {Math.abs(trend)}%
        </span>
      )}
    </div>
  </div>
);

export default KPICard;
