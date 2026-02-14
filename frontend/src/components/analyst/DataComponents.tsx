import { TrendingUp, TrendingDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface StatCardProps {
  label: string;
  value: string | number;
  trend?: {
    direction: "up" | "down";
    value: number;
  };
  className?: string;
}

export const StatCard = ({ label, value, trend, className }: StatCardProps) => (
  <div className={cn("bento-card p-8 text-center", className)}>
    <div className="condensed-header text-muted-foreground mb-4">{label}</div>
    <div className="text-6xl font-bold text-white mb-4">{value}</div>
    {trend && (
      <div className={cn(
        "inline-flex items-center gap-2 px-4 py-2 rounded-pill text-sm font-bold",
        trend.direction === "up" ? "bg-acid-lime/20 text-acid-lime" : "bg-safety-orange/20 text-safety-orange"
      )}>
        {trend.direction === "up" ? <TrendingUp className="h-4 w-4" /> : <TrendingDown className="h-4 w-4" />}
        {trend.value}%
      </div>
    )}
  </div>
);

interface LeaderboardItem {
  label: string;
  value: number;
  maxValue: number;
}

interface LeaderboardProps {
  items: LeaderboardItem[];
  className?: string;
}

export const Leaderboard = ({ items, className }: LeaderboardProps) => (
  <div className={cn("space-y-3", className)}>
    {items.map((item, idx) => (
      <div key={idx} className="relative">
        {/* Background progress bar */}
        <div 
          className="absolute inset-0 bg-acid-lime/10 rounded-pill transition-all"
          style={{ width: `${(item.value / item.maxValue) * 100}%` }}
        />
        {/* Content */}
        <div className="relative flex items-center justify-between px-5 py-3 rounded-pill bg-card-charcoal/50 backdrop-blur-sm">
          <div className="flex items-center gap-3">
            <span className="flex items-center justify-center w-7 h-7 rounded-full bg-acid-lime/20 text-acid-lime text-xs font-bold">
              {idx + 1}
            </span>
            <span className="font-medium">{item.label}</span>
          </div>
          <span className="text-acid-lime font-bold">{item.value.toLocaleString()}</span>
        </div>
      </div>
    ))}
  </div>
);

interface DataTableProps {
  data: Record<string, any>[];
  maxRows?: number;
}

export const DataTable = ({ data, maxRows = 10 }: DataTableProps) => {
  if (!data || data.length === 0) return null;
  
  const keys = Object.keys(data[0]);
  const displayData = data.slice(0, maxRows);
  
  return (
    <div className="space-y-2">
      {displayData.map((row, idx) => (
        <div 
          key={idx}
          className={cn(
            "px-5 py-3 rounded-pill flex items-center justify-between",
            idx % 2 === 0 ? "bg-card-charcoal" : "bg-card-charcoal/50"
          )}
        >
          {keys.map((key, i) => (
            <div key={i} className={cn("flex-1", i === 0 ? "font-medium" : "text-muted-foreground")}>
              {row[key] !== null && row[key] !== undefined ? String(row[key]) : '-'}
            </div>
          ))}
        </div>
      ))}
      {data.length > maxRows && (
        <p className="text-xs text-muted-foreground text-center pt-2">
          Showing {maxRows} of {data.length} rows
        </p>
      )}
    </div>
  );
};
