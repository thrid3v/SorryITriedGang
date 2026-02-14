import { Sparkles, TrendingUp, Users, Package, DollarSign } from "lucide-react";
import { cn } from "@/lib/utils";

interface ActionCardProps {
  title: string;
  description: string;
  icon: React.ReactNode;
  onClick: () => void;
  className?: string;
}

export const ActionCard = ({ title, description, icon, onClick, className }: ActionCardProps) => (
  <button
    onClick={onClick}
    className={cn(
      "bento-card p-5 text-left transition-all hover:shadow-[0_12px_40px_-12px_rgba(212,255,0,0.2)] hover:scale-[1.02] group",
      className
    )}
  >
    <div className="flex items-start gap-3 mb-3">
      <div className="flex items-center justify-center w-10 h-10 rounded-full bg-acid-lime/10 text-acid-lime group-hover:bg-acid-lime/20 transition-colors">
        {icon}
      </div>
      <div className="flex-1">
        <h4 className="font-bold text-sm mb-1">{title}</h4>
        <p className="text-xs text-muted-foreground leading-relaxed">{description}</p>
      </div>
    </div>
    {/* Decorative sparkline */}
    <div className="flex items-end gap-0.5 h-6">
      {[3, 7, 4, 9, 6, 8, 5].map((height, i) => (
        <div
          key={i}
          className="flex-1 bg-acid-lime/20 rounded-t-sm transition-all group-hover:bg-acid-lime/40"
          style={{ height: `${height * 10}%` }}
        />
      ))}
    </div>
  </button>
);

interface QuickAction {
  title: string;
  description: string;
  question: string;
  icon: React.ReactNode;
}

export const quickActions: QuickAction[] = [
  {
    title: "Revenue Analysis",
    description: "Get total revenue and trends",
    question: "What is my total revenue?",
    icon: <DollarSign className="h-5 w-5" />
  },
  {
    title: "Top Customers",
    description: "Find highest spending customers",
    question: "Who are my top 5 customers by spending?",
    icon: <Users className="h-5 w-5" />
  },
  {
    title: "Product Performance",
    description: "Analyze best-selling products",
    question: "Which products sell best in New York?",
    icon: <Package className="h-5 w-5" />
  },
  {
    title: "Category Breakdown",
    description: "Revenue by product category",
    question: "Show me revenue by category",
    icon: <TrendingUp className="h-5 w-5" />
  }
];

interface SessionHistoryProps {
  history: Array<{ question: string; timestamp: Date }>;
  onSelect: (question: string) => void;
}

export const SessionHistory = ({ history, onSelect }: SessionHistoryProps) => {
  if (history.length === 0) {
    return (
      <div className="text-center py-12 text-muted-foreground text-sm">
        <Sparkles className="h-8 w-8 mx-auto mb-2 opacity-50" />
        <p>No queries yet</p>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* Vertical timeline line */}
      <div className="relative">
        <div className="absolute left-[15px] top-0 bottom-0 w-px bg-border/30" />
        
        {history.slice(-10).reverse().map((item, idx) => (
          <div key={idx} className="relative pl-10 pb-4">
            {/* Timeline dot */}
            <div className="absolute left-[11px] top-[6px] w-2 h-2 rounded-full bg-acid-lime shadow-[0_0_8px_rgba(212,255,0,0.4)]" />
            
            {/* Content pill */}
            <button
              onClick={() => onSelect(item.question)}
              className="w-full text-left px-4 py-2.5 rounded-pill bg-card-charcoal hover:bg-card-charcoal/80 transition-all group"
            >
              <p className="text-xs text-muted-foreground mb-1">
                {item.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              </p>
              <p className="text-sm font-medium group-hover:text-acid-lime transition-colors line-clamp-2">
                {item.question}
              </p>
            </button>
          </div>
        ))}
      </div>
    </div>
  );
};

interface SystemStatusProps {
  sources: Array<{ name: string; status: "live" | "syncing" | "offline" }>;
}

export const SystemStatus = ({ sources }: SystemStatusProps) => (
  <div className="flex items-center gap-6 px-6 py-3 bg-card-charcoal/50 rounded-pill backdrop-blur-sm">
    <span className="condensed-header text-muted-foreground">System Status</span>
    <div className="flex items-center gap-4">
      {sources.map((source, idx) => (
        <div key={idx} className="flex items-center gap-2">
          <div className={cn(
            "w-2 h-2 rounded-full",
            source.status === "live" && "bg-acid-lime shadow-[0_0_8px_rgba(212,255,0,0.6)] animate-pulse",
            source.status === "syncing" && "bg-safety-orange shadow-[0_0_8px_rgba(255,159,41,0.6)] animate-pulse",
            source.status === "offline" && "bg-muted"
          )} />
          <span className="text-xs font-medium">{source.name}</span>
          <span className="text-xs text-muted-foreground capitalize">({source.status})</span>
        </div>
      ))}
    </div>
  </div>
);
