import { CheckCircle, XCircle, Shield, Zap, Loader2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import KPICard from "./KPICard";
import { GlassChart, GlassLineChart } from "./Charts";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { fetchDataQualityKPIs, fetchDataQualityTrend, fetchDataQualityChecks } from "@/data/api";
import { cn } from "@/lib/utils";

const DataQualityTab = () => {
  const { data: kpis, isLoading: kpisLoading } = useQuery({
    queryKey: ["data-quality-kpis"],
    queryFn: fetchDataQualityKPIs,
  });

  const { data: trend, isLoading: trendLoading } = useQuery({
    queryKey: ["data-quality-trend"],
    queryFn: fetchDataQualityTrend,
  });

  const { data: checks, isLoading: checksLoading } = useQuery({
    queryKey: ["data-quality-checks"],
    queryFn: fetchDataQualityChecks,
  });

  const isLoading = kpisLoading || trendLoading || checksLoading;

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const qualityKPIs = kpis || { completeness: 0, accuracy: 0, consistency: 0, timeliness: 0 };
  const qualityTrend = trend || [];
  const qualityChecks = checks || [];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Completeness" value={`${qualityKPIs.completeness}%`} icon={<Shield className="h-4 w-4" />} className="border-glow-teal/20" />
        <KPICard title="Accuracy" value={`${qualityKPIs.accuracy}%`} icon={<CheckCircle className="h-4 w-4" />} className="border-primary/20" />
        <KPICard title="Consistency" value={`${qualityKPIs.consistency}%`} icon={<Zap className="h-4 w-4" />} className="border-glow-purple/20" />
        <KPICard title="Timeliness" value={`${qualityKPIs.timeliness}%`} icon={<Zap className="h-4 w-4" />} className="border-glow-blue/20" />
      </div>

      {qualityTrend.length > 0 && (
        <GlassChart title="Data Quality Trends Over Time">
          <GlassLineChart
            data={qualityTrend.map((d) => ({
              month: d.month,
              completeness: +(d.completeness?.toFixed(1) ?? 0),
              accuracy: +(d.accuracy?.toFixed(1) ?? 0),
              consistency: +(d.consistency?.toFixed(1) ?? 0),
            }))}
            xKey="month"
            lines={[
              { key: "completeness", color: "hsl(175, 70%, 45%)", name: "Completeness" },
              { key: "accuracy", color: "hsl(217, 91%, 60%)", name: "Accuracy" },
              { key: "consistency", color: "hsl(270, 60%, 55%)", name: "Consistency" },
            ]}
          />
        </GlassChart>
      )}

      <GlassChart title="Recent Data Quality Checks">
        <div className="overflow-auto">
          {qualityChecks.length === 0 ? (
            <p className="text-sm text-muted-foreground text-center py-8">No data quality checks available. Upload a dataset to begin.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow className="border-border/50">
                  <TableHead>Check</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead className="text-right">Records</TableHead>
                  <TableHead className="text-right">Issues</TableHead>
                  <TableHead>Timestamp</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {qualityChecks.map((c) => (
                  <TableRow key={c.check} className="border-border/30">
                    <TableCell className="font-medium text-sm">{c.check}</TableCell>
                    <TableCell>
                      <span className={cn("inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium", c.status === "pass" ? "text-glow-teal bg-glow-teal/10" : "text-destructive bg-destructive/10")}>
                        {c.status === "pass" ? <CheckCircle className="h-3 w-3" /> : <XCircle className="h-3 w-3" />}
                        {c.status}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">{c.records?.toLocaleString()}</TableCell>
                    <TableCell className="text-right">{c.issues}</TableCell>
                    <TableCell className="text-xs text-muted-foreground">{c.timestamp}</TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </div>
      </GlassChart>
    </div>
  );
};

export default DataQualityTab;
