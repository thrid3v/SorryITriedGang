import { CheckCircle, XCircle, Shield, Zap } from "lucide-react";
import KPICard from "./KPICard";
import { GlassChart, GlassLineChart } from "./Charts";
import { Table, TableHeader, TableBody, TableRow, TableHead, TableCell } from "@/components/ui/table";
import { dataQualityKPIs, qualityChecks, qualityTrend } from "@/data/mockData";
import { cn } from "@/lib/utils";

const DataQualityTab = () => (
  <div className="space-y-6">
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <KPICard title="Completeness" value={`${dataQualityKPIs.completeness}%`} icon={<Shield className="h-4 w-4" />} className="border-glow-teal/20" />
      <KPICard title="Accuracy" value={`${dataQualityKPIs.accuracy}%`} icon={<CheckCircle className="h-4 w-4" />} className="border-primary/20" />
      <KPICard title="Consistency" value={`${dataQualityKPIs.consistency}%`} icon={<Zap className="h-4 w-4" />} className="border-glow-purple/20" />
      <KPICard title="Timeliness" value={`${dataQualityKPIs.timeliness}%`} icon={<Zap className="h-4 w-4" />} className="border-glow-blue/20" />
    </div>

    <GlassChart title="Data Quality Trends Over Time">
      <GlassLineChart
        data={qualityTrend.map((d) => ({
          month: d.month,
          completeness: +d.completeness.toFixed(1),
          accuracy: +d.accuracy.toFixed(1),
          consistency: +d.consistency.toFixed(1),
        }))}
        xKey="month"
        lines={[
          { key: "completeness", color: "hsl(175, 70%, 45%)", name: "Completeness" },
          { key: "accuracy", color: "hsl(217, 91%, 60%)", name: "Accuracy" },
          { key: "consistency", color: "hsl(270, 60%, 55%)", name: "Consistency" },
        ]}
      />
    </GlassChart>

    <GlassChart title="Recent Data Quality Checks">
      <div className="overflow-auto">
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
                  <span className={cn("inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium", c.status === "Pass" ? "text-glow-teal bg-glow-teal/10" : "text-destructive bg-destructive/10")}>
                    {c.status === "Pass" ? <CheckCircle className="h-3 w-3" /> : <XCircle className="h-3 w-3" />}
                    {c.status}
                  </span>
                </TableCell>
                <TableCell className="text-right">{c.records.toLocaleString()}</TableCell>
                <TableCell className="text-right">{c.issues}</TableCell>
                <TableCell className="text-xs text-muted-foreground">{c.timestamp}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>
    </GlassChart>
  </div>
);

export default DataQualityTab;
