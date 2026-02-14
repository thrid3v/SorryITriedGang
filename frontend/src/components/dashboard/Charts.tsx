import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell, Legend } from "recharts";

const chartTooltipStyle = {
  contentStyle: {
    background: "#161616",
    border: "none",
    borderRadius: "16px",
    color: "#FFFFFF",
    fontSize: "12px",
    padding: "12px",
    boxShadow: "0 8px 32px -8px rgba(0,0,0,0.6)",
  },
};

interface GlassChartProps {
  title: string;
  children: React.ReactNode;
  className?: string;
  actions?: React.ReactNode;
}

export const GlassChart = ({ title, children, className, actions }: GlassChartProps) => (
  <div className={`bento-card p-6 animate-fade-in hover:shadow-[0_12px_40px_-12px_rgba(0,0,0,0.5)] transition-shadow ${className || ""}`}>
    <div className="flex items-center justify-between mb-6">
      <h3 className="condensed-header text-foreground">{title}</h3>
      {actions}
    </div>
    {children}
  </div>
);

export const GlassBarChart = ({ data, dataKey, xKey, color = "#D4FF00", height = 300 }: { data: any[]; dataKey: string; xKey: string; color?: string; height?: number }) => (
  <ResponsiveContainer width="100%" height={height}>
    <BarChart data={data} barGap={8}>
      <XAxis dataKey={xKey} tick={{ fill: "hsl(0, 0%, 60%)", fontSize: 11 }} axisLine={false} tickLine={false} />
      <YAxis tick={{ fill: "hsl(0, 0%, 60%)", fontSize: 11 }} axisLine={false} tickLine={false} />
      <Tooltip {...chartTooltipStyle} cursor={{ fill: 'transparent' }} />
      <Bar dataKey={dataKey} fill={color} radius={[999, 999, 999, 999]} />
    </BarChart>
  </ResponsiveContainer>
);

export const GlassLineChart = ({ data, lines, xKey, height = 300 }: { data: any[]; lines: { key: string; color: string; name?: string }[]; xKey: string; height?: number }) => (
  <ResponsiveContainer width="100%" height={height}>
    <LineChart data={data}>
      <XAxis dataKey={xKey} tick={{ fill: "hsl(0, 0%, 60%)", fontSize: 11 }} axisLine={false} tickLine={false} />
      <YAxis tick={{ fill: "hsl(0, 0%, 60%)", fontSize: 11 }} axisLine={false} tickLine={false} />
      <Tooltip {...chartTooltipStyle} />
      {lines.map((l) => (
        <Line key={l.key} type="monotone" dataKey={l.key} stroke={l.color} strokeWidth={3} dot={{ fill: l.color, r: 4 }} name={l.name || l.key} />
      ))}
      {lines.length > 1 && <Legend wrapperStyle={{ fontSize: "11px" }} />}
    </LineChart>
  </ResponsiveContainer>
);

export const GlassDonutChart = ({ data, height = 250 }: { data: { name: string; value: number; fill: string }[]; height?: number }) => (
  <ResponsiveContainer width="100%" height={height}>
    <PieChart>
      <Pie data={data} cx="50%" cy="50%" innerRadius={60} outerRadius={90} paddingAngle={4} dataKey="value" label={({ name, value }) => `${name}: ${value}%`} labelLine={false}>
        {data.map((entry, i) => (
          <Cell key={i} fill={entry.fill} />
        ))}
      </Pie>
      <Tooltip {...chartTooltipStyle} />
    </PieChart>
  </ResponsiveContainer>
);
