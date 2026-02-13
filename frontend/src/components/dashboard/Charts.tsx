import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, LineChart, Line, PieChart, Pie, Cell, Legend } from "recharts";

const chartTooltipStyle = {
  contentStyle: {
    background: "hsl(222, 47%, 9%)",
    border: "1px solid hsl(220, 30%, 22%)",
    borderRadius: "8px",
    color: "hsl(210, 40%, 96%)",
    fontSize: "12px",
  },
};

interface GlassChartProps {
  title: string;
  children: React.ReactNode;
  className?: string;
  actions?: React.ReactNode;
}

export const GlassChart = ({ title, children, className, actions }: GlassChartProps) => (
  <div className={`glass-card p-5 animate-fade-in ${className || ""}`}>
    <div className="flex items-center justify-between mb-4">
      <h3 className="text-sm font-semibold">{title}</h3>
      {actions}
    </div>
    {children}
  </div>
);

export const GlassBarChart = ({ data, dataKey, xKey, color = "hsl(217, 91%, 60%)", height = 300 }: { data: any[]; dataKey: string; xKey: string; color?: string; height?: number }) => (
  <ResponsiveContainer width="100%" height={height}>
    <BarChart data={data}>
      <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 30%, 18%)" />
      <XAxis dataKey={xKey} tick={{ fill: "hsl(215, 20%, 55%)", fontSize: 11 }} axisLine={false} />
      <YAxis tick={{ fill: "hsl(215, 20%, 55%)", fontSize: 11 }} axisLine={false} />
      <Tooltip {...chartTooltipStyle} />
      <Bar dataKey={dataKey} fill={color} radius={[4, 4, 0, 0]} />
    </BarChart>
  </ResponsiveContainer>
);

export const GlassLineChart = ({ data, lines, xKey, height = 300 }: { data: any[]; lines: { key: string; color: string; name?: string }[]; xKey: string; height?: number }) => (
  <ResponsiveContainer width="100%" height={height}>
    <LineChart data={data}>
      <CartesianGrid strokeDasharray="3 3" stroke="hsl(220, 30%, 18%)" />
      <XAxis dataKey={xKey} tick={{ fill: "hsl(215, 20%, 55%)", fontSize: 11 }} axisLine={false} />
      <YAxis tick={{ fill: "hsl(215, 20%, 55%)", fontSize: 11 }} axisLine={false} />
      <Tooltip {...chartTooltipStyle} />
      {lines.map((l) => (
        <Line key={l.key} type="monotone" dataKey={l.key} stroke={l.color} strokeWidth={2} dot={false} name={l.name || l.key} />
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
