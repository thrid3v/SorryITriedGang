import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export default function CLVChart({ data }) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-slate-800 rounded-xl p-8 text-center">
                <p className="text-slate-400">No CLV data available</p>
            </div>
        );
    }

    // Take top 10 customers
    const topCustomers = data.slice(0, 10);

    return (
        <div className="bg-slate-800 rounded-xl p-6 shadow-lg border border-slate-700">
            <h3 className="text-xl font-bold text-white mb-4">Top 10 Customers by Lifetime Value</h3>
            <ResponsiveContainer width="100%" height={400}>
                <BarChart data={topCustomers}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis
                        dataKey="customer_name"
                        stroke="#94a3b8"
                        angle={-45}
                        textAnchor="end"
                        height={100}
                    />
                    <YAxis stroke="#94a3b8" />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: '#1e293b',
                            border: '1px solid #475569',
                            borderRadius: '8px'
                        }}
                    />
                    <Legend />
                    <Bar dataKey="estimated_clv" fill="#3b82f6" name="Estimated CLV ($)" />
                </BarChart>
            </ResponsiveContainer>
        </div>
    );
}
