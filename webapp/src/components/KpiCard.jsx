export default function KpiCard({ title, value, icon, trend }) {
    return (
        <div className="bg-gradient-to-br from-slate-800 to-slate-900 rounded-xl p-6 shadow-lg border border-slate-700 hover:border-blue-500 transition-all">
            <div className="flex items-center justify-between">
                <div>
                    <p className="text-slate-400 text-sm font-medium mb-1">{title}</p>
                    <p className="text-3xl font-bold text-white">{value}</p>
                    {trend && (
                        <p className="text-sm text-green-400 mt-2">
                            ↑ {trend}
                        </p>
                    )}
                </div>
                {icon && (
                    <div className="text-4xl opacity-20">
                        {icon}
                    </div>
                )}
            </div>
        </div>
    );
}
