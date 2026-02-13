export default function MarketBasketTable({ data }) {
    if (!data || data.length === 0) {
        return (
            <div className="bg-slate-800 rounded-xl p-8 text-center">
                <p className="text-slate-400">No market basket data available</p>
                <p className="text-sm text-slate-500 mt-2">
                    Market basket analysis requires transactions with multiple products
                </p>
            </div>
        );
    }

    return (
        <div className="bg-slate-800 rounded-xl p-6 shadow-lg border border-slate-700">
            <h3 className="text-xl font-bold text-white mb-4">Product Pairs Frequently Bought Together</h3>
            <div className="overflow-x-auto">
                <table className="w-full">
                    <thead>
                        <tr className="border-b border-slate-700">
                            <th className="text-left py-3 px-4 text-slate-300 font-semibold">Product A</th>
                            <th className="text-left py-3 px-4 text-slate-300 font-semibold">Product B</th>
                            <th className="text-right py-3 px-4 text-slate-300 font-semibold">Co-Purchases</th>
                        </tr>
                    </thead>
                    <tbody>
                        {data.slice(0, 15).map((item, idx) => (
                            <tr
                                key={idx}
                                className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors"
                            >
                                <td className="py-3 px-4 text-slate-200">{item.product_a_name}</td>
                                <td className="py-3 px-4 text-slate-200">{item.product_b_name}</td>
                                <td className="py-3 px-4 text-right">
                                    <span className="inline-block bg-blue-500/20 text-blue-300 px-3 py-1 rounded-full text-sm font-semibold">
                                        {item.times_bought_together}
                                    </span>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
