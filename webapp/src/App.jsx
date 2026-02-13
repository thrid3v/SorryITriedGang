import { useState, useEffect } from 'react';
import { fetchSummaryKPIs, fetchCLVData, fetchMarketBasket } from './services/api';
import { mockSummaryKPIs, mockCLVData, mockMarketBasketData } from './services/mockData';
import KpiCard from './components/KpiCard';
import CLVChart from './components/CLVChart';
import MarketBasketTable from './components/MarketBasketTable';

function App() {
    const [kpis, setKpis] = useState(null);
    const [clvData, setClvData] = useState([]);
    const [basketData, setBasketData] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [usingMockData, setUsingMockData] = useState(false);

    useEffect(() => {
        loadData();
        // Auto-refresh removed - data only loads on mount or manual refresh
    }, []);


    const loadData = async () => {
        try {
            setLoading(true);
            setError(null);

            // Try to fetch from API first
            try {
                const [kpisRes, clvRes, basketRes] = await Promise.all([
                    fetchSummaryKPIs(),
                    fetchCLVData(50),
                    fetchMarketBasket(2, 50)
                ]);

                if (kpisRes.status === 'success') {
                    setKpis(kpisRes.data);
                }
                if (clvRes.status === 'success') {
                    setClvData(clvRes.data);
                }
                if (basketRes.status === 'success') {
                    setBasketData(basketRes.data);
                }
                setUsingMockData(false);
            } catch (apiError) {
                // If API fails, use mock data for development
                console.warn('⚠️ API not available, using mock data:', apiError.message);

                setKpis(mockSummaryKPIs.data);
                setClvData(mockCLVData.data);
                setBasketData(mockMarketBasketData.data);
                setUsingMockData(true);
            }
        } catch (err) {
            setError(err.message);
            console.error('Error loading data:', err);
        } finally {
            setLoading(false);
        }
    };

    if (loading && !kpis) {
        return (
            <div className="min-h-screen bg-slate-900 flex items-center justify-center">
                <div className="text-center">
                    <div className="animate-spin rounded-full h-16 w-16 border-b-2 border-blue-500 mx-auto mb-4"></div>
                    <p className="text-slate-400">Loading analytics...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="min-h-screen bg-slate-900 flex items-center justify-center">
                <div className="bg-red-500/10 border border-red-500 rounded-xl p-6 max-w-md">
                    <h3 className="text-red-400 font-bold mb-2">Error Loading Data</h3>
                    <p className="text-slate-300">{error}</p>
                    <button
                        onClick={loadData}
                        className="mt-4 bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg transition-colors"
                    >
                        Retry
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div className="min-h-screen bg-slate-900 text-white">
            {/* Header */}
            <header className="bg-slate-800 border-b border-slate-700 shadow-lg">
                <div className="max-w-7xl mx-auto px-6 py-4">
                    <div className="flex items-center justify-between">
                        <div>
                            <h1 className="text-2xl font-bold">📊 RetailNexus Analytics</h1>
                            <p className="text-slate-400 text-sm">Smart Retail Supply Chain & Customer Intelligence</p>
                        </div>
                        <div className="flex items-center gap-4">
                            {usingMockData && (
                                <span className="bg-yellow-500/20 text-yellow-300 px-3 py-1 rounded-full text-sm">
                                    🔧 Using Mock Data
                                </span>
                            )}
                            <button
                                onClick={loadData}
                                className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-lg transition-colors flex items-center gap-2"
                                disabled={loading}
                            >
                                {loading ? '⟳ Refreshing...' : '↻ Refresh'}
                            </button>
                        </div>
                    </div>
                </div>
            </header>

            {/* Main Content */}
            <main className="max-w-7xl mx-auto px-6 py-8">
                {/* KPI Cards */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                    <KpiCard
                        title="Total Revenue"
                        value={kpis ? `$${kpis.total_revenue.toLocaleString()}` : '$0'}
                        icon="💰"
                    />
                    <KpiCard
                        title="Active Users"
                        value={kpis ? kpis.active_users.toLocaleString() : '0'}
                        icon="👥"
                    />
                    <KpiCard
                        title="Total Orders"
                        value={kpis ? kpis.total_orders.toLocaleString() : '0'}
                        icon="📦"
                    />
                </div>

                {/* CLV Chart */}
                <div className="mb-8">
                    <CLVChart data={clvData} />
                </div>

                {/* Market Basket Table */}
                <div>
                    <MarketBasketTable data={basketData} />
                </div>
            </main>

            {/* Footer */}
            <footer className="bg-slate-800 border-t border-slate-700 mt-12">
                <div className="max-w-7xl mx-auto px-6 py-4 text-center text-slate-400 text-sm">
                    RetailNexus v1.0 • Data Lakehouse Dashboard • DuckDB + Parquet + Flask + React
                </div>
            </footer>
        </div>
    );
}

export default App;
