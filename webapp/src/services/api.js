import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:5000';

const api = axios.create({
    baseURL: API_BASE_URL,
    timeout: 30000, // Increased to 30s for large Parquet scans
    headers: {
        'Content-Type': 'application/json',
    },
});

export const fetchSummaryKPIs = async () => {
    const response = await api.get('/api/v1/kpis/summary');
    return response.data;
};

export const fetchCLVData = async (limit = 50) => {
    const response = await api.get('/api/v1/kpis/clv', {
        params: { limit }
    });
    return response.data;
};

export const fetchMarketBasket = async (support = 2, limit = 50) => {
    const response = await api.get('/api/v1/kpis/market-basket', {
        params: { support, limit }
    });
    return response.data;
};

export default api;
