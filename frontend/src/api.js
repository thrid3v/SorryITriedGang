/**
 * API Client for RetailNexus Backend
 * Fetches data from FastAPI server running on localhost:8000
 */

const API_BASE_URL = "http://localhost:8000";

/**
 * Fetch summary KPIs
 * @returns {Promise<{total_revenue: number, active_users: number, total_orders: number}>}
 */
export async function fetchKPIs() {
  const response = await fetch(`${API_BASE_URL}/api/kpis`);
  if (!response.ok) throw new Error("Failed to fetch KPIs");
  return response.json();
}

/**
 * Fetch Customer Lifetime Value data
 * @returns {Promise<Array>}
 */
export async function fetchCLV() {
  const response = await fetch(`${API_BASE_URL}/api/clv`);
  if (!response.ok) throw new Error("Failed to fetch CLV data");
  return response.json();
}

/**
 * Fetch Market Basket Analysis data
 * @param {number} minSupport - Minimum support threshold
 * @returns {Promise<Array>}
 */
export async function fetchMarketBasket(minSupport = 2) {
  const response = await fetch(
    `${API_BASE_URL}/api/basket?min_support=${minSupport}`,
  );
  if (!response.ok) throw new Error("Failed to fetch market basket data");
  return response.json();
}

// Streaming endpoints
export async function startStream() {
  const response = await fetch(`${API_BASE_URL}/api/stream/start`, { method: 'POST' });
  if (!response.ok) throw new Error('Failed to start stream');
  return response.json();
}

export async function stopStream() {
  const response = await fetch(`${API_BASE_URL}/api/stream/stop`, { method: 'POST' });
  if (!response.ok) throw new Error('Failed to stop stream');
  return response.json();
}

export async function getStreamStatus() {
  const response = await fetch(`${API_BASE_URL}/api/stream/status`);
  if (!response.ok) throw new Error('Failed to fetch stream status');
  return response.json();
}

/**
 * Health check
 * @returns {Promise<Object>}
 */
export async function checkHealth() {
  const response = await fetch(`${API_BASE_URL}/api/health`);
  if (!response.ok) throw new Error("Health check failed");
  return response.json();
}
