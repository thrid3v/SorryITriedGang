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

/**
 * Health check
 * @returns {Promise<Object>}
 */
export async function checkHealth() {
  const response = await fetch(`${API_BASE_URL}/api/health`);
  if (!response.ok) throw new Error("Health check failed");
  return response.json();
}

/**
 * Trigger the full pipeline (generator + transformation)
 * @param {number} numTransactions - Number of transactions to generate
 * @returns {Promise<Object>}
 */
export async function triggerPipeline(numTransactions = 200) {
  const response = await fetch(
    `${API_BASE_URL}/api/pipeline/run?num_transactions=${numTransactions}`,
    { method: "POST" }
  );
  if (!response.ok) throw new Error("Failed to trigger pipeline");
  return response.json();
}

/**
 * Trigger just the data generator
 * @param {number} numTransactions - Number of transactions to generate
 * @returns {Promise<Object>}
 */
export async function triggerGenerator(numTransactions = 200) {
  const response = await fetch(
    `${API_BASE_URL}/api/pipeline/generate?num_transactions=${numTransactions}`,
    { method: "POST" }
  );
  if (!response.ok) throw new Error("Failed to trigger generator");
  return response.json();
}
