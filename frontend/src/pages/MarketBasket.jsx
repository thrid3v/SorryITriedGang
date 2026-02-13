import { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { fetchMarketBasket } from '../api';

export default function MarketBasket() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    try {
      const basketData = await fetchMarketBasket(2);
      setData(basketData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  if (loading) {
    return <div className="loading">Loading market basket analysis...</div>;
  }

  if (error) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Market Basket Analysis</h1>
        </div>
        <div className="error">Error: {error}</div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Market Basket Analysis</h1>
        </div>
        <div className="empty-state">
          <div className="empty-state-icon">🛍️</div>
          <p>No product pairs found. Generate more transactions with multiple products.</p>
        </div>
      </div>
    );
  }

  // Prepare data for chart
  const truncate = (str, len = 15) => {
    if (!str) return 'Unknown';
    return str.length > len ? str.substring(0, len) + '…' : str;
  };
  
  const chartData = data.slice(0, 10).map(item => ({
    name: `${truncate(item.product_a_name)} + ${truncate(item.product_b_name)}`,
    count: item.times_bought_together,
    fullName: `${item.product_a_name || 'Unknown'} + ${item.product_b_name || 'Unknown'}`
  }));

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Market Basket Analysis</h1>
        <p className="page-description">
          Discover which products are frequently bought together
        </p>
      </div>

      <div className="card">
        <h2 className="card-title">Top Product Pairs</h2>
        <ResponsiveContainer width="100%" height={400}>
          <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 80 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
            <XAxis 
              dataKey="name" 
              angle={-45}
              textAnchor="end"
              height={100}
              stroke="var(--text-secondary)"
              style={{ fontSize: '0.75rem' }}
            />
            <YAxis stroke="var(--text-secondary)" />
            <Tooltip 
              contentStyle={{
                background: 'var(--bg-secondary)',
                border: '1px solid var(--border-color)',
                borderRadius: '0.5rem'
              }}
              formatter={(value, name, props) => [value, props.payload.fullName]}
            />
            <Bar dataKey="count" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="card">
        <h2 className="card-title">Product Associations</h2>
        <table className="data-table">
          <thead>
            <tr>
              <th>Product A</th>
              <th>Product B</th>
              <th>Times Bought Together</th>
            </tr>
          </thead>
          <tbody>
            {data.map((pair, idx) => (
              <tr key={idx}>
                <td>{pair.product_a_name}</td>
                <td>{pair.product_b_name}</td>
                <td style={{ fontWeight: 600, color: 'var(--accent-purple)' }}>
                  {pair.times_bought_together}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2 className="card-title">💡 Insights</h2>
        <p style={{ color: 'var(--text-secondary)' }}>
          Use these product associations to:
        </p>
        <ul style={{ marginTop: '0.5rem', paddingLeft: '1.5rem', color: 'var(--text-secondary)' }}>
          <li>Create product bundles and cross-sell recommendations</li>
          <li>Optimize store layout by placing related products nearby</li>
          <li>Design targeted marketing campaigns</li>
          <li>Improve inventory management for complementary products</li>
        </ul>
      </div>
    </div>
  );
}
