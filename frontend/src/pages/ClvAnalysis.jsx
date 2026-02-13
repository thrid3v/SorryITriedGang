import { useState, useEffect } from 'react';
import { ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { fetchCLV } from '../api';

export default function ClvAnalysis() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [sortField, setSortField] = useState('estimated_clv');
  const [sortDirection, setSortDirection] = useState('desc');

  useEffect(() => {
    loadData();
  }, []);

  async function loadData() {
    try {
      const clvData = await fetchCLV();
      setData(clvData);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  const handleSort = (field) => {
    if (sortField === field) {
      setSortDirection(sortDirection === 'asc' ? 'desc' : 'asc');
    } else {
      setSortField(field);
      setSortDirection('desc');
    }
  };

  const sortedData = [...data].sort((a, b) => {
    const aVal = a[sortField];
    const bVal = b[sortField];
    const direction = sortDirection === 'asc' ? 1 : -1;
    return aVal > bVal ? direction : -direction;
  });

  if (loading) {
    return <div className="loading">Loading CLV analysis...</div>;
  }

  if (error) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Customer Lifetime Value</h1>
        </div>
        <div className="error">Error: {error}</div>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Customer Lifetime Value</h1>
        </div>
        <div className="empty-state">
          <div className="empty-state-icon">📊</div>
          <p>No CLV data available. Run the pipeline to generate data.</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Customer Lifetime Value</h1>
        <p className="page-description">
          Analyze customer spending patterns and lifetime value
        </p>
      </div>

      <div className="card">
        <h2 className="card-title">CLV Distribution</h2>
        <ResponsiveContainer width="100%" height={400}>
          <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
            <XAxis 
              type="number" 
              dataKey="purchase_count" 
              name="Purchase Count"
              stroke="var(--text-secondary)"
            />
            <YAxis 
              type="number" 
              dataKey="total_spend" 
              name="Total Spend"
              stroke="var(--text-secondary)"
            />
            <Tooltip 
              cursor={{ strokeDasharray: '3 3' }}
              contentStyle={{
                background: 'var(--bg-secondary)',
                border: '1px solid var(--border-color)',
                borderRadius: '0.5rem'
              }}
            />
            <Scatter data={data} fill="#3b82f6">
              {data.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={`hsl(${220 + index * 10}, 70%, 60%)`} />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>

      <div className="card">
        <h2 className="card-title">Top Customers</h2>
        <table className="data-table">
          <thead>
            <tr>
              <th onClick={() => handleSort('customer_name')} style={{ cursor: 'pointer' }}>
                Customer {sortField === 'customer_name' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
              <th onClick={() => handleSort('customer_city')} style={{ cursor: 'pointer' }}>
                City {sortField === 'customer_city' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
              <th onClick={() => handleSort('purchase_count')} style={{ cursor: 'pointer' }}>
                Orders {sortField === 'purchase_count' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
              <th onClick={() => handleSort('total_spend')} style={{ cursor: 'pointer' }}>
                Total Spend {sortField === 'total_spend' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
              <th onClick={() => handleSort('avg_order_value')} style={{ cursor: 'pointer' }}>
                Avg Order {sortField === 'avg_order_value' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
              <th onClick={() => handleSort('estimated_clv')} style={{ cursor: 'pointer' }}>
                CLV {sortField === 'estimated_clv' && (sortDirection === 'asc' ? '↑' : '↓')}
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedData.slice(0, 20).map((customer, idx) => (
              <tr key={idx}>
                <td>{customer.customer_name}</td>
                <td>{customer.customer_city}</td>
                <td>{customer.purchase_count}</td>
                <td>${customer.total_spend.toFixed(2)}</td>
                <td>${customer.avg_order_value.toFixed(2)}</td>
                <td style={{ fontWeight: 600, color: 'var(--accent-blue)' }}>
                  ${customer.estimated_clv.toFixed(2)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
