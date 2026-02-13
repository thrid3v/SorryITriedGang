import { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { fetchKPIs, fetchCLV, startStream, stopStream, getStreamStatus } from '../api';

export default function Dashboard() {
  const [kpis, setKpis] = useState(null);
  const [topCustomers, setTopCustomers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [streamStatus, setStreamStatus] = useState({ status: 'stopped', events_in_buffer: 0 });
  const [streamLoading, setStreamLoading] = useState(false);

  useEffect(() => {
    loadData();
    loadStreamStatus();
    // Auto-refresh every 30 seconds
    const interval = setInterval(() => {
      loadData();
      loadStreamStatus();
    }, 30000);
    return () => clearInterval(interval);
  }, []);

  async function loadData() {
    try {
      const [kpiData, clvData] = await Promise.all([
        fetchKPIs(),
        fetchCLV().catch(() => [])
      ]);
      setKpis(kpiData);
      // Top 10 customers by CLV for the chart
      setTopCustomers(clvData.slice(0, 10));
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function loadStreamStatus() {
    try {
      const status = await getStreamStatus();
      setStreamStatus(status);
    } catch (err) {
      console.error('Failed to load stream status:', err);
    }
  }

  async function handleToggleStream() {
    setStreamLoading(true);
    try {
      if (streamStatus.status === 'running') {
        await stopStream();
      } else {
        await startStream();
      }
      await loadStreamStatus();
    } catch (err) {
      alert(`Failed to ${streamStatus.status === 'running' ? 'stop' : 'start'} stream: ${err.message}`);
    } finally {
      setStreamLoading(false);
    }
  }

  if (loading) {
    return <div className="loading">Loading dashboard...</div>;
  }

  if (error) {
    return (
      <div>
        <div className="page-header">
          <h1 className="page-title">Dashboard Overview</h1>
        </div>
        <div className="error">
          <strong>Error:</strong> {error}
          <br />
          <small>Make sure the FastAPI server is running on port 8000</small>
        </div>
      </div>
    );
  }

  const chartData = topCustomers.map(c => ({
    name: (c.customer_name || 'Unknown').split(' ')[0],
    clv: Math.round(c.estimated_clv || 0),
    orders: c.purchase_count || 0
  }));

  return (
    <div>
      <div className="page-header">
        <h1 className="page-title">Dashboard Overview</h1>
        <p className="page-description">
          Real-time analytics from your data lakehouse
        </p>
      </div>

      {/* Streaming Controls */}
      <div style={{
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        borderRadius: '12px',
        padding: '20px',
        marginBottom: '24px',
        color: 'white',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <div>
          <h3 style={{ margin: '0 0 8px 0', fontSize: '18px', fontWeight: '600' }}>
            🌊 Real-Time Ingestion
          </h3>
          <p style={{ margin: 0, opacity: 0.9, fontSize: '14px' }}>
            {streamStatus.status === 'running'
              ? `Stream active • ${streamStatus.events_in_buffer} events in buffer`
              : 'Stream stopped • Click to start receiving live orders'}
          </p>
        </div>
        <button
          onClick={handleToggleStream}
          disabled={streamLoading}
          style={{
            background: streamStatus.status === 'running' ? '#ef4444' : '#10b981',
            color: 'white',
            border: 'none',
            padding: '12px 24px',
            borderRadius: '8px',
            fontSize: '14px',
            fontWeight: '600',
            cursor: streamLoading ? 'not-allowed' : 'pointer',
            opacity: streamLoading ? 0.6 : 1,
            transition: 'all 0.2s',
            boxShadow: '0 4px 6px rgba(0,0,0,0.1)'
          }}
        >
          {streamLoading ? '...' : streamStatus.status === 'running' ? '⏹️ Stop Stream' : '▶️ Start Stream'}
        </button>
      </div>

      <div className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-label">Total Revenue</div>
          <div className="kpi-value">
            ${(kpis.total_revenue || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-label">Active Users</div>
          <div className="kpi-value">
            {(kpis.active_users || 0).toLocaleString()}
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-label">Total Orders</div>
          <div className="kpi-value">
            {(kpis.total_orders || 0).toLocaleString()}
          </div>
        </div>
      </div>

      {chartData.length > 0 && (
        <div className="card">
          <h2 className="card-title">🏆 Top Customers by CLV</h2>
          <ResponsiveContainer width="100%" height={350}>
            <BarChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border-color)" />
              <XAxis dataKey="name" stroke="var(--text-secondary)" />
              <YAxis stroke="var(--text-secondary)" />
              <Tooltip
                contentStyle={{
                  background: 'var(--bg-secondary)',
                  border: '1px solid var(--border-color)',
                  borderRadius: '0.5rem'
                }}
                formatter={(value) => [`$${value.toLocaleString()}`, 'CLV']}
              />
              <Bar dataKey="clv" fill="#3b82f6" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="card">
        <h2 className="card-title">📊 Pipeline Info</h2>
        <p style={{ color: 'var(--text-secondary)' }}>
          Data is being served from the Gold layer (Parquet files with Hive-style partitioning).
          The pipeline automatically deduplicates, applies SCD Type 2 history tracking, and builds
          a star schema optimized for analytics.
        </p>
        <div style={{ marginTop: '1rem', display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
          <div style={{ padding: '0.5rem 1rem', background: 'var(--bg-secondary)', borderRadius: '0.5rem', fontSize: '0.875rem' }}>
            <strong>Bronze:</strong> Raw CSV files
          </div>
          <div style={{ padding: '0.5rem 1rem', background: 'var(--bg-secondary)', borderRadius: '0.5rem', fontSize: '0.875rem' }}>
            <strong>Silver:</strong> Cleaned Parquet
          </div>
          <div style={{ padding: '0.5rem 1rem', background: 'var(--bg-secondary)', borderRadius: '0.5rem', fontSize: '0.875rem' }}>
            <strong>Gold:</strong> Star Schema
          </div>
        </div>
      </div>
    </div>
  );
}
