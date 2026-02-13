import { useState, useEffect } from 'react';
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { fetchKPIs, fetchCLV, triggerPipeline } from '../api';

export default function Dashboard() {
  const [kpis, setKpis] = useState(null);
  const [topCustomers, setTopCustomers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [pipelineMessage, setPipelineMessage] = useState('');

  useEffect(() => {
    loadData();
    // Auto-refresh every 30 seconds
    const interval = setInterval(loadData, 30000);
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

  async function handleRunPipeline() {
    setPipelineRunning(true);
    setPipelineMessage('Starting pipeline...');
    
    try {
      const result = await triggerPipeline(200);
      setPipelineMessage(`✅ ${result.message} — Data will refresh automatically when complete.`);
      // Poll for completion by checking health endpoint
      const pollInterval = setInterval(async () => {
        try {
          await loadData();
          clearInterval(pollInterval);
          setPipelineMessage('✅ Pipeline completed! Data refreshed.');
          setPipelineRunning(false);
        } catch {
          // Still running, keep polling
        }
      }, 5000); // Check every 5 seconds
      
      // Stop polling after 5 minutes max
      setTimeout(() => {
        clearInterval(pollInterval);
        setPipelineRunning(false);
      }, 300000);
    } catch (err) {
      setPipelineMessage(`❌ Error: ${err.message}`);
      setPipelineRunning(false);
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
        <h2 className="card-title">🚀 Run Pipeline</h2>
        <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem' }}>
          Generate new data and run the transformation pipeline directly from the dashboard:
        </p>
        <button
          onClick={handleRunPipeline}
          disabled={pipelineRunning}
          style={{
            padding: '0.75rem 1.5rem',
            background: pipelineRunning ? 'var(--text-muted)' : 'linear-gradient(135deg, var(--accent-blue), var(--accent-purple))',
            color: 'white',
            border: 'none',
            borderRadius: '0.5rem',
            fontSize: '1rem',
            fontWeight: 600,
            cursor: pipelineRunning ? 'not-allowed' : 'pointer',
            transition: 'transform 0.2s ease',
          }}
          onMouseEnter={(e) => !pipelineRunning && (e.target.style.transform = 'translateY(-2px)')}
          onMouseLeave={(e) => e.target.style.transform = 'translateY(0)'}
        >
          {pipelineRunning ? '⏳ Running...' : '▶️ Run Pipeline (200 transactions)'}
        </button>
        {pipelineMessage && (
          <div style={{
            marginTop: '1rem',
            padding: '0.75rem',
            background: 'var(--bg-secondary)',
            borderRadius: '0.5rem',
            fontSize: '0.875rem',
            color: pipelineMessage.startsWith('✅') ? 'var(--accent-green)' : 'var(--text-secondary)'
          }}>
            {pipelineMessage}
          </div>
        )}
      </div>

      <div className="card">
        <h2 className="card-title">📊 Pipeline Status</h2>
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
