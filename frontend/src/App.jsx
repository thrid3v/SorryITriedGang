import { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useLocation } from 'react-router-dom';
import Dashboard from './pages/Dashboard';
import ClvAnalysis from './pages/ClvAnalysis';
import MarketBasket from './pages/MarketBasket';
import { checkHealth } from './api';
import './App.css';

function Sidebar() {
  const location = useLocation();
  const [apiStatus, setApiStatus] = useState('checking');
  
  const isActive = (path) => location.pathname === path;
  
  useEffect(() => {
    const check = async () => {
      try {
        const health = await checkHealth();
        setApiStatus(health.data_available ? 'connected' : 'no-data');
      } catch {
        setApiStatus('disconnected');
      }
    };
    check();
    const interval = setInterval(check, 15000);
    return () => clearInterval(interval);
  }, []);
  
  const statusLabel = {
    checking: 'Checking...',
    connected: 'API Connected',
    'no-data': 'API Up · No Data',
    disconnected: 'API Disconnected',
  };
  
  const statusColor = {
    checking: 'var(--accent-orange)',
    connected: 'var(--accent-green)',
    'no-data': 'var(--accent-orange)',
    disconnected: '#ef4444',
  };
  
  return (
    <nav className="sidebar">
      <div className="sidebar-header">
        <h1>🛒 RetailNexus</h1>
        <p className="subtitle">Data Lakehouse Analytics</p>
      </div>
      
      <div className="nav-links">
        <Link to="/" className={`nav-link ${isActive('/') ? 'active' : ''}`}>
          <span className="icon">📊</span>
          <span>Dashboard</span>
        </Link>
        <Link to="/clv" className={`nav-link ${isActive('/clv') ? 'active' : ''}`}>
          <span className="icon">💰</span>
          <span>CLV Analysis</span>
        </Link>
        <Link to="/basket" className={`nav-link ${isActive('/basket') ? 'active' : ''}`}>
          <span className="icon">🛍️</span>
          <span>Market Basket</span>
        </Link>
      </div>
      
      <div className="sidebar-footer">
        <div className="status-indicator">
          <div className="status-dot" style={{ background: statusColor[apiStatus] }}></div>
          <span>{statusLabel[apiStatus]}</span>
        </div>
      </div>
    </nav>
  );
}

function App() {
  return (
    <Router>
      <div className="app">
        <Sidebar />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/clv" element={<ClvAnalysis />} />
            <Route path="/basket" element={<MarketBasket />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
