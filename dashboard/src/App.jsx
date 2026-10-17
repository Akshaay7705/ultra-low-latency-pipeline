import { useState, useEffect } from 'react'

function App() {
  const [signals, setSignals] = useState([]);
  const [loading, setLoading] = useState(true);
  const [avgLatency, setAvgLatency] = useState(0);

  const fetchSignals = async () => {
    try {
      // In production, this would be a WebSocket connection to inference-service
      // Using polling for the mock dashboard simulation
      const res = await fetch('/api/signals');
      const data = await res.json();
      
      if (data && data.signals) {
        setSignals(data.signals);
        
        // Calculate mock average latency based on current signals
        if (data.signals.length > 0) {
          const avg = data.signals.reduce((acc, curr) => acc + curr.inference_latency_us, 0) / data.signals.length;
          setAvgLatency(avg.toFixed(2));
        }
      }
      setLoading(false);
    } catch (err) {
      console.error("Failed to fetch signals:", err);
      // Fallback dummy data if backend is not running yet
      setSignals([
        { symbol: "AAPL", signal: "BUY", inference_latency_us: 142.5, end_to_end_latency_ms: 2.1, timestamp: Date.now()/1000 },
        { symbol: "TSLA", signal: "SELL", inference_latency_us: 156.2, end_to_end_latency_ms: 2.4, timestamp: Date.now()/1000 },
        { symbol: "MSFT", signal: "HOLD", inference_latency_us: 138.9, end_to_end_latency_ms: 1.9, timestamp: Date.now()/1000 },
        { symbol: "GOOG", signal: "BUY", inference_latency_us: 145.1, end_to_end_latency_ms: 2.2, timestamp: Date.now()/1000 }
      ]);
      setAvgLatency(145.67);
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSignals();
    const interval = setInterval(fetchSignals, 1000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="dashboard-container">
      <header className="header">
        <div className="title-section">
          <h1>ML Signal Pipeline</h1>
          <p>Ultra-Low-Latency HFT Simulation Dashboard</p>
        </div>
        <div className="status-badge">
          <div className="status-dot"></div>
          Pipeline Active
        </div>
      </header>

      <div className="grid-metrics">
        <div className="glass-panel metric-card">
          <h3>P99 Inference Latency</h3>
          <div className="metric-value highlight">{avgLatency} µs</div>
        </div>
        <div className="glass-panel metric-card">
          <h3>Throughput</h3>
          <div className="metric-value">~1,240 msg/s</div>
        </div>
        <div className="glass-panel metric-card">
          <h3>Active Regions</h3>
          <div className="metric-value">us-east, us-west</div>
        </div>
        <div className="glass-panel metric-card">
          <h3>Kafka Brokers</h3>
          <div className="metric-value">Healthy (1/1)</div>
        </div>
      </div>

      <h2 style={{ marginBottom: '1.5rem', fontSize: '1.25rem', fontWeight: 500 }}>Real-Time Signals</h2>
      
      <div className="signal-grid">
        {loading ? (
          Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="glass-panel" style={{ height: '120px' }}>
              <div className="skeleton"></div>
              <div className="skeleton" style={{ marginTop: '1rem', width: '60%' }}></div>
            </div>
          ))
        ) : (
          signals.map((sig, idx) => (
            <div key={idx} className="glass-panel signal-card">
              <div className="symbol-info">
                <h2>{sig.symbol}</h2>
                <p>Updated: {new Date(sig.timestamp * 1000).toLocaleTimeString()}</p>
              </div>
              
              <div className="latency-info">
                <div className={`signal-badge signal-${sig.signal}`}>
                  {sig.signal}
                </div>
                <div className="latency-value mt-2">
                  Inf: {sig.inference_latency_us}µs
                </div>
                <div className="latency-value">
                  E2E: {sig.end_to_end_latency_ms}ms
                </div>
              </div>
            </div>
          ))
        )}
      </div>
    </div>
  )
}

export default App
