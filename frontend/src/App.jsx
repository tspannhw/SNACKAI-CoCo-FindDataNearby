import { useState } from 'react';
import MapView from './components/MapView';
import DatabaseBrowser from './components/DatabaseBrowser';
import QueryEditor from './components/QueryEditor';
import CortexChat from './components/CortexChat';

const TABS = ['Map Search', 'Database Browser', 'Query Editor', 'AI Chat'];

const styles = {
  app: {
    minHeight: '100vh',
    background: '#1a1a2e',
    color: '#e0e0e0',
    fontFamily: "'Segoe UI', system-ui, -apple-system, sans-serif",
  },
  header: {
    background: '#16213e',
    padding: '14px 24px',
    display: 'flex',
    alignItems: 'center',
    borderBottom: '2px solid #0f3460',
  },
  title: {
    margin: 0,
    fontSize: '20px',
    fontWeight: 700,
    color: '#e94560',
    letterSpacing: '0.5px',
  },
  subtitle: {
    marginLeft: 12,
    fontSize: '13px',
    color: '#8892b0',
  },
  nav: {
    display: 'flex',
    gap: 0,
    background: '#16213e',
    borderBottom: '1px solid #0f3460',
    paddingLeft: 24,
  },
  tab: (active) => ({
    padding: '10px 20px',
    cursor: 'pointer',
    background: active ? '#0f3460' : 'transparent',
    color: active ? '#e94560' : '#8892b0',
    border: 'none',
    borderBottom: active ? '2px solid #e94560' : '2px solid transparent',
    fontSize: '14px',
    fontWeight: active ? 600 : 400,
    transition: 'all 0.2s',
  }),
  content: {
    padding: 0,
  },
};

export default function App() {
  const [activeTab, setActiveTab] = useState(0);

  return (
    <div style={styles.app}>
      <header style={styles.header}>
        <h1 style={styles.title}>Find Data Nearby</h1>
        <span style={styles.subtitle}>Snowflake Geo-Data Explorer</span>
      </header>
      <nav style={styles.nav}>
        {TABS.map((tab, i) => (
          <button
            key={tab}
            style={styles.tab(i === activeTab)}
            onClick={() => setActiveTab(i)}
          >
            {tab}
          </button>
        ))}
      </nav>
      <div style={styles.content}>
        <div style={{ display: activeTab === 0 ? 'block' : 'none' }}>
          <MapView visible={activeTab === 0} />
        </div>
        <div style={{ display: activeTab === 1 ? 'block' : 'none' }}>
          <DatabaseBrowser />
        </div>
        <div style={{ display: activeTab === 2 ? 'block' : 'none' }}>
          <QueryEditor />
        </div>
        <div style={{ display: activeTab === 3 ? 'block' : 'none' }}>
          <CortexChat />
        </div>
      </div>
    </div>
  );
}
