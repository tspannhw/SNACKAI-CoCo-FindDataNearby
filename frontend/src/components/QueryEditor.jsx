import { useState, useCallback } from 'react';
import { executeQuery } from '../api/client';
import { downloadCsv, downloadJson, printTable } from '../utils/export';

const styles = {
  container: {
    padding: 20,
    height: 'calc(100vh - 90px)',
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
  },
  editor: {
    position: 'relative',
  },
  textarea: {
    width: '100%',
    minHeight: 160,
    padding: 14,
    background: '#16213e',
    border: '1px solid #0f3460',
    borderRadius: 8,
    color: '#e0e0e0',
    fontFamily: "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
    fontSize: 13,
    lineHeight: 1.5,
    resize: 'vertical',
    outline: 'none',
    boxSizing: 'border-box',
  },
  toolbar: {
    display: 'flex',
    alignItems: 'center',
    gap: 12,
  },
  btn: {
    padding: '8px 20px',
    background: '#e94560',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 600,
  },
  btnDisabled: {
    padding: '8px 20px',
    background: '#3a3a5c',
    color: '#8892b0',
    border: 'none',
    borderRadius: 4,
    cursor: 'not-allowed',
    fontSize: 13,
    fontWeight: 600,
  },
  info: {
    fontSize: 12,
    color: '#8892b0',
  },
  resultsWrap: {
    flex: 1,
    overflow: 'auto',
    background: '#16213e',
    borderRadius: 8,
    border: '1px solid #0f3460',
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 12,
  },
  th: {
    position: 'sticky',
    top: 0,
    textAlign: 'left',
    padding: '8px 10px',
    background: '#0f3460',
    color: '#e94560',
    fontWeight: 600,
    whiteSpace: 'nowrap',
  },
  td: {
    padding: '6px 10px',
    borderBottom: '1px solid rgba(15,52,96,0.4)',
    color: '#e0e0e0',
    maxWidth: 250,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  error: {
    background: 'rgba(233,69,96,0.1)',
    border: '1px solid #e94560',
    borderRadius: 6,
    padding: 12,
    color: '#e94560',
    fontSize: 13,
    whiteSpace: 'pre-wrap',
  },
  empty: {
    color: '#8892b0',
    textAlign: 'center',
    padding: 40,
    fontSize: 13,
  },
};

export default function QueryEditor() {
  const [sql, setSql] = useState('');
  const [results, setResults] = useState(null);
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [rowCount, setRowCount] = useState(0);
  const [elapsed, setElapsed] = useState(null);

  const run = useCallback(async () => {
    if (!sql.trim()) return;
    setLoading(true);
    setError('');
    setResults(null);
    const start = Date.now();
    try {
      const data = await executeQuery(sql);
      setElapsed(Date.now() - start);
      const rows = data.results || [];
      setResults(rows);
      setRowCount(rows.length);
    } catch (err) {
      setElapsed(Date.now() - start);
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [sql]);

  const handleKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      run();
    }
  };

  return (
    <div style={styles.container}>
      <div style={styles.editor}>
        <textarea
          style={styles.textarea}
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Enter SQL query... (Ctrl+Enter to execute)"
          spellCheck={false}
        />
      </div>

      <div style={styles.toolbar}>
        <button
          style={loading ? styles.btnDisabled : styles.btn}
          onClick={run}
          disabled={loading}
        >
          {loading ? 'Executing...' : 'Execute'}
        </button>
        {results && results.length > 0 && (
          <>
            <button style={{...styles.btn, background: '#0f3460', border: '1px solid #e94560'}} onClick={() => downloadCsv(results, 'query_results.csv')}>
              CSV
            </button>
            <button style={{...styles.btn, background: '#0f3460', border: '1px solid #e94560'}} onClick={() => downloadJson(results, 'query_results.json')}>
              JSON
            </button>
            <button style={{...styles.btn, background: '#0f3460', border: '1px solid #e94560'}} onClick={() => printTable(results, 'Query Results')}>
              Print
            </button>
          </>
        )}
        {results && (
          <span style={styles.info}>
            {rowCount} row{rowCount !== 1 ? 's' : ''}
            {elapsed != null ? ` in ${(elapsed / 1000).toFixed(2)}s` : ''}
          </span>
        )}
      </div>

      {error && <div style={styles.error}>{error}</div>}

      <div style={styles.resultsWrap}>
        {!results && !error && (
          <div style={styles.empty}>Results will appear here after executing a query.</div>
        )}
        {results && results.length === 0 && (
          <div style={styles.empty}>Query returned no rows.</div>
        )}
        {results && results.length > 0 && (
          <table style={styles.table}>
            <thead>
              <tr>
                {Object.keys(results[0]).map((col) => (
                  <th key={col} style={styles.th}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.map((row, i) => (
                <tr key={i}>
                  {Object.values(row).map((val, j) => (
                    <td key={j} style={styles.td}>{String(val ?? 'NULL')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
