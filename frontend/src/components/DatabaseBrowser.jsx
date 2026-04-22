import { useState, useEffect, useCallback } from 'react';
import {
  fetchDatabases,
  fetchSchemas,
  fetchTables,
  fetchViews,
  fetchSemanticViews,
  executeQuery,
} from '../api/client';

const styles = {
  container: {
    display: 'flex',
    height: 'calc(100vh - 90px)',
  },
  tree: {
    width: 320,
    background: '#16213e',
    borderRight: '1px solid #0f3460',
    overflowY: 'auto',
    padding: 12,
  },
  detail: {
    flex: 1,
    padding: 20,
    overflowY: 'auto',
  },
  section: {
    marginBottom: 16,
  },
  sectionTitle: {
    fontSize: 12,
    fontWeight: 700,
    color: '#e94560',
    textTransform: 'uppercase',
    letterSpacing: 1,
    marginBottom: 8,
  },
  node: (level, active) => ({
    padding: '5px 8px',
    paddingLeft: 8 + level * 16,
    cursor: 'pointer',
    background: active ? 'rgba(233,69,96,0.15)' : 'transparent',
    borderRadius: 4,
    fontSize: 13,
    color: active ? '#e94560' : '#e0e0e0',
    display: 'flex',
    alignItems: 'center',
    gap: 6,
  }),
  icon: {
    fontSize: 11,
    color: '#8892b0',
    width: 14,
    textAlign: 'center',
  },
  badge: (type) => ({
    fontSize: 9,
    fontWeight: 700,
    padding: '1px 5px',
    borderRadius: 3,
    marginLeft: 'auto',
    background: type === 'VIEW' ? '#0f3460' : type === 'SEMANTIC' ? '#e94560' : '#1a1a2e',
    color: type === 'VIEW' ? '#8892b0' : type === 'SEMANTIC' ? '#fff' : '#8892b0',
  }),
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 13,
  },
  th: {
    textAlign: 'left',
    padding: '8px 10px',
    borderBottom: '2px solid #0f3460',
    color: '#e94560',
    fontWeight: 600,
  },
  td: {
    padding: '6px 10px',
    borderBottom: '1px solid rgba(15,52,96,0.5)',
    color: '#e0e0e0',
  },
  empty: {
    color: '#8892b0',
    fontSize: 13,
    padding: 20,
  },
  loading: {
    color: '#8892b0',
    fontSize: 12,
    padding: '4px 8px',
  },
  error: {
    color: '#e94560',
    fontSize: 12,
    padding: 8,
  },
};

export default function DatabaseBrowser() {
  const [databases, setDatabases] = useState([]);
  const [expanded, setExpanded] = useState({});
  const [schemas, setSchemas] = useState({});
  const [tables, setTables] = useState({});
  const [views, setViews] = useState({});
  const [semanticViews, setSemanticViews] = useState([]);
  const [selected, setSelected] = useState(null);
  const [columns, setColumns] = useState([]);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    fetchDatabases()
      .then((data) => setDatabases(data.databases || []))
      .catch((err) => setError(err.message));
    fetchSemanticViews()
      .then((data) => setSemanticViews(data.semantic_views || []))
      .catch(() => {});
  }, []);

  const toggle = useCallback(async (key, loader) => {
    setExpanded((prev) => {
      const next = { ...prev };
      if (next[key]) {
        delete next[key];
      } else {
        next[key] = true;
        loader();
      }
      return next;
    });
  }, []);

  const loadSchemas = async (db) => {
    try {
      const data = await fetchSchemas(db);
      setSchemas((prev) => ({ ...prev, [db]: data.schemas || [] }));
    } catch (err) {
      setError(err.message);
    }
  };

  const loadTables = async (db, schema) => {
    const key = `${db}.${schema}`;
    try {
      const [tData, vData] = await Promise.all([
        fetchTables(db, schema),
        fetchViews(db, schema),
      ]);
      setTables((prev) => ({ ...prev, [key]: tData.tables || [] }));
      setViews((prev) => ({ ...prev, [key]: vData.views || [] }));
    } catch (err) {
      setError(err.message);
    }
  };

  const selectObject = async (db, schema, name, type) => {
    const fqn = `${db}.${schema}.${name}`;
    setSelected({ db, schema, name, type, fqn });
    setLoadingDetail(true);
    setColumns([]);
    try {
      const data = await executeQuery(`DESCRIBE TABLE ${fqn}`);
      setColumns(data.results || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoadingDetail(false);
    }
  };

  return (
    <div style={styles.container}>
      <div style={styles.tree}>
        <div style={styles.section}>
          <div style={styles.sectionTitle}>Databases</div>
          {databases.map((db) => {
            const dbName = db.name || db;
            const dbKey = `db:${dbName}`;
            return (
              <div key={dbName}>
                <div
                  style={styles.node(0, false)}
                  onClick={() => toggle(dbKey, () => loadSchemas(dbName))}
                >
                  <span style={styles.icon}>{expanded[dbKey] ? '▼' : '▶'}</span>
                  {dbName}
                </div>
                {expanded[dbKey] && schemas[dbName]?.map((s) => {
                  const sName = s.name || s;
                  const sKey = `schema:${dbName}.${sName}`;
                  return (
                    <div key={sName}>
                      <div
                        style={styles.node(1, false)}
                        onClick={() => toggle(sKey, () => loadTables(dbName, sName))}
                      >
                        <span style={styles.icon}>{expanded[sKey] ? '▼' : '▶'}</span>
                        {sName}
                      </div>
                      {expanded[sKey] && (
                        <>
                          {(tables[`${dbName}.${sName}`] || []).map((t) => {
                            const tName = t.name || t;
                            return (
                              <div
                                key={`t:${tName}`}
                                style={styles.node(2, selected?.fqn === `${dbName}.${sName}.${tName}`)}
                                onClick={() => selectObject(dbName, sName, tName, 'TABLE')}
                              >
                                <span style={styles.icon}>T</span>
                                {tName}
                                <span style={styles.badge('TABLE')}>TBL</span>
                              </div>
                            );
                          })}
                          {(views[`${dbName}.${sName}`] || []).map((v) => {
                            const vName = v.name || v;
                            return (
                              <div
                                key={`v:${vName}`}
                                style={styles.node(2, selected?.fqn === `${dbName}.${sName}.${vName}`)}
                                onClick={() => selectObject(dbName, sName, vName, 'VIEW')}
                              >
                                <span style={styles.icon}>V</span>
                                {vName}
                                <span style={styles.badge('VIEW')}>VIEW</span>
                              </div>
                            );
                          })}
                        </>
                      )}
                    </div>
                  );
                })}
              </div>
            );
          })}
          {databases.length === 0 && (
            <div style={styles.loading}>Loading databases...</div>
          )}
        </div>

        <div style={styles.section}>
          <div style={styles.sectionTitle}>Semantic Views</div>
          {semanticViews.map((sv) => {
            const svName = sv.name || sv;
            return (
              <div
                key={svName}
                style={styles.node(0, selected?.fqn === svName)}
                onClick={() => setSelected({ fqn: svName, name: svName, type: 'SEMANTIC' })}
              >
                <span style={styles.icon}>S</span>
                {svName}
                <span style={styles.badge('SEMANTIC')}>SEM</span>
              </div>
            );
          })}
          {semanticViews.length === 0 && (
            <div style={styles.loading}>No semantic views found</div>
          )}
        </div>

        {error && <div style={styles.error}>{error}</div>}
      </div>

      <div style={styles.detail}>
        {!selected && (
          <div style={styles.empty}>Select a table or view to see its details.</div>
        )}
        {selected && (
          <>
            <h3 style={{ margin: '0 0 4px', color: '#e94560', fontSize: 16 }}>
              {selected.fqn}
            </h3>
            <div style={{ color: '#8892b0', fontSize: 12, marginBottom: 16 }}>
              Type: {selected.type}
            </div>
            {loadingDetail && <div style={styles.loading}>Loading columns...</div>}
            {columns.length > 0 && (
              <table style={styles.table}>
                <thead>
                  <tr>
                    {Object.keys(columns[0]).map((k) => (
                      <th key={k} style={styles.th}>{k}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {columns.map((row, i) => (
                    <tr key={i}>
                      {Object.values(row).map((v, j) => (
                        <td key={j} style={styles.td}>{String(v ?? '')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </div>
    </div>
  );
}
