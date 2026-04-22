import { useState, useCallback } from 'react';
import { geocodeAddress, reverseGeocode } from '../api/client';
import { downloadCsv, downloadJson, printTable } from '../utils/export';

const styles = {
  panel: {
    background: '#16213e',
    borderRadius: 8,
    padding: 16,
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
  },
  label: {
    fontSize: 12,
    color: '#8892b0',
    marginBottom: 4,
    display: 'block',
  },
  input: {
    width: '100%',
    padding: '8px 10px',
    background: '#1a1a2e',
    border: '1px solid #0f3460',
    borderRadius: 4,
    color: '#e0e0e0',
    fontSize: 13,
    outline: 'none',
    boxSizing: 'border-box',
  },
  row: {
    display: 'flex',
    gap: 8,
    alignItems: 'flex-end',
  },
  col: {
    flex: 1,
  },
  btn: {
    padding: '8px 16px',
    background: '#e94560',
    color: '#fff',
    border: 'none',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 13,
    fontWeight: 600,
    whiteSpace: 'nowrap',
  },
  btnSecondary: {
    padding: '8px 16px',
    background: '#0f3460',
    color: '#e0e0e0',
    border: '1px solid #e94560',
    borderRadius: 4,
    cursor: 'pointer',
    fontSize: 13,
  },
  slider: {
    width: '100%',
    accentColor: '#e94560',
  },
  checkbox: {
    display: 'flex',
    alignItems: 'center',
    gap: 6,
    fontSize: 13,
    color: '#e0e0e0',
    cursor: 'pointer',
  },
  error: {
    color: '#e94560',
    fontSize: 12,
  },
};

const SOURCE_OPTIONS = [
  { id: 'zip_codes', label: 'Zip Codes' },
  { id: 'addresses', label: 'Addresses' },
  { id: 'demo_data', label: 'DEMO Data' },
];

export default function SearchPanel({ onSearch, onLocationChange, results }) {
  const [address, setAddress] = useState('');
  const [lat, setLat] = useState('');
  const [lon, setLon] = useState('');
  const [radius, setRadius] = useState(10);
  const [sources, setSources] = useState(['zip_codes', 'addresses', 'demo_data']);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const toggleSource = (id) => {
    setSources((prev) =>
      prev.includes(id) ? prev.filter((s) => s !== id) : [...prev, id]
    );
  };

  const handleGeocode = useCallback(async () => {
    if (!address.trim()) return;
    setLoading(true);
    setError('');
    try {
      const data = await geocodeAddress(address);
      setLat(String(data.lat));
      setLon(String(data.lon));
      if (onLocationChange) onLocationChange(data.lat, data.lon);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [address, onLocationChange]);

  const handleSearch = useCallback(() => {
    const la = parseFloat(lat);
    const lo = parseFloat(lon);
    if (isNaN(la) || isNaN(lo)) {
      setError('Enter valid coordinates or geocode an address first.');
      return;
    }
    setError('');
    onSearch(la, lo, radius * 1000, sources);
  }, [lat, lon, radius, sources, onSearch]);

  return (
    <div style={styles.panel}>
      <div>
        <label style={styles.label}>Address Search</label>
        <div style={styles.row}>
          <div style={{ flex: 1 }}>
            <input
              style={styles.input}
              placeholder="Enter address to geocode..."
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              onKeyDown={(e) => e.key === 'Enter' && handleGeocode()}
            />
          </div>
          <button style={styles.btn} onClick={handleGeocode} disabled={loading}>
            {loading ? 'Geocoding...' : 'Geocode'}
          </button>
        </div>
      </div>

      <div style={styles.row}>
        <div style={styles.col}>
          <label style={styles.label}>Latitude</label>
          <input
            style={styles.input}
            type="number"
            step="any"
            placeholder="39.8"
            value={lat}
            onChange={(e) => setLat(e.target.value)}
          />
        </div>
        <div style={styles.col}>
          <label style={styles.label}>Longitude</label>
          <input
            style={styles.input}
            type="number"
            step="any"
            placeholder="-98.6"
            value={lon}
            onChange={(e) => setLon(e.target.value)}
          />
        </div>
      </div>

      <div>
        <label style={styles.label}>Radius: {radius} km</label>
        <input
          type="range"
          min={1}
          max={50}
          value={radius}
          onChange={(e) => setRadius(Number(e.target.value))}
          style={styles.slider}
        />
      </div>

      <div>
        <label style={styles.label}>Data Sources</label>
        <div style={{ display: 'flex', gap: 16 }}>
          {SOURCE_OPTIONS.map((s) => (
            <label key={s.id} style={styles.checkbox}>
              <input
                type="checkbox"
                checked={sources.includes(s.id)}
                onChange={() => toggleSource(s.id)}
              />
              {s.label}
            </label>
          ))}
        </div>
      </div>

      {error && <div style={styles.error}>{error}</div>}

      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <button style={styles.btn} onClick={handleSearch}>
          Search Nearby
        </button>
        {results && results.length > 0 && (
          <>
            <button style={styles.btnSecondary} onClick={() => downloadCsv(results, 'search_results.csv')}>
              CSV
            </button>
            <button style={styles.btnSecondary} onClick={() => downloadJson(results, 'search_results.json')}>
              JSON
            </button>
            <button style={styles.btnSecondary} onClick={() => printTable(results, 'Search Results')}>
              Print
            </button>
          </>
        )}
      </div>
    </div>
  );
}
