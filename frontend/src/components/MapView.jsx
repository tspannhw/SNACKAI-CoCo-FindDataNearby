import { useState, useCallback, useRef, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup, useMapEvents, useMap } from 'react-leaflet';
import L from 'leaflet';
import SearchPanel from './SearchPanel';
import { searchNearby, reverseGeocode } from '../api/client';

// Fix default marker icon issue with bundlers
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
});

const redIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41],
});

function makeIcon(color) {
  return new L.Icon({
    iconUrl: `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-${color}.png`,
    shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
    iconSize: [25, 41],
    iconAnchor: [12, 41],
    popupAnchor: [1, -34],
    shadowSize: [41, 41],
  });
}

const SOURCE_ICONS = {
  zip_code: makeIcon('blue'),
  address: makeIcon('blue'),
  weather_station: makeIcon('green'),
  air_quality: makeIcon('orange'),
  traffic_event: makeIcon('red'),
  camera: makeIcon('violet'),
  aircraft: makeIcon('grey'),
  iot_node: makeIcon('gold'),
  ghost_sighting: makeIcon('red'),
  ghost_sensor: makeIcon('violet'),
  ghost_office: makeIcon('green'),
  ghost_threat: makeIcon('orange'),
  ghost_mission: makeIcon('gold'),
  subway_station: makeIcon('blue'),
  bus_position: makeIcon('green'),
};

const SOURCE_LABELS = {
  zip_code: 'Zip Code',
  address: 'Address',
  weather_station: 'Weather Station',
  air_quality: 'Air Quality Monitor',
  traffic_event: 'Traffic Event',
  camera: 'Camera',
  aircraft: 'Aircraft (ADS-B)',
  iot_node: 'IoT Node',
  ghost_sighting: 'Ghost Sighting',
  ghost_sensor: 'Ghost Sensor',
  ghost_office: 'Ghost Office',
  ghost_threat: 'OSINT Threat',
  ghost_mission: 'Ghost Mission',
  subway_station: 'Subway Station',
  bus_position: 'Bus Position',
};

function formatDistance(meters) {
  if (meters == null) return '';
  const m = Number(meters);
  if (m < 1000) return `${Math.round(m)} m`;
  return `${(m / 1000).toFixed(1)} km`;
}

function googleMapsUrl(lat, lon) {
  return `https://www.google.com/maps?q=${lat},${lon}`;
}

const styles = {
  container: {
    display: 'flex',
    height: 'calc(100vh - 90px)',
  },
  sidebar: {
    width: 340,
    background: '#16213e',
    borderRight: '1px solid #0f3460',
    overflowY: 'auto',
    padding: 12,
    display: 'flex',
    flexDirection: 'column',
    gap: 12,
  },
  mapWrap: {
    flex: 1,
    position: 'relative',
  },
  results: {
    background: '#1a1a2e',
    borderRadius: 8,
    padding: 12,
    maxHeight: 300,
    overflowY: 'auto',
  },
  resultsTitle: {
    fontSize: 13,
    fontWeight: 600,
    color: '#e94560',
    marginBottom: 8,
  },
  table: {
    width: '100%',
    borderCollapse: 'collapse',
    fontSize: 11,
  },
  th: {
    textAlign: 'left',
    padding: '4px 6px',
    borderBottom: '1px solid #0f3460',
    color: '#8892b0',
    fontWeight: 600,
  },
  td: {
    padding: '4px 6px',
    borderBottom: '1px solid rgba(15,52,96,0.4)',
    color: '#e0e0e0',
    maxWidth: 120,
    overflow: 'hidden',
    textOverflow: 'ellipsis',
    whiteSpace: 'nowrap',
  },
  clickHint: {
    fontSize: 11,
    color: '#8892b0',
    textAlign: 'center',
    padding: 8,
  },
  locationInfo: {
    background: '#1a1a2e',
    borderRadius: 6,
    padding: '8px 10px',
    fontSize: 12,
    color: '#8892b0',
  },
};

function MapClickHandler({ onClick }) {
  useMapEvents({
    click: (e) => onClick(e.latlng),
  });
  return null;
}

function FlyTo({ center }) {
  const map = useMap();
  useEffect(() => {
    if (center) map.flyTo(center, 10, { duration: 1 });
  }, [center, map]);
  return null;
}

function InvalidateSize({ visible }) {
  const map = useMap();
  useEffect(() => {
    if (visible) {
      // Small delay lets the browser finish the display:block layout pass
      const t = setTimeout(() => map.invalidateSize(), 100);
      return () => clearTimeout(t);
    }
  }, [visible, map]);
  return null;
}

export default function MapView({ visible = true }) {
  const [results, setResults] = useState([]);
  const [markers, setMarkers] = useState([]);
  const [clickedPos, setClickedPos] = useState(null);
  const [flyTarget, setFlyTarget] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [locationLabel, setLocationLabel] = useState('');

  const handleMapClick = useCallback(async (latlng) => {
    setClickedPos(latlng);
    try {
      const data = await reverseGeocode(latlng.lat, latlng.lng);
      setLocationLabel(data.display_name || `${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`);
    } catch {
      setLocationLabel(`${latlng.lat.toFixed(4)}, ${latlng.lng.toFixed(4)}`);
    }
  }, []);

  const handleSearch = useCallback(async (lat, lon, radiusMeters, sources) => {
    setLoading(true);
    setError('');
    try {
      const data = await searchNearby(lat, lon, radiusMeters, sources);
      const items = Array.isArray(data.results) ? data.results : [];
      setResults(items);
      setMarkers(
        items
          .filter((r) => r.lat && r.lon)
          .map((r, i) => ({
            key: i,
            position: [r.lat, r.lon],
            label: r.name || r.label || `Result ${i + 1}`,
            source: r.source || 'unknown',
            details: r,
          }))
      );
    } catch (err) {
      setError(err.message);
      setResults([]);
      setMarkers([]);
    } finally {
      setLoading(false);
    }
  }, []);

  const handleLocationChange = useCallback((lat, lon) => {
    setFlyTarget([lat, lon]);
    setClickedPos({ lat, lng: lon });
  }, []);

  return (
    <div style={styles.container}>
      <div style={styles.sidebar}>
        <SearchPanel
          onSearch={handleSearch}
          onLocationChange={handleLocationChange}
          results={results}
        />

        {clickedPos && (
          <div style={styles.locationInfo}>
            <strong>Selected:</strong> {locationLabel || `${clickedPos.lat.toFixed(4)}, ${clickedPos.lng.toFixed(4)}`}
          </div>
        )}

        {error && (
          <div style={{ color: '#e94560', fontSize: 12, padding: 8 }}>{error}</div>
        )}

        {loading && (
          <div style={{ color: '#8892b0', fontSize: 12, textAlign: 'center', padding: 8 }}>
            Searching...
          </div>
        )}

        {results.length > 0 && (
          <div style={styles.results}>
            <div style={styles.resultsTitle}>{results.length} result{results.length !== 1 ? 's' : ''} found</div>
            <table style={styles.table}>
              <thead>
                <tr>
                  <th style={styles.th}>Source</th>
                  <th style={styles.th}>Name</th>
                  <th style={styles.th}>Distance</th>
                  <th style={styles.th}>Table</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => (
                  <tr
                    key={i}
                    style={{cursor: 'pointer'}}
                    onClick={() => {
                      if (r.lat && r.lon) {
                        setFlyTarget([r.lat, r.lon]);
                      }
                    }}
                    onMouseEnter={(e) => { e.currentTarget.style.background = 'rgba(233,69,96,0.15)'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}
                  >
                    <td style={styles.td}>
                      <span style={{
                        display: 'inline-block',
                        background: '#0f3460',
                        color: '#e0e0e0',
                        borderRadius: 3,
                        padding: '1px 5px',
                        fontSize: 10,
                        whiteSpace: 'nowrap',
                      }}>
                        {SOURCE_LABELS[r.source] || r.source}
                      </span>
                    </td>
                    <td style={styles.td} title={r.name}>{r.name}</td>
                    <td style={{...styles.td, whiteSpace: 'nowrap'}}>{formatDistance(r.distance_meters)}</td>
                    <td style={{...styles.td, fontSize: 9, fontFamily: 'monospace', maxWidth: 90}} title={r.table}>
                      {r.table ? r.table.split('.').pop() : ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        <div style={styles.clickHint}>Click the map to select a location</div>
      </div>

      <div style={styles.mapWrap}>
        <MapContainer
          center={[39.8, -98.6]}
          zoom={4}
          style={{ height: '100%', width: '100%' }}
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <MapClickHandler onClick={handleMapClick} />
          <InvalidateSize visible={visible} />
          {flyTarget && <FlyTo center={flyTarget} />}
          {clickedPos && (
            <Marker position={[clickedPos.lat, clickedPos.lng]} icon={redIcon}>
              <Popup>
                <strong>Search Point</strong><br />
                {clickedPos.lat.toFixed(5)}, {clickedPos.lng.toFixed(5)}<br />
                <a
                  href={googleMapsUrl(clickedPos.lat, clickedPos.lng)}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{fontSize: 11, color: '#1a73e8', textDecoration: 'none'}}
                >
                  Open in Google Maps &rarr;
                </a>
              </Popup>
            </Marker>
          )}
          {markers.map((m) => (
            <Marker key={m.key} position={m.position} icon={SOURCE_ICONS[m.source] || makeIcon('blue')}>
              <Popup maxWidth={320} minWidth={200}>
                <div style={{fontSize: 13, lineHeight: 1.5}}>
                  <strong style={{fontSize: 14}}>{m.label}</strong><br />
                  <span style={{
                    display: 'inline-block',
                    background: '#e8f0fe',
                    color: '#1a73e8',
                    borderRadius: 3,
                    padding: '1px 6px',
                    fontSize: 11,
                    fontWeight: 600,
                    marginTop: 2,
                    marginBottom: 4,
                  }}>
                    {SOURCE_LABELS[m.source] || m.source}
                  </span>
                  {m.details.description && (
                    <div style={{color: '#444', fontSize: 12, margin: '4px 0'}}>
                      {m.details.description}
                    </div>
                  )}
                  {m.details.distance_meters != null && (
                    <div style={{color: '#555', fontSize: 12}}>
                      <strong>Distance:</strong> {formatDistance(m.details.distance_meters)}
                    </div>
                  )}
                  <div style={{color: '#555', fontSize: 11, margin: '3px 0'}}>
                    <strong>Lat/Lon:</strong> {m.position[0]?.toFixed(5)}, {m.position[1]?.toFixed(5)}
                  </div>
                  {m.details.table && (
                    <div style={{
                      color: '#666',
                      fontSize: 10,
                      fontFamily: 'monospace',
                      background: '#f5f5f5',
                      borderRadius: 3,
                      padding: '2px 5px',
                      margin: '4px 0',
                      wordBreak: 'break-all',
                    }}>
                      {m.details.table}
                    </div>
                  )}
                  {Object.entries(m.details)
                    .filter(([k]) => !['lat', 'lon', 'name', 'label', 'source', 'table', 'description', 'distance_meters'].includes(k))
                    .slice(0, 4)
                    .map(([k, v]) => (
                      <div key={k} style={{color: '#555', fontSize: 11}}>
                        <strong>{k}:</strong> {String(v)}
                      </div>
                    ))}
                  <a
                    href={googleMapsUrl(m.position[0], m.position[1])}
                    target="_blank"
                    rel="noopener noreferrer"
                    style={{
                      display: 'inline-block',
                      marginTop: 6,
                      fontSize: 11,
                      color: '#1a73e8',
                      textDecoration: 'none',
                    }}
                  >
                    Open in Google Maps &rarr;
                  </a>
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
    </div>
  );
}
