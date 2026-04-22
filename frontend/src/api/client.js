const BASE = '/api';

async function request(url, options = {}) {
  const res = await fetch(`${BASE}${url}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `Request failed: ${res.status}`);
  }
  return res.json();
}

export function fetchDatabases() {
  return request('/databases');
}

export function fetchSchemas(db) {
  return request(`/databases/${encodeURIComponent(db)}/schemas`);
}

export function fetchTables(db, schema) {
  return request(`/databases/${encodeURIComponent(db)}/schemas/${encodeURIComponent(schema)}/tables`);
}

export function fetchViews(db, schema) {
  return request(`/databases/${encodeURIComponent(db)}/schemas/${encodeURIComponent(schema)}/views`);
}

export function fetchSemanticViews() {
  return request('/semantic-views');
}

export function executeQuery(sql) {
  return request('/query', {
    method: 'POST',
    body: JSON.stringify({ sql }),
  });
}

export function searchNearby(lat, lon, radiusMeters, sources) {
  return request('/search/nearby', {
    method: 'POST',
    body: JSON.stringify({ lat, lon, radius_meters: radiusMeters, sources }),
  });
}

export function geocodeAddress(address) {
  return request('/geocode', {
    method: 'POST',
    body: JSON.stringify({ address }),
  });
}

export function reverseGeocode(lat, lon) {
  return request('/reverse-geocode', {
    method: 'POST',
    body: JSON.stringify({ lat, lon }),
  });
}

export function sendChatMessage(message, model) {
  return request('/chat', {
    method: 'POST',
    body: JSON.stringify({ message, model }),
  });
}
