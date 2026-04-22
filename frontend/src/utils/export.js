/**
 * Shared export/download/print helpers for Find Data Nearby.
 */

/** Trigger a file download in the browser. */
function downloadBlob(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

/**
 * Export an array of objects as CSV and trigger download.
 * @param {Object[]} rows - Array of flat objects.
 * @param {string} [filename='export.csv']
 */
export function downloadCsv(rows, filename = 'export.csv') {
  if (!rows || rows.length === 0) return;
  const keys = Object.keys(rows[0]);
  const header = keys.join(',');
  const body = rows.map((r) =>
    keys.map((k) => `"${String(r[k] ?? '').replace(/"/g, '""')}"`).join(',')
  );
  const csv = [header, ...body].join('\n');
  downloadBlob(new Blob([csv], { type: 'text/csv' }), filename);
}

/**
 * Export an array of objects (or any value) as JSON and trigger download.
 * @param {*} data - Data to serialize.
 * @param {string} [filename='export.json']
 */
export function downloadJson(data, filename = 'export.json') {
  if (!data) return;
  const json = JSON.stringify(data, null, 2);
  downloadBlob(new Blob([json], { type: 'application/json' }), filename);
}

/**
 * Open a print-friendly window with an HTML table of results.
 * @param {Object[]} rows - Array of flat objects.
 * @param {string} [title='Find Data Nearby']
 */
export function printTable(rows, title = 'Find Data Nearby') {
  if (!rows || rows.length === 0) return;
  const keys = Object.keys(rows[0]);
  const ths = keys.map((k) => `<th style="border:1px solid #ccc;padding:4px 8px;background:#f5f5f5">${k}</th>`).join('');
  const trs = rows.map((r) =>
    '<tr>' + keys.map((k) => `<td style="border:1px solid #eee;padding:4px 8px">${String(r[k] ?? '')}</td>`).join('') + '</tr>'
  ).join('');
  const html = `<!DOCTYPE html><html><head><title>${title}</title></head><body>
<h2>${title}</h2><p>${rows.length} row${rows.length !== 1 ? 's' : ''}</p>
<table style="border-collapse:collapse;font-family:sans-serif;font-size:12px">
<thead><tr>${ths}</tr></thead><tbody>${trs}</tbody></table>
<script>window.print();<\/script></body></html>`;
  const w = window.open('', '_blank');
  if (w) { w.document.write(html); w.document.close(); }
}

/**
 * Open a print-friendly window with plain text content (e.g., chat transcript).
 * @param {string} text - Text content to print.
 * @param {string} [title='Find Data Nearby']
 */
export function printText(text, title = 'Find Data Nearby') {
  if (!text) return;
  const escaped = text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  const html = `<!DOCTYPE html><html><head><title>${title}</title></head><body>
<h2>${title}</h2><pre style="font-family:sans-serif;font-size:13px;line-height:1.6;white-space:pre-wrap">${escaped}</pre>
<script>window.print();<\/script></body></html>`;
  const w = window.open('', '_blank');
  if (w) { w.document.write(html); w.document.close(); }
}
