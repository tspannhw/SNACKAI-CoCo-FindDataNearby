"""Find Data Nearby - REST API.

Flask application providing endpoints for Snowflake metadata browsing,
geo-spatial search, geocoding, and Cortex AI chat.

Start with:
    python app.py
or:
    flask run --port 5001
"""

import logging
import os
import re
import sys

from flask import Flask, jsonify, request
from flask_cors import CORS

from nominatim_client import NominatimClient
from snowflake_client import SnowflakeClient

# ── Logging ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.DEBUG if os.getenv("FLASK_ENV") == "development" else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# ── App setup ──────────────────────────────────────────────────────────

app = Flask(__name__)
CORS(app)

nominatim = NominatimClient()

# Regex that read-only queries must match (case-insensitive, leading whitespace ok)
_READ_ONLY_RE = re.compile(
    r"^\s*(SELECT|SHOW|DESCRIBE|DESC|WITH|EXPLAIN)\b",
    re.IGNORECASE,
)


def _sf() -> SnowflakeClient:
    """Create a new SnowflakeClient (used inside request context)."""
    return SnowflakeClient()


def _error(message: str, status: int = 400) -> tuple:
    """Return a JSON error response."""
    return jsonify({"error": message}), status


# ── Health ─────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    """Health check endpoint."""
    return jsonify({"status": "ok", "service": "find-data-nearby-api"})


# ── Database metadata ──────────────────────────────────────────────────

@app.route("/api/databases", methods=["GET"])
def list_databases():
    """List all databases visible to the current role."""
    try:
        with _sf() as sf:
            rows = sf.list_databases()
        return jsonify({"databases": rows})
    except Exception as exc:
        logger.exception("list_databases failed")
        return _error(str(exc), 500)


@app.route("/api/databases/<database>/schemas", methods=["GET"])
def list_schemas(database: str):
    """List schemas in a database."""
    try:
        with _sf() as sf:
            rows = sf.list_schemas(database)
        return jsonify({"schemas": rows})
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.exception("list_schemas failed")
        return _error(str(exc), 500)


@app.route("/api/databases/<database>/schemas/<schema>/tables", methods=["GET"])
def list_tables(database: str, schema: str):
    """List tables in a schema."""
    try:
        with _sf() as sf:
            rows = sf.list_tables(database, schema)
        return jsonify({"tables": rows})
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.exception("list_tables failed")
        return _error(str(exc), 500)


@app.route("/api/databases/<database>/schemas/<schema>/views", methods=["GET"])
def list_views(database: str, schema: str):
    """List views in a schema."""
    try:
        with _sf() as sf:
            rows = sf.list_views(database, schema)
        return jsonify({"views": rows})
    except ValueError as exc:
        return _error(str(exc), 400)
    except Exception as exc:
        logger.exception("list_views failed")
        return _error(str(exc), 500)


@app.route("/api/semantic-views", methods=["GET"])
def list_semantic_views():
    """List all semantic views in the account."""
    try:
        with _sf() as sf:
            rows = sf.list_semantic_views()
        return jsonify({"semantic_views": rows})
    except Exception as exc:
        logger.exception("list_semantic_views failed")
        return _error(str(exc), 500)


# ── Query execution ───────────────────────────────────────────────────

@app.route("/api/query", methods=["POST"])
def execute_query():
    """Execute a read-only SQL query.

    Body: {"sql": "SELECT ..."}
    Only SELECT, SHOW, DESCRIBE, WITH, and EXPLAIN statements are allowed.
    """
    body = request.get_json(silent=True)
    if not body or "sql" not in body:
        return _error("Request body must include 'sql'.")

    sql = body["sql"].strip()
    if not _READ_ONLY_RE.match(sql):
        return _error(
            "Only SELECT, SHOW, DESCRIBE, WITH, and EXPLAIN statements are allowed.",
            403,
        )

    try:
        with _sf() as sf:
            rows = sf.execute_query(sql)
        return jsonify({"results": rows, "row_count": len(rows)})
    except Exception as exc:
        logger.exception("execute_query failed")
        return _error(str(exc), 500)


# ── Geo search ─────────────────────────────────────────────────────────

@app.route("/api/search/nearby", methods=["POST"])
def search_nearby():
    """Search for nearby data in Snowflake.

    Body: {
        "lat": float,
        "lon": float,
        "radius_meters": float,
        "sources": ["zip_codes", "addresses", "demo_data"]
    }

    Returns a flat array of results normalised to {source, name, lat, lon, distance_meters, ...}.
    """
    body = request.get_json(silent=True)
    if not body:
        return _error("JSON body required.")

    try:
        lat = float(body["lat"])
        lon = float(body["lon"])
        radius_meters = float(body.get("radius_meters", 1000))
    except (KeyError, TypeError, ValueError) as exc:
        return _error(f"Invalid parameters: {exc}")

    sources = body.get("sources", ["zip_codes", "addresses", "demo_data"])
    valid_sources = {"zip_codes", "addresses", "demo_data"}
    if not set(sources).issubset(valid_sources):
        return _error(f"Invalid sources. Allowed: {sorted(valid_sources)}")

    # Map source type to the Snowflake table it comes from
    SOURCE_TABLE = {
        "zip_code": "U_S__ZIP_CODE_METADATA.ZIP_DEMOGRAPHICS.ZIP_CODE_METADATA",
        "address": "WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS",
        "weather_station": "DEMO.DEMO.WEATHER_STATIONS_GEO",
        "air_quality": "DEMO.DEMO.AIR_QUALITY_MONITORS_GEO",
        "traffic_event": "DEMO.DEMO.NYCTRAFFICEVENTS",
        "camera": "DEMO.DEMO.CAMERAS",
        "aircraft": "DEMO.DEMO.ADSB_CURRENT_AIRCRAFT",
        "iot_node": "DEMO.DEMO.MESHTASTIC_ACTIVE_NODES",
        "ghost_sighting": "GHOST_DETECTION.APP.GHOST_SIGHTINGS",
        "ghost_sensor": "GHOST_DETECTION.APP.SENSOR_FUSION_DATA",
        "ghost_office": "GHOST_DETECTION.APP.OFFICES",
        "ghost_threat": "GHOST_DETECTION.APP.OSINT_THREAT_FEED",
        "ghost_mission": "GHOST_DETECTION.APP.MISSION_CONTROL_LOG",
        "subway_station": "NYC_TRANSIT.RAW_DATA.SUBWAY_STATIONS",
        "bus_position": "NYC_TRANSIT.CURATED.BUS_POSITIONS_LATEST",
    }

    flat: list[dict] = []
    try:
        with _sf() as sf:
            if "zip_codes" in sources:
                for row in sf.search_nearby_zip_codes(lat, lon, radius_meters):
                    flat.append({
                        "source": "zip_code",
                        "table": SOURCE_TABLE["zip_code"],
                        "name": f"{row.get('ZIP', '')} - {row.get('CITY', '')}, {row.get('STATE', '')}",
                        "lat": row.get("LATITUDE"),
                        "lon": row.get("LONGITUDE"),
                        "distance_meters": row.get("DISTANCE_METERS"),
                        "zip": row.get("ZIP"),
                        "city": row.get("CITY"),
                        "state": row.get("STATE"),
                    })
            if "addresses" in sources:
                limit = int(body.get("limit", 50))
                for row in sf.search_nearby_addresses(lat, lon, radius_meters, limit):
                    flat.append({
                        "source": "address",
                        "table": SOURCE_TABLE["address"],
                        "name": " ".join(filter(None, [
                            row.get("STREET"), row.get("CITY"),
                            row.get("REGION"), row.get("POSTCODE"),
                        ])),
                        "lat": row.get("LATITUDE"),
                        "lon": row.get("LONGITUDE"),
                        "distance_meters": row.get("DISTANCE_METERS"),
                        "street": row.get("STREET"),
                        "city": row.get("CITY"),
                        "postcode": row.get("POSTCODE"),
                    })
            if "demo_data" in sources:
                for row in sf.search_nearby_demo_data(lat, lon, radius_meters):
                    src = row.get("SOURCE", "demo")
                    flat.append({
                        "source": src,
                        "table": SOURCE_TABLE.get(src, ""),
                        "name": row.get("NAME", ""),
                        "description": row.get("DESCRIPTION", ""),
                        "lat": row.get("LATITUDE"),
                        "lon": row.get("LONGITUDE"),
                        "distance_meters": row.get("DISTANCE_METERS"),
                    })
    except Exception as exc:
        logger.exception("search_nearby failed")
        return _error(str(exc), 500)

    flat.sort(key=lambda r: r.get("distance_meters") or 999999999)
    return jsonify({"results": flat})


# ── Geocoding (Nominatim) ─────────────────────────────────────────────

@app.route("/api/geocode", methods=["POST"])
def geocode():
    """Forward-geocode an address string.

    Body: {"address": "1600 Amphitheatre Parkway, Mountain View, CA"}
    """
    body = request.get_json(silent=True)
    if not body or "address" not in body:
        return _error("Request body must include 'address'.")

    result = nominatim.geocode(body["address"])
    if result is None:
        return _error("Address not found.", 404)
    return jsonify(result)


@app.route("/api/reverse-geocode", methods=["POST"])
def reverse_geocode():
    """Reverse-geocode coordinates to an address.

    Body: {"lat": float, "lon": float}
    """
    body = request.get_json(silent=True)
    if not body:
        return _error("JSON body required.")

    try:
        lat = float(body["lat"])
        lon = float(body["lon"])
    except (KeyError, TypeError, ValueError) as exc:
        return _error(f"Invalid parameters: {exc}")

    result = nominatim.reverse_geocode(lat, lon)
    if result is None:
        return _error("Location not found.", 404)
    return jsonify(result)


# ── Cortex AI chat ─────────────────────────────────────────────────────

@app.route("/api/chat", methods=["POST"])
def chat():
    """Chat with Snowflake Cortex AI.

    Body: {"message": "...", "model": "llama3.1-8b"}   // model is optional
    """
    body = request.get_json(silent=True)
    if not body or "message" not in body:
        return _error("Request body must include 'message'.")

    message = body["message"].strip()
    if not message:
        return _error("Message cannot be empty.")

    model = body.get("model", "llama3.1-8b")

    try:
        with _sf() as sf:
            rows = sf.cortex_complete(message, model=model)
        response_text = rows[0].get("RESPONSE") or rows[0].get("response", "") if rows else ""
        return jsonify({"response": response_text, "model": model})
    except Exception as exc:
        logger.exception("chat failed")
        return _error(str(exc), 500)


# ── Main ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_ENV") == "development"
    logger.info("Starting Find Data Nearby API on port %d (debug=%s)", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug)
