#!/usr/bin/env python3
"""
Find Data Nearby - MCP Server

Model Context Protocol server using stdin/stdout JSON-RPC transport.
Exposes geospatial search, geocoding, database browsing, query execution,
and Cortex AI chat as MCP tools.

Uses externalbrowser auth or SNOWFLAKE_CONNECTION_NAME (no passwords).

Register as:
    {"command": "python3", "args": ["/path/to/mcp/server.py"]}
"""

import json
import os
import re
import sys

import requests
import snowflake.connector

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SERVER_NAME = "findnearby-mcp"
SERVER_VERSION = "1.0.0"
PROTOCOL_VERSION = "2024-11-05"

NOMINATIM_BASE = "https://nominatim.openstreetmap.org"
NOMINATIM_HEADERS = {"User-Agent": "FindDataNearby-MCP/1.0"}

# Read-only SQL prefixes (upper-cased for comparison)
READONLY_PREFIXES = ("SELECT", "SHOW", "DESCRIBE", "DESC", "WITH", "EXPLAIN")

# Allowlist for SQL object identifiers (database, schema, table names)
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_$.]*$')


def _validate_identifier(name: str) -> str:
    """Validate a SQL identifier against the allowlist pattern."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


def _load_private_key(path):
    """Load a PEM private key file and return DER bytes for the connector."""
    from cryptography.hazmat.primitives import serialization
    with open(os.path.expanduser(path), "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None)
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _read_connection_config(conn_name):
    """Read a named connection from ~/.snowflake/connections.toml."""
    import tomllib
    toml_path = os.path.expanduser("~/.snowflake/connections.toml")
    if not os.path.exists(toml_path):
        return None
    with open(toml_path, "rb") as f:
        config = tomllib.load(f)
    return config.get(conn_name)


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

_sf_conn = None


def get_snowflake_connection():
    """Return a cached Snowflake connection.

    Tries SNOWFLAKE_CONNECTION_NAME first (Snow CLI), handling SNOWFLAKE_JWT
    key-pair auth automatically. Falls back to externalbrowser auth with
    SNOWFLAKE_ACCOUNT + SNOWFLAKE_USER.
    """
    global _sf_conn
    if _sf_conn is not None:
        try:
            _sf_conn.cursor().execute("SELECT 1")
            return _sf_conn
        except Exception:
            _sf_conn = None

    conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME")
    if conn_name:
        cfg = _read_connection_config(conn_name)
        if cfg and cfg.get("authenticator") == "SNOWFLAKE_JWT" and cfg.get("private_key_path"):
            pkb = _load_private_key(cfg["private_key_path"])
            _sf_conn = snowflake.connector.connect(
                account=cfg["account"],
                user=cfg["user"],
                private_key=pkb,
                role=cfg.get("role", "ACCOUNTADMIN"),
                warehouse=cfg.get("warehouse", "INGEST"),
                database=cfg.get("database"),
                schema=cfg.get("schema"),
            )
        else:
            _sf_conn = snowflake.connector.connect(connection_name=conn_name)
    else:
        account = os.environ.get("SNOWFLAKE_ACCOUNT")
        user = os.environ.get("SNOWFLAKE_USER")
        if not account or not user:
            raise RuntimeError(
                "Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars, "
                "or set SNOWFLAKE_CONNECTION_NAME for Snow CLI auth."
            )
        _sf_conn = snowflake.connector.connect(
            account=account,
            user=user,
            authenticator="externalbrowser",
        )

    # Set role and warehouse per project config
    _sf_conn.cursor().execute("USE ROLE ACCOUNTADMIN")
    _sf_conn.cursor().execute("USE WAREHOUSE INGEST")
    return _sf_conn


def execute_query(sql, params=None):
    """Execute SQL and return {"columns": [...], "rows": [...]}."""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        cols = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        return {
            "columns": cols,
            "rows": [
                {cols[i]: (str(v) if v is not None else None) for i, v in enumerate(row)}
                for row in rows
            ],
            "row_count": len(rows),
        }
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Nominatim helpers
# ---------------------------------------------------------------------------

def nominatim_geocode(address):
    """Forward geocode via Nominatim."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/search",
        params={"q": address, "format": "json", "limit": 5},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def nominatim_reverse(lat, lon):
    """Reverse geocode via Nominatim."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/reverse",
        params={"lat": lat, "lon": lon, "format": "json"},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Tool definitions
# ---------------------------------------------------------------------------

TOOLS = [
    {
        "name": "search_nearby",
        "description": "Search for geospatial data near a geographic point in Snowflake and external sources.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "lat": {"type": "number", "description": "Latitude"},
                "lon": {"type": "number", "description": "Longitude"},
                "radius_meters": {"type": "number", "description": "Search radius in meters", "default": 5000},
                "source": {
                    "type": "string",
                    "enum": ["zip_codes", "addresses", "demo_data", "all"],
                    "default": "all",
                    "description": "Data source to search",
                },
            },
            "required": ["lat", "lon"],
        },
    },
    {
        "name": "geocode_address",
        "description": "Forward geocode an address to lat/lon coordinates using Nominatim.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "address": {"type": "string", "description": "Address to geocode"},
            },
            "required": ["address"],
        },
    },
    {
        "name": "reverse_geocode",
        "description": "Reverse geocode lat/lon coordinates to an address using Nominatim.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "lat": {"type": "number", "description": "Latitude"},
                "lon": {"type": "number", "description": "Longitude"},
            },
            "required": ["lat", "lon"],
        },
    },
    {
        "name": "browse_databases",
        "description": "Browse Snowflake databases, schemas, and tables.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "database": {"type": "string", "description": "Database name (optional)"},
                "schema": {"type": "string", "description": "Schema name (optional, requires database)"},
            },
        },
    },
    {
        "name": "run_query",
        "description": "Run a read-only SQL query against Snowflake (SELECT, SHOW, DESCRIBE, WITH, EXPLAIN only).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SQL statement to execute"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "cortex_chat",
        "description": "Send a message to Snowflake Cortex AI and get a response.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "description": "Message to send to Cortex AI"},
                "model": {"type": "string", "description": "Cortex model name", "default": "llama3.1-8b"},
            },
            "required": ["message"],
        },
    },
    {
        "name": "list_semantic_views",
        "description": "List all semantic views available in the Snowflake account.",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
]


# ---------------------------------------------------------------------------
# Tool handlers
# ---------------------------------------------------------------------------

def handle_search_nearby(params):
    lat = params["lat"]
    lon = params["lon"]
    radius = params.get("radius_meters", 5000)
    source = params.get("source", "all")

    results = {"search_point": {"lat": lat, "lon": lon, "radius_meters": radius}}

    if source in ("zip_codes", "all"):
        results["zip_codes"] = execute_query(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.SEARCH_NEARBY_ZIP_CODES(%s::FLOAT, %s::FLOAT, %s::FLOAT))",
            (float(lat), float(lon), float(radius)),
        )

    if source in ("addresses", "all"):
        results["addresses"] = execute_query(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.SEARCH_NEARBY_ADDRESSES(%s::FLOAT, %s::FLOAT, %s::FLOAT, %s))",
            (float(lat), float(lon), float(radius), 50),
        )
        rev = nominatim_reverse(lat, lon)
        results["nominatim_reverse"] = rev

    if source in ("demo_data", "all"):
        results["demo_data"] = execute_query(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.SEARCH_NEARBY_DEMO_DATA(%s::FLOAT, %s::FLOAT, %s::FLOAT))",
            (float(lat), float(lon), float(radius)),
        )

    return results


def handle_geocode_address(params):
    return nominatim_geocode(params["address"])


def handle_reverse_geocode(params):
    return nominatim_reverse(params["lat"], params["lon"])


def handle_browse_databases(params):
    db = params.get("database")
    schema = params.get("schema")
    if db and schema:
        db = _validate_identifier(db)
        schema = _validate_identifier(schema)
        sql = f"SHOW TABLES IN {db}.{schema}"
    elif db:
        db = _validate_identifier(db)
        sql = f"SHOW SCHEMAS IN DATABASE {db}"
    else:
        sql = "SHOW DATABASES"
    return execute_query(sql)


def handle_run_query(params):
    sql = params["sql"].strip()
    first_word = sql.split()[0].upper() if sql else ""
    if first_word not in READONLY_PREFIXES:
        raise ValueError(
            f"Only read-only queries are allowed ({', '.join(READONLY_PREFIXES)}). "
            f"Got: {first_word}"
        )
    return execute_query(sql)


def handle_cortex_chat(params):
    message = params["message"]
    model = params.get("model", "llama3.1-8b")
    result = execute_query(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS response",
        (model, message),
    )
    if result["rows"]:
        return {"response": result["rows"][0].get("RESPONSE", "")}
    return {"response": ""}


def handle_list_semantic_views(params):
    return execute_query("SHOW SEMANTIC VIEWS")


TOOL_HANDLERS = {
    "search_nearby": handle_search_nearby,
    "geocode_address": handle_geocode_address,
    "reverse_geocode": handle_reverse_geocode,
    "browse_databases": handle_browse_databases,
    "run_query": handle_run_query,
    "cortex_chat": handle_cortex_chat,
    "list_semantic_views": handle_list_semantic_views,
}


# ---------------------------------------------------------------------------
# JSON-RPC / MCP protocol
# ---------------------------------------------------------------------------

def make_response(req_id, result):
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def make_error(req_id, code, message, data=None):
    err = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def handle_initialize(req_id, _params):
    return make_response(req_id, {
        "protocolVersion": PROTOCOL_VERSION,
        "capabilities": {"tools": {}},
        "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
    })


def handle_tools_list(req_id, _params):
    return make_response(req_id, {"tools": TOOLS})


def handle_tools_call(req_id, params):
    tool_name = params.get("name", "")
    tool_args = params.get("arguments", {})

    handler = TOOL_HANDLERS.get(tool_name)
    if handler is None:
        return make_error(req_id, -32602, f"Unknown tool: {tool_name}")

    try:
        result = handler(tool_args)
        content = json.dumps(result, default=str)
        return make_response(req_id, {
            "content": [{"type": "text", "text": content}],
        })
    except Exception as exc:
        return make_response(req_id, {
            "content": [{"type": "text", "text": f"Error: {exc}"}],
            "isError": True,
        })


METHOD_HANDLERS = {
    "initialize": handle_initialize,
    "tools/list": handle_tools_list,
    "tools/call": handle_tools_call,
}


def handle_request(msg):
    """Route a JSON-RPC request to the appropriate handler."""
    req_id = msg.get("id")
    method = msg.get("method", "")
    params = msg.get("params", {})

    # Notifications (no id) — just acknowledge
    if req_id is None:
        return None

    handler = METHOD_HANDLERS.get(method)
    if handler is None:
        return make_error(req_id, -32601, f"Method not found: {method}")

    return handler(req_id, params)


# ---------------------------------------------------------------------------
# Main loop — stdin/stdout transport
# ---------------------------------------------------------------------------

def main():
    """Read JSON-RPC messages from stdin, write responses to stdout."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError as exc:
            resp = make_error(None, -32700, f"Parse error: {exc}")
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()
            continue

        resp = handle_request(msg)
        if resp is not None:
            sys.stdout.write(json.dumps(resp) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
