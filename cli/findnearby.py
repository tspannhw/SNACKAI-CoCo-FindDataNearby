#!/usr/bin/env python3
"""
Find Data Nearby - CLI Tool

Search for geospatial data in Snowflake and external sources.
Uses externalbrowser auth or SNOWFLAKE_CONNECTION_NAME (no passwords).

Usage:
    python3 cli/findnearby.py search --lat 40.7 --lon -74.0 --radius 5000
    python3 cli/findnearby.py geocode --address "123 Main St, New York, NY"
    python3 cli/findnearby.py reverse-geocode --lat 40.7 --lon -74.0
    python3 cli/findnearby.py databases [--database DB] [--schema SCHEMA]
    python3 cli/findnearby.py query --sql "SELECT ..."
    python3 cli/findnearby.py chat --question "Find restaurants near Times Square"
    python3 cli/findnearby.py semantic-views
"""

import argparse
import json
import os
import re
import sys

import requests
import snowflake.connector

_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_$.]*$')


def _validate_identifier(name: str) -> str:
    """Validate a SQL identifier against the allowlist pattern.

    Raises ValueError if the name contains disallowed characters.
    """
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


# ---------------------------------------------------------------------------
# Snowflake connection
# ---------------------------------------------------------------------------

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


def get_snowflake_connection():
    """Return a Snowflake connection using Snow CLI config or externalbrowser."""
    conn_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME")
    if conn_name:
        cfg = _read_connection_config(conn_name)
        if cfg and cfg.get("authenticator") == "SNOWFLAKE_JWT" and cfg.get("private_key_path"):
            pkb = _load_private_key(cfg["private_key_path"])
            return snowflake.connector.connect(
                account=cfg["account"],
                user=cfg["user"],
                private_key=pkb,
                role=cfg.get("role", "ACCOUNTADMIN"),
                warehouse=cfg.get("warehouse", "INGEST"),
                database=cfg.get("database"),
                schema=cfg.get("schema"),
            )
        return snowflake.connector.connect(connection_name=conn_name)

    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    if not account or not user:
        print("Error: Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars,", file=sys.stderr)
        print("       or set SNOWFLAKE_CONNECTION_NAME for Snow CLI auth.", file=sys.stderr)
        sys.exit(1)
    return snowflake.connector.connect(
        account=account,
        user=user,
        authenticator="externalbrowser",
        role="ACCOUNTADMIN",
        warehouse="INGEST",
    )


def run_query(conn, sql):
    """Execute a SQL query and return (column_names, rows)."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description] if cur.description else []
        rows = cur.fetchall()
        return cols, rows
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Table formatting
# ---------------------------------------------------------------------------

def print_table(cols, rows, max_col_width=40):
    """Print a nicely aligned table to stdout."""
    if not cols:
        print("(no results)")
        return

    str_rows = []
    for row in rows:
        str_rows.append([_truncate(str(v) if v is not None else "NULL", max_col_width) for v in row])

    widths = [len(c) for c in cols]
    for row in str_rows:
        for i, val in enumerate(row):
            widths[i] = max(widths[i], len(val))

    header = "  ".join(c.ljust(widths[i]) for i, c in enumerate(cols))
    sep = "  ".join("-" * widths[i] for i in range(len(cols)))
    print(header)
    print(sep)
    for row in str_rows:
        print("  ".join(row[i].ljust(widths[i]) for i in range(len(cols))))
    print(f"\n({len(rows)} row{'s' if len(rows) != 1 else ''})")


def _truncate(s, n):
    return s if len(s) <= n else s[: n - 3] + "..."


# ---------------------------------------------------------------------------
# Nominatim geocoding helpers
# ---------------------------------------------------------------------------

NOMINATIM_BASE = "https://nominatim.openstreetmap.org"
NOMINATIM_HEADERS = {"User-Agent": "FindDataNearby/1.0"}


def nominatim_geocode(address):
    """Forward geocode an address via Nominatim. Returns list of results."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/search",
        params={"q": address, "format": "json", "limit": 5},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def nominatim_reverse(lat, lon):
    """Reverse geocode (lat, lon) via Nominatim. Returns a single result dict."""
    resp = requests.get(
        f"{NOMINATIM_BASE}/reverse",
        params={"lat": lat, "lon": lon, "format": "json"},
        headers=NOMINATIM_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Subcommand handlers
# ---------------------------------------------------------------------------

def cmd_search(args):
    """Search for geospatial data near a point using Snowflake UDTFs."""
    conn = get_snowflake_connection()
    try:
        source = args.source or "all"
        print(f"Searching near ({args.lat}, {args.lon}) within {args.radius}m...\n")

        if source in ("zip_codes", "all"):
            print("--- Nearby Zip Codes ---")
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_zip_codes(%s::FLOAT, %s::FLOAT, %s::FLOAT)) LIMIT 20",
                    (args.lat, args.lon, args.radius),
                )
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                print_table(cols, rows)
            except Exception as exc:
                warn_msg = str(exc)
                if "does not exist" in warn_msg:
                    print("  UDTF not found. Run: ./manage.sh setup")
                else:
                    print(f"  Error: {warn_msg}")
            finally:
                cur.close()

        if source in ("addresses", "all"):
            print("\n--- Nearby Addresses ---")
            cur = conn.cursor()
            try:
                cur.execute(
                    "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_addresses(%s::FLOAT, %s::FLOAT, %s::FLOAT, %s)) LIMIT 20",
                    (args.lat, args.lon, args.radius, 20),
                )
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                print_table(cols, rows)
            except Exception as exc:
                warn_msg = str(exc)
                if "does not exist" in warn_msg:
                    print("  UDTF not found. Run: ./manage.sh setup")
                else:
                    print(f"  Error: {warn_msg}")
            finally:
                cur.close()

        # Always include Nominatim reverse lookup
        print("\n--- Nominatim Reverse Lookup ---")
        result = nominatim_reverse(args.lat, args.lon)
        if "display_name" in result:
            print(f"  Location: {result['display_name']}")
            addr = result.get("address", {})
            for key in ("road", "city", "state", "postcode", "country"):
                if key in addr:
                    print(f"  {key.title():12s}: {addr[key]}")
        else:
            print("  No results from Nominatim.")
    finally:
        conn.close()


def cmd_geocode(args):
    """Forward geocode an address using Nominatim."""
    results = nominatim_geocode(args.address)
    if not results:
        print("No results found.")
        return
    cols = ["LAT", "LON", "DISPLAY_NAME", "TYPE"]
    rows = [(r["lat"], r["lon"], r["display_name"], r.get("type", "")) for r in results]
    print_table(cols, rows, max_col_width=60)


def cmd_reverse_geocode(args):
    """Reverse geocode coordinates using Nominatim."""
    result = nominatim_reverse(args.lat, args.lon)
    if "error" in result:
        print(f"Error: {result['error']}")
        return
    print(f"Address: {result.get('display_name', 'N/A')}")
    addr = result.get("address", {})
    if addr:
        print("\nDetails:")
        for key, val in addr.items():
            print(f"  {key:20s}: {val}")


def cmd_databases(args):
    """Browse databases, schemas, and tables."""
    conn = get_snowflake_connection()
    try:
        if args.database and args.schema:
            db = _validate_identifier(args.database)
            schema = _validate_identifier(args.schema)
            sql = f"SHOW TABLES IN {db}.{schema}"
        elif args.database:
            db = _validate_identifier(args.database)
            sql = f"SHOW SCHEMAS IN DATABASE {db}"
        else:
            sql = "SHOW DATABASES"
        cols, rows = run_query(conn, sql)
        print_table(cols, rows)
    finally:
        conn.close()


def cmd_query(args):
    """Run an arbitrary SQL query."""
    conn = get_snowflake_connection()
    try:
        cols, rows = run_query(conn, args.sql)
        print_table(cols, rows)
    finally:
        conn.close()


def cmd_chat(args):
    """Chat with Cortex AI about nearby data."""
    conn = get_snowflake_connection()
    try:
        model = args.model or "llama3.1-8b"
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS response",
                (model, args.question),
            )
            row = cur.fetchone()
            if row and row[0]:
                print(row[0])
            else:
                print("No response from Cortex.")
        finally:
            cur.close()
    finally:
        conn.close()


def cmd_semantic_views(args):
    """List semantic views available in the account."""
    conn = get_snowflake_connection()
    try:
        sql = "SHOW SEMANTIC VIEWS"
        cols, rows = run_query(conn, sql)
        if rows:
            print_table(cols, rows)
        else:
            print("No semantic views found.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="findnearby",
        description="Find Data Nearby - search geospatial data in Snowflake and external sources",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # search
    p_search = sub.add_parser("search", help="Search for data near a geographic point")
    p_search.add_argument("--lat", type=float, required=True, help="Latitude")
    p_search.add_argument("--lon", type=float, required=True, help="Longitude")
    p_search.add_argument("--radius", type=float, default=5000, help="Radius in meters (default 5000)")
    p_search.add_argument("--source", choices=["zip_codes", "addresses", "all"], default="all",
                          help="Data source to search")

    # geocode
    p_geo = sub.add_parser("geocode", help="Forward geocode an address")
    p_geo.add_argument("--address", required=True, help="Address to geocode")

    # reverse-geocode
    p_rev = sub.add_parser("reverse-geocode", help="Reverse geocode coordinates")
    p_rev.add_argument("--lat", type=float, required=True, help="Latitude")
    p_rev.add_argument("--lon", type=float, required=True, help="Longitude")

    # databases
    p_db = sub.add_parser("databases", help="Browse databases, schemas, tables")
    p_db.add_argument("--database", default=None, help="Database name")
    p_db.add_argument("--schema", default=None, help="Schema name")

    # query
    p_q = sub.add_parser("query", help="Run a SQL query")
    p_q.add_argument("--sql", required=True, help="SQL statement to execute")

    # chat
    p_chat = sub.add_parser("chat", help="Chat with Cortex AI")
    p_chat.add_argument("--question", required=True, help="Question to ask")
    p_chat.add_argument("--model", default=None, help="Cortex model (default: llama3.1-8b)")

    # semantic-views
    sub.add_parser("semantic-views", help="List semantic views")

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

DISPATCH = {
    "search": cmd_search,
    "geocode": cmd_geocode,
    "reverse-geocode": cmd_reverse_geocode,
    "databases": cmd_databases,
    "query": cmd_query,
    "chat": cmd_chat,
    "semantic-views": cmd_semantic_views,
}


def main():
    parser = build_parser()
    args = parser.parse_args()
    handler = DISPATCH.get(args.command)
    if handler is None:
        parser.print_help()
        sys.exit(1)
    try:
        handler(args)
    except snowflake.connector.errors.Error as exc:
        print(f"Snowflake error: {exc}", file=sys.stderr)
        sys.exit(1)
    except requests.RequestException as exc:
        print(f"HTTP error: {exc}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)


if __name__ == "__main__":
    main()
