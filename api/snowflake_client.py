"""Snowflake client for the Find Data Nearby application.

Connects using externalbrowser auth or Snow CLI connection name.
Never uses passwords.
"""

import logging
import os
import re

import snowflake.connector

logger = logging.getLogger(__name__)

# Allowlist for SQL object identifiers (database, schema, table names)
_IDENTIFIER_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_$.]*$')


def _validate_identifier(name: str) -> str:
    """Validate a SQL identifier against the allowlist pattern.

    Raises ValueError if the name contains disallowed characters.
    """
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


class SnowflakeClient:
    """Snowflake connection wrapper with safe query methods.

    Supports two auth modes (no passwords):
      1. connection_name - uses SNOWFLAKE_CONNECTION_NAME env var for Snow CLI config
         (handles SNOWFLAKE_JWT key-pair auth automatically)
      2. externalbrowser - uses SNOWFLAKE_ACCOUNT + SNOWFLAKE_USER env vars
    """

    def __init__(self):
        self._connection_name = os.environ.get("SNOWFLAKE_CONNECTION_NAME")
        self._account = os.environ.get("SNOWFLAKE_ACCOUNT")
        self._user = os.environ.get("SNOWFLAKE_USER")

    def _connect(self) -> snowflake.connector.SnowflakeConnection:
        """Create a new Snowflake connection."""
        if self._connection_name:
            cfg = _read_connection_config(self._connection_name)
            if cfg and cfg.get("authenticator") == "SNOWFLAKE_JWT" and cfg.get("private_key_path"):
                logger.info("Connecting via key-pair for %s", cfg.get("user"))
                pkb = _load_private_key(cfg["private_key_path"])
                conn = snowflake.connector.connect(
                    account=cfg["account"],
                    user=cfg["user"],
                    private_key=pkb,
                    role=cfg.get("role", "ACCOUNTADMIN"),
                    warehouse=cfg.get("warehouse", "INGEST"),
                    database=cfg.get("database"),
                    schema=cfg.get("schema"),
                )
            else:
                logger.info("Connecting via Snow CLI connection: %s", self._connection_name)
                conn = snowflake.connector.connect(
                    connection_name=self._connection_name,
                )
        else:
            if not self._account or not self._user:
                raise RuntimeError(
                    "Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER env vars, "
                    "or set SNOWFLAKE_CONNECTION_NAME for Snow CLI auth."
                )
            logger.info("Connecting via externalbrowser for %s@%s", self._user, self._account)
            conn = snowflake.connector.connect(
                account=self._account,
                user=self._user,
                authenticator="externalbrowser",
            )

        # Set role and warehouse per project config
        conn.cursor().execute("USE ROLE ACCOUNTADMIN")
        conn.cursor().execute("USE WAREHOUSE INGEST")
        return conn

    def __enter__(self):
        self._conn = self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "_conn") and self._conn:
            self._conn.close()
            self._conn = None

    def _fetch_all(self, sql: str, params: tuple | None = None) -> list[dict]:
        """Execute SQL and return results as a list of dicts."""
        cur = self._conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(sql, params)
            return cur.fetchall()
        finally:
            cur.close()

    # ── Metadata browsing ──────────────────────────────────────────────

    def list_databases(self) -> list[dict]:
        """Return all databases visible to the current role."""
        logger.debug("SHOW DATABASES")
        return self._fetch_all("SHOW DATABASES")

    def list_schemas(self, database: str) -> list[dict]:
        """Return schemas in *database*."""
        db = _validate_identifier(database)
        logger.debug("SHOW SCHEMAS IN DATABASE %s", db)
        return self._fetch_all(f"SHOW SCHEMAS IN DATABASE {db}")

    def list_tables(self, database: str, schema: str) -> list[dict]:
        """Return tables in *database.schema*."""
        db = _validate_identifier(database)
        sc = _validate_identifier(schema)
        logger.debug("SHOW TABLES IN %s.%s", db, sc)
        return self._fetch_all(f"SHOW TABLES IN {db}.{sc}")

    def list_views(self, database: str, schema: str) -> list[dict]:
        """Return views in *database.schema*."""
        db = _validate_identifier(database)
        sc = _validate_identifier(schema)
        logger.debug("SHOW VIEWS IN %s.%s", db, sc)
        return self._fetch_all(f"SHOW VIEWS IN {db}.{sc}")

    def list_semantic_views(self) -> list[dict]:
        """Return all semantic views visible in the account."""
        logger.debug("SHOW SEMANTIC VIEWS IN ACCOUNT")
        return self._fetch_all("SHOW SEMANTIC VIEWS IN ACCOUNT")

    # ── Query execution ────────────────────────────────────────────────

    def execute_query(self, sql: str) -> list[dict]:
        """Execute a read-only SQL statement and return results as dicts.

        The caller is responsible for validating that *sql* is read-only
        (SELECT, SHOW, DESCRIBE, WITH, EXPLAIN).
        """
        logger.info("Executing query: %.120s", sql)
        return self._fetch_all(sql)

    # ── Geo search procedures ──────────────────────────────────────────

    def search_nearby_zip_codes(
        self, lat: float, lon: float, radius_meters: float
    ) -> list[dict]:
        """Query the search_nearby_zip_codes table function."""
        logger.info(
            "search_nearby_zip_codes(lat=%s, lon=%s, radius=%s)",
            lat, lon, radius_meters,
        )
        return self._fetch_all(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_zip_codes(%s::FLOAT, %s::FLOAT, %s::FLOAT))",
            (lat, lon, radius_meters),
        )

    def search_nearby_addresses(
        self, lat: float, lon: float, radius_meters: float, limit: int = 50
    ) -> list[dict]:
        """Query the search_nearby_addresses table function."""
        logger.info(
            "search_nearby_addresses(lat=%s, lon=%s, radius=%s, limit=%s)",
            lat, lon, radius_meters, limit,
        )
        return self._fetch_all(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_addresses(%s::FLOAT, %s::FLOAT, %s::FLOAT, %s))",
            (lat, lon, radius_meters, limit),
        )

    def search_nearby_demo_data(
        self, lat: float, lon: float, radius_meters: float
    ) -> list[dict]:
        """Query the search_nearby_demo_data table function (DEMO.DEMO tables)."""
        logger.info(
            "search_nearby_demo_data(lat=%s, lon=%s, radius=%s)",
            lat, lon, radius_meters,
        )
        return self._fetch_all(
            "SELECT * FROM TABLE(ANALYTICS_DEV_DB.STAGING.search_nearby_demo_data(%s::FLOAT, %s::FLOAT, %s::FLOAT))",
            (lat, lon, radius_meters),
        )

    # ── Cortex AI ──────────────────────────────────────────────────────

    def cortex_complete(self, prompt: str, model: str = "llama3.1-8b") -> list[dict]:
        """Run a Cortex COMPLETE call and return the result rows."""
        _validate_identifier(model.replace("-", "_").replace(".", "_"))
        logger.info("cortex_complete model=%s prompt=%.80s", model, prompt)
        return self._fetch_all(
            "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS response",
            (model, prompt),
        )

    def find_nearby_agent(self, question: str) -> list[dict]:
        """Call the find_nearby_agent stored procedure."""
        logger.info("find_nearby_agent question=%.80s", question)
        return self._fetch_all(
            "CALL ANALYTICS_DEV_DB.STAGING.find_nearby_agent(%s)",
            (question,),
        )
