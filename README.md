# Find Data Nearby

Find geospatial data in Snowflake and external sources. Browse databases, views, semantic views, run queries, and chat with Cortex AI — all from a React dashboard, CLI, REST API, or MCP server.


<img width="2492" height="1247" alt="finddatanearby" src="https://github.com/user-attachments/assets/ffd1bec0-e506-4ce6-9e18-959f5bc61fc1" />


## Architecture

```
┌─────────────┐    ┌──────────────┐    ┌──────────────────────────┐
│  React App  │───▶│  Flask API   │───▶│  Snowflake               │
│  (Vite 6)   │    │  (port 5001) │    │  ├─ UDTFs (geo search)   │
└─────────────┘    └──────┬───────┘    │  ├─ Cortex AI (LLM)      │
                          │            │  └─ Semantic Views       │
┌─────────────┐           │            └──────────────────────────┘
│  CLI Tool   │───────────┘                       │
└─────────────┘           │            ┌──────────┴───────────────┐
                          ▼            │  External                │
┌─────────────┐    ┌──────────────┐    │  ├─ Nominatim/OSM        │
│  MCP Server │───▶│  Snowflake   │    │  └─ OpenAddress (564M)   │
│  (stdio)    │    │  Connector   │    └──────────────────────────┘
└─────────────┘    └──────────────┘
```

## Components

| Component    | Path        | Description                                  |
|--------------|-------------|----------------------------------------------|
| SQL Objects  | `sql/`      | Database, UDTFs, Cortex agent procedure      |
| REST API     | `api/`      | Flask server with 11 endpoints               |
| Frontend     | `frontend/` | React 19 + Leaflet map dashboard             |
| CLI          | `cli/`      | 7-subcommand terminal tool                   |
| MCP Server   | `mcp/`      | JSON-RPC over stdin/stdout, 7 tools          |
| Management   | `manage.sh` | Install, start, stop, test, validate, backup |

## Prerequisites

- Python 3.11+
- Node.js 18+
- Snowflake account with `ACCOUNTADMIN` role
- Snow CLI configured (`snow connection list` shows a valid connection)

## Quick Start

```bash
# 1. Validate dependencies
./manage.sh validate

# 2. Install Python and Node packages
./manage.sh install

# 3. Configure Snowflake connection
cp api/.env.example api/.env
# Edit api/.env with your SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER

# 4. Deploy SQL objects (run in Snowflake)
snow sql -f sql/01_setup_database.sql
snow sql -f sql/02_geo_functions.sql
snow sql -f sql/03_cortex_agent.sql

# 5. Start everything
./manage.sh startall
```

The dashboard opens at `http://localhost:5173`. The API runs at `http://localhost:5001`.

## Snowflake Objects

All objects live in `ANALYTICS_DEV_DB.STAGING`:

| Object                                      | Type | Description |
|---------------------------------------------|------|-------------|
| `search_history`                            | Table | Logged searches |
| `cached_locations` | Table | Geocoding cache |
| `search_nearby_zip_codes(lat, lon, radius)` | UDTF | Find zip codes within radius (meters) |
| `search_nearby_addresses(lat, lon, radius, limit)` | UDTF | Find street addresses within radius |
| `search_nearby_demo_data(lat, lon, radius)` | UDTF | Search 13 geo tables across 3 databases |
| `geocode_to_nearby_summary(lat, lon, radius)` | Procedure | LLM-summarized nearby results |
| `find_nearby_agent(question)` | Procedure | Natural language geo agent |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/databases` | List databases |
| GET | `/api/schemas/<db>` | List schemas |
| GET | `/api/tables/<db>/<schema>` | List tables and views |
| GET | `/api/semantic-views` | List semantic views |
| POST | `/api/query` | Execute read-only SQL |
| GET | `/api/search/nearby` | Search nearby zip codes |
| GET | `/api/search/addresses` | Search nearby addresses |
| POST | `/api/search/nearby` | Unified geo search (zip codes, addresses, demo data) |
| POST | `/api/cortex/complete` | Cortex AI chat |
| GET | `/api/geocode` | Forward geocode (Nominatim) |
| GET | `/api/reverse-geocode` | Reverse geocode (Nominatim) |

## CLI Usage

```bash
cd cli
python findnearby.py search --lat 40.7128 --lon -74.006 --radius 5000
python findnearby.py addresses --lat 40.7128 --lon -74.006 --radius 500
python findnearby.py chat --question "What zip codes are near Times Square?"
python findnearby.py agent --question "Find addresses near the Statue of Liberty"
python findnearby.py databases
python findnearby.py semantic-views
python findnearby.py geocode --address "350 Fifth Avenue, New York"
```

## MCP Server

The MCP server exposes 7 tools over JSON-RPC stdin/stdout for integration with AI assistants:

```bash
python mcp/server.py
```

Tools: `search_nearby`, `geocode_address`, `reverse_geocode`, `browse_databases`, `run_query`, `cortex_chat`, `list_semantic_views`

## Management Script

```bash
./manage.sh install      # Install all dependencies
./manage.sh setup        # Deploy SQL objects via Snow CLI
./manage.sh validate     # Check required tools are installed
./manage.sh startall     # Start API + frontend
./manage.sh stopall      # Stop all services
./manage.sh startapi     # Start API only
./manage.sh startweb     # Start frontend only
./manage.sh list         # Show running services
./manage.sh test         # Run test suite
./manage.sh backup       # Backup project files
./manage.sh document     # Generate project summary
```

## Security

- **No passwords in code or config.** Authentication uses `externalbrowser` (SSO) or key-pair via Snow CLI connection.
- API validates all SQL queries are read-only (`SELECT`, `SHOW`, `DESCRIBE`, `WITH`, `EXPLAIN` only).
- Identifier validation uses strict regex to prevent SQL injection.
- `.gitignore` blocks credentials (`*.pem`, `*.p8`, `*.key`, `.env`, `credentials.json`).

## Data Sources

| Source | Table | Rows | Use |
|--------|-------|------|-----|
| ZIP Code Metadata | `U_S__ZIP_CODE_METADATA.ZIP_DEMOGRAPHICS.ZIP_CODE_METADATA` | 37K | Zip code geo search |
| OpenAddress | `WORLDWIDE_ADDRESS_DATA.ADDRESS.OPENADDRESS` | 564M | Street address search |
| Weather Stations | `DEMO.DEMO.WEATHER_STATIONS_GEO` | 6,325 | Nearby weather stations |
| Air Quality | `DEMO.DEMO.AIR_QUALITY_MONITORS_GEO` | 1,493 | Air quality monitors |
| NYC Traffic | `DEMO.DEMO.NYCTRAFFICEVENTS` | 18,751 | Traffic events |
| Cameras | `DEMO.DEMO.CAMERAS` | 35 | Surveillance cameras |
| Aircraft (ADS-B) | `DEMO.DEMO.ADSB_CURRENT_AIRCRAFT` | 93 | Live aircraft positions |
| IoT Nodes | `DEMO.DEMO.MESHTASTIC_ACTIVE_NODES` | 105 | Meshtastic mesh nodes |
| Ghost Sightings | `GHOST_DETECTION.APP.GHOST_SIGHTINGS` | 30,050 | Reported ghost sightings |
| Ghost Sensors | `GHOST_DETECTION.APP.SENSOR_FUSION_DATA` | 100 | Sensor fusion readings |
| Ghost Offices | `GHOST_DETECTION.APP.OFFICES` | 30 | Ghost detection offices |
| Ghost Threats | `GHOST_DETECTION.APP.OSINT_THREAT_FEED` | 50 | OSINT threat intel |
| Ghost Missions | `GHOST_DETECTION.APP.MISSION_CONTROL_LOG` | 60 | Mission control logs |
| Subway Stations | `NYC_TRANSIT.RAW_DATA.SUBWAY_STATIONS` | 0* | NYC subway stops |
| Bus Positions | `NYC_TRANSIT.CURATED.BUS_POSITIONS_LATEST` | 0* | Live bus positions |
| Nominatim/OSM | External API | -- | Forward/reverse geocoding |
| Cortex AI | `SNOWFLAKE.CORTEX.COMPLETE()` | -- | LLM chat and NL agent |

*NYC Transit tables are wired up and will return results when data is loaded.

## Project Structure

```
finddatanearby/
├── api/
│   ├── app.py                 # Flask REST API (338 lines)
│   ├── snowflake_client.py    # Snowflake connector wrapper (224 lines)
│   ├── nominatim_client.py    # Geocoding client (119 lines)
│   ├── requirements.txt       # Python dependencies
│   └── .env.example           # Config template
├── cli/
│   └── findnearby.py          # CLI tool, 7 subcommands (416 lines)
├── frontend/
│   ├── src/
│   │   ├── App.jsx            # Main app with tab navigation
│   │   ├── main.jsx           # Entry point
│   │   ├── api/client.js      # API client
│   │   └── components/
│   │       ├── MapView.jsx        # Leaflet map with rich popups, Google Maps links (397 lines)
│   │       ├── SearchPanel.jsx    # Address/coordinate search (228 lines)
│   │       ├── DatabaseBrowser.jsx # DB tree explorer
│   │       ├── QueryEditor.jsx    # SQL editor with results
│   │       └── CortexChat.jsx     # AI chat interface
│   ├── index.html
│   ├── vite.config.js         # Vite config with API proxy
│   └── package.json
├── mcp/
│   └── server.py              # MCP server, 7 tools (462 lines)
├── sql/
│   ├── 01_setup_database.sql  # Database and table DDL
│   ├── 02_geo_functions.sql   # Geospatial UDTFs (375 lines, 3 UDTFs)
│   └── 03_cortex_agent.sql    # Cortex AI agent procedure
├── manage.sh                  # Management script (394 lines)
├── .gitignore
├── AGENTS.md                  # Original requirements
└── README.md
```

**~5,900 lines** across 25 source files.
