#!/usr/bin/env bash
# manage.sh - Management script for Find Data Nearby
# chmod +x manage.sh before first use
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_DIR="$PROJECT_DIR/.pids"
BACKUP_DIR="$PROJECT_DIR/backups"
API_PID_FILE="$PID_DIR/api.pid"
WEB_PID_FILE="$PID_DIR/web.pid"
API_LOG="$PROJECT_DIR/api.log"
WEB_LOG="$PROJECT_DIR/web.log"
API_DIR="$PROJECT_DIR/api"
WEB_DIR="$PROJECT_DIR/frontend"
CLI_DIR="$PROJECT_DIR/cli"
MCP_DIR="$PROJECT_DIR/mcp"
SQL_DIR="$PROJECT_DIR/sql"

# ---------------------------------------------------------------------------
# Colors
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
ensure_pid_dir() {
    mkdir -p "$PID_DIR"
}

is_running() {
    local pid_file="$1"
    if [[ -f "$pid_file" ]]; then
        local pid
        pid=$(cat "$pid_file")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        # Stale PID file
        rm -f "$pid_file"
    fi
    return 1
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_install() {
    info "Installing dependencies..."

    # Python deps
    if [[ -f "$API_DIR/requirements.txt" ]]; then
        info "Installing Python dependencies from api/requirements.txt..."
        pip3 install -r "$API_DIR/requirements.txt"
        success "Python dependencies installed."
    else
        warn "api/requirements.txt not found — skipping Python deps."
    fi

    # Node deps
    if [[ -d "$WEB_DIR" && -f "$WEB_DIR/package.json" ]]; then
        info "Installing Node.js dependencies..."
        (cd "$WEB_DIR" && npm install)
        success "Node.js dependencies installed."
    else
        warn "frontend/package.json not found — skipping Node deps."
    fi

    success "Install complete."
}

cmd_setup() {
    info "Running SQL setup scripts via Snow CLI..."
    if [[ ! -d "$SQL_DIR" ]]; then
        warn "sql/ directory not found — nothing to set up."
        return 0
    fi
    local count=0
    for sql_file in "$SQL_DIR"/*.sql; do
        [[ -f "$sql_file" ]] || continue
        info "Executing: $(basename "$sql_file")"
        snow sql -f "$sql_file"
        count=$((count + 1))
    done
    if [[ $count -eq 0 ]]; then
        warn "No .sql files found in sql/."
    else
        success "Executed $count SQL file(s)."
    fi
}

cmd_start_api() {
    ensure_pid_dir
    if is_running "$API_PID_FILE"; then
        warn "API is already running (PID $(cat "$API_PID_FILE"))."
        return 0
    fi
    if [[ ! -f "$API_DIR/app.py" ]]; then
        error "api/app.py not found."
        return 1
    fi
    info "Starting Flask API..."
    nohup python3 "$API_DIR/app.py" > "$API_LOG" 2>&1 &
    local pid=$!
    echo "$pid" > "$API_PID_FILE"
    sleep 1
    if kill -0 "$pid" 2>/dev/null; then
        success "API started (PID $pid). Log: $API_LOG"
    else
        error "API failed to start. Check $API_LOG"
        rm -f "$API_PID_FILE"
        return 1
    fi
}

cmd_start_web() {
    ensure_pid_dir
    if is_running "$WEB_PID_FILE"; then
        warn "Web server is already running (PID $(cat "$WEB_PID_FILE"))."
        return 0
    fi
    if [[ ! -d "$WEB_DIR" ]]; then
        error "frontend/ directory not found."
        return 1
    fi
    info "Starting React dev server..."
    (cd "$WEB_DIR" && nohup npm run dev > "$WEB_LOG" 2>&1 &
     echo $! > "$WEB_PID_FILE")
    sleep 2
    if is_running "$WEB_PID_FILE"; then
        success "Web server started (PID $(cat "$WEB_PID_FILE")). Log: $WEB_LOG"
    else
        error "Web server failed to start. Check $WEB_LOG"
        return 1
    fi
}

cmd_start_all() {
    cmd_start_api
    cmd_start_web
}

cmd_stop_api() {
    if is_running "$API_PID_FILE"; then
        local pid
        pid=$(cat "$API_PID_FILE")
        info "Stopping API (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        rm -f "$API_PID_FILE"
        success "API stopped."
    else
        warn "API is not running."
    fi
}

cmd_stop_web() {
    if is_running "$WEB_PID_FILE"; then
        local pid
        pid=$(cat "$WEB_PID_FILE")
        info "Stopping web server (PID $pid)..."
        kill "$pid" 2>/dev/null || true
        rm -f "$WEB_PID_FILE"
        success "Web server stopped."
    else
        warn "Web server is not running."
    fi
}

cmd_stop_all() {
    cmd_stop_api
    cmd_stop_web
}

cmd_list() {
    echo ""
    info "Running processes:"
    echo ""
    if is_running "$API_PID_FILE"; then
        success "  Flask API     — PID $(cat "$API_PID_FILE")"
    else
        warn   "  Flask API     — not running"
    fi
    if is_running "$WEB_PID_FILE"; then
        success "  React Web     — PID $(cat "$WEB_PID_FILE")"
    else
        warn   "  React Web     — not running"
    fi
    echo ""
}

cmd_validate() {
    info "Validating dependencies..."
    local ok=true

    for cmd in python3 pip3 node npm; do
        if command -v "$cmd" &>/dev/null; then
            success "  $cmd — $(command -v "$cmd")"
        else
            error "  $cmd — NOT FOUND"
            ok=false
        fi
    done

    if command -v snow &>/dev/null; then
        success "  snow CLI — $(command -v snow)"
    else
        warn "  snow CLI — not found (optional, needed for setup command)"
    fi

    if $ok; then
        success "All required dependencies found."
    else
        error "Some dependencies are missing."
        return 1
    fi
}

cmd_test() {
    info "Running tests..."

    # Test API health endpoint
    if is_running "$API_PID_FILE"; then
        info "Checking API health..."
        if curl -sf http://localhost:5001/api/health > /dev/null 2>&1; then
            success "  API health check passed."
        else
            warn "  API health check failed (is it listening on :5001?)."
        fi
    else
        warn "  API is not running — skipping API test."
    fi

    # Test CLI import
    info "Checking CLI imports..."
    if python3 -c "import cli.findnearby" 2>/dev/null; then
        success "  CLI module imports OK."
    else
        # Try direct import
        if python3 -c "import sys; sys.path.insert(0, '$PROJECT_DIR'); import cli.findnearby" 2>/dev/null; then
            success "  CLI module imports OK."
        else
            warn "  CLI module import failed (missing dependencies?)."
        fi
    fi

    # Test MCP server import
    info "Checking MCP server imports..."
    if python3 -c "import sys; sys.path.insert(0, '$MCP_DIR'); import server" 2>/dev/null; then
        success "  MCP server module imports OK."
    else
        warn "  MCP server module import failed (missing dependencies?)."
    fi

    success "Tests complete."
}

cmd_backup() {
    local timestamp
    timestamp=$(date +%Y%m%d_%H%M%S)
    local dest="$BACKUP_DIR/backup_$timestamp"
    mkdir -p "$dest"

    info "Backing up to $dest..."

    # SQL files
    if [[ -d "$SQL_DIR" ]]; then
        cp -r "$SQL_DIR" "$dest/sql"
        success "  Backed up sql/"
    fi

    # CLI
    if [[ -d "$CLI_DIR" ]]; then
        cp -r "$CLI_DIR" "$dest/cli"
        success "  Backed up cli/"
    fi

    # MCP
    if [[ -d "$MCP_DIR" ]]; then
        cp -r "$MCP_DIR" "$dest/mcp"
        success "  Backed up mcp/"
    fi

    # API (source only, not venv)
    if [[ -d "$API_DIR" ]]; then
        mkdir -p "$dest/api"
        find "$API_DIR" -maxdepth 1 -type f -exec cp {} "$dest/api/" \;
        success "  Backed up api/ (top-level files)"
    fi

    # Config files
    for f in .gitignore AGENTS.md manage.sh; do
        if [[ -f "$PROJECT_DIR/$f" ]]; then
            cp "$PROJECT_DIR/$f" "$dest/"
        fi
    done

    success "Backup complete: $dest"
}

cmd_document() {
    echo ""
    info "Project Structure: Find Data Nearby"
    echo "========================================"
    echo ""
    echo "Root: $PROJECT_DIR"
    echo ""

    # Show directory tree (1 level deep, excluding node_modules etc.)
    if command -v tree &>/dev/null; then
        tree -L 2 -I 'node_modules|__pycache__|.venv|venv|.git|backups|.pids' "$PROJECT_DIR"
    else
        find "$PROJECT_DIR" -maxdepth 2 \
            -not -path '*node_modules*' \
            -not -path '*__pycache__*' \
            -not -path '*.venv*' \
            -not -path '*.git/*' \
            -not -path '*backups*' \
            -not -path '*.pids*' \
            | sort \
            | sed "s|$PROJECT_DIR/||"
    fi

    echo ""
    info "Components:"
    echo "  cli/findnearby.py  — CLI tool for geospatial search"
    echo "  mcp/server.py      — MCP server (JSON-RPC over stdin/stdout)"
    echo "  api/               — Flask REST API"
    echo "  frontend/          — React web dashboard"
    echo "  sql/               — Snowflake SQL setup scripts"
    echo "  manage.sh          — This management script"
    echo ""
}

cmd_help() {
    echo ""
    echo "Usage: ./manage.sh <command>"
    echo ""
    echo "Commands:"
    echo "  install      Install all dependencies (pip, npm)"
    echo "  setup        Run SQL setup scripts against Snowflake"
    echo "  start-api    Start Flask API in background"
    echo "  start-web    Start React dev server in background"
    echo "  start-all    Start both API and web"
    echo "  stop-api     Stop Flask API"
    echo "  stop-web     Stop React dev server"
    echo "  stop-all     Stop everything"
    echo "  list         List running processes"
    echo "  validate     Check all dependencies exist"
    echo "  test         Run basic tests"
    echo "  backup       Backup project files with timestamp"
    echo "  document     Show project structure summary"
    echo "  help         Show this help message"
    echo ""
}

# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------

case "${1:-help}" in
    install)    cmd_install ;;
    setup)      cmd_setup ;;
    start-api)  cmd_start_api ;;
    start-web)  cmd_start_web ;;
    start-all)  cmd_start_all ;;
    stop-api)   cmd_stop_api ;;
    stop-web)   cmd_stop_web ;;
    stop-all)   cmd_stop_all ;;
    list)       cmd_list ;;
    validate)   cmd_validate ;;
    test)       cmd_test ;;
    backup)     cmd_backup ;;
    document)   cmd_document ;;
    help|--help|-h) cmd_help ;;
    *)
        error "Unknown command: $1"
        cmd_help
        exit 1
        ;;
esac
