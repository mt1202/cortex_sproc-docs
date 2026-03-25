#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/bootstrap_and_launch_${TIMESTAMP}.log"

run_main() {
  echo "========================================"
  echo "SPROC DOCS BOOTSTRAP + LAUNCH"
  echo "Project root: $PROJECT_ROOT"
  echo "Log file: $LOG_FILE"
  echo "Timestamp: $(date)"
  echo "========================================"

  cd "$PROJECT_ROOT" || {
    echo "ERROR: Could not cd to project root: $PROJECT_ROOT"
    exit 1
  }

  echo
  echo "[1/9] Detecting Python..."
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  elif command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
  else
    echo "ERROR: Python is not installed."
    echo "Install Python 3 first, then rerun this script."
    exit 1
  fi
  echo "Using Python: $(command -v "$PYTHON_BIN")"
  "$PYTHON_BIN" --version

  echo
  echo "[2/9] Ensuring required Ubuntu packages are installed..."
  NEED_APT=0

  if ! dpkg -s python3-venv >/dev/null 2>&1; then
    NEED_APT=1
  fi
  if ! dpkg -s python3-pip >/dev/null 2>&1; then
    NEED_APT=1
  fi
  if ! dpkg -s wslu >/dev/null 2>&1; then
    NEED_APT=1
  fi

  if [ "$NEED_APT" -eq 1 ]; then
    echo "Installing missing Ubuntu packages: python3-venv, python3-pip, wslu"
    sudo apt update
    sudo apt install -y python3-venv python3-pip wslu
  else
    echo "Ubuntu packages already present."
  fi

  echo
  echo "[3/9] Creating Python virtual environment if needed..."
  if [ ! -d ".venv" ]; then
    "$PYTHON_BIN" -m venv .venv
    echo "Created .venv"
  else
    echo ".venv already exists"
  fi

  echo
  echo "[4/9] Activating virtual environment..."
  # shellcheck disable=SC1091
  . ".venv/bin/activate"
  echo "Active Python: $(command -v python)"
  python --version

  echo
  echo "[5/9] Installing Python requirements..."
  if [ -f "requirements.txt" ]; then
    python -m pip install --upgrade pip
    pip install -r requirements.txt
  else
    echo "ERROR: requirements.txt not found in project root."
    exit 1
  fi

  echo
  echo "[6/9] Preparing Snowflake/Cortex folders..."
  mkdir -p "$HOME/.snowflake"
  mkdir -p "$HOME/.snowflake/cortex/skills"

  if [ -f "$PROJECT_ROOT/skills/sproc-documenter.md" ]; then
    cp "$PROJECT_ROOT/skills/sproc-documenter.md" "$HOME/.snowflake/cortex/skills/"
    echo "Copied sproc-documenter.md to ~/.snowflake/cortex/skills/"
  else
    echo "WARNING: skills/sproc-documenter.md not found."
  fi

  echo
  echo "[7/9] Checking connections.toml..."
  CONNECTIONS_FILE="$HOME/.snowflake/connections.toml"
  if [ -f "$CONNECTIONS_FILE" ]; then
    chown "$USER" "$CONNECTIONS_FILE" || true
    chmod 600 "$CONNECTIONS_FILE" || true
    echo "connections.toml found and permissions set to 600"
  else
    echo "WARNING: $CONNECTIONS_FILE not found."
    echo "The app may launch, but Snowflake connection tests will fail until this file exists."
  fi

  echo
  echo "[8/9] Setting browser bridge for WSL..."
  echo
  echo "[8/9] Browser integration note..."
  echo "WSL browser auto-open will not be used."
  echo "Open Streamlit manually in Windows at:"
  echo "  http://localhost:8501"
  if command -v wslview >/dev/null 2>&1; then
    echo "Using BROWSER=$BROWSER"
  else
    echo "WARNING: wslview not found even after installation step."
  fi

  echo
  echo "[9/9] Launching Streamlit..."
  echo "If browser auto-open fails, manually open: http://localhost:8501"
  python -m streamlit run app/streamlit_app.py
  EXIT_CODE=$?

  echo
  echo "========================================"
  echo "Bootstrap/launch finished with exit code: $EXIT_CODE"
  echo "Log file: $LOG_FILE"
  echo "========================================"

  exit $EXIT_CODE
}

run_main 2>&1 | tee "$LOG_FILE"