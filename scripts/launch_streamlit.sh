#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/streamlit_${TIMESTAMP}.log"

run_main() {
  echo "========================================"
  echo "Launching Streamlit"
  echo "Project root: $PROJECT_ROOT"
  echo "Log file: $LOG_FILE"
  echo "Timestamp: $(date)"
  echo "========================================"

  cd "$PROJECT_ROOT" || exit 1

  if [ ! -d ".venv" ]; then
    echo "ERROR: .venv not found."
    echo "Run ./scripts/bootstrap_and_launch.sh first."
    exit 1
  fi

  . ".venv/bin/activate"

  echo "Python: $(command -v python)"
  python --version
  echo
  echo "Streamlit will run in headless mode."
  echo "Open this URL manually in Windows:"
  echo "  http://localhost:8501"
  echo

  python -m streamlit run app/streamlit_app.py --server.headless true
  EXIT_CODE=$?

  echo "Streamlit exited with code: $EXIT_CODE"
  exit $EXIT_CODE
}

run_main 2>&1 | tee "$LOG_FILE"