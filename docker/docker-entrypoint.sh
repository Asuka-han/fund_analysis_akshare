#!/usr/bin/env bash
set -e

APP_ROOT="/app"
mkdir -p "$APP_ROOT/data" "$APP_ROOT/reports"

case "$1" in
  ""|"main")
    shift || true
    exec python main.py "$@"
    ;;
  "update-db")
    shift
    exec python scripts/update_db.py "$@"
    ;;
  "run-analysis")
    shift
    exec python scripts/run_analysis_from_db.py "$@"
    ;;
  "analysis-from-excel")
    shift
    exec python scripts/analysis_from_excel.py "$@"
    ;;
  "import-excel")
    shift
    exec python scripts/import_excel_to_db.py "$@"
    ;;
  "shell"|"bash")
    shift || true
    exec /bin/bash "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
