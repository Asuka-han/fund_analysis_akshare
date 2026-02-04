#!/usr/bin/env bash
set -e

cd /app
python scripts/update_db.py --all --clean-old "$@"
