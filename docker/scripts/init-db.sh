#!/usr/bin/env bash
set -e

cd /app

if [ ! -f data/fund_data.db ]; then
  echo "Initializing database..."
  python reset_database.py reset
else
  echo "Database already exists at data/fund_data.db"
fi
