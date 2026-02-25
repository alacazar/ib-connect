---
name: data-upload
description: Monitors folder for JSON/CSV data files, validates, and uploads to PostgreSQL TimescaleDB.
location: C:\Users\clawuser\.openclaw\shared\projects\ib-connect\skills\data-upload\data_upload.py
---

## Data Upload Skill

Monitors a folder for new data files (contracts in JSON, OHLCV in CSV), validates them, and inserts into PostgreSQL under the `finance` schema.

### Prerequisites
- PostgreSQL with TimescaleDB.
- `PG_URI` env var set.
- Python deps: `psycopg2`, `watchdog`, `pandas`.

### Usage
Run the service: `python data_upload.py`

Config in `config.json`: input/processed/error folders.

### Processing
- **Contracts (.json)**: Validate schema, compute conid if missing (uint64 hash), insert to `finance.contracts`.
- **OHLCV (.csv)**: Filename `<symbol>.<conid>.<barsize>.csv` (barsize: 1min/1day). Parse header (case-insensitive), validate mandatory fields (time, open, high, low, close), deduce bar size, insert to `finance.ohlcv_1m` or `finance.ohlcv_1d` in batches.
- Files moved to processed/ or errors/ after handling.

### Notes
- Uses file monitoring for real-time uploads.
- Handles duplicates with ON CONFLICT DO NOTHING.