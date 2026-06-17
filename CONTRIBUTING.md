# Contributing

## Prerequisites

- Python 3.9+
- IB Gateway or TWS with API connections enabled
  - Paper trading: port `7497`; live trading: port `7496`
- For `data_upload`: PostgreSQL with TimescaleDB

## Local setup

1. Clone and install in editable mode:
   ```bash
   pip install -e .
   ```

2. Copy config templates and customize:
   ```bash
   cp ib_connect/skills/ib_download/config.example.json ib_connect/skills/ib_download/config.json
   cp ib_connect/gui/config.example.json ib_connect/gui/config.json
   ```

3. For `data_upload`, set `PG_URI`:
   ```bash
   export PG_URI="postgresql://user:password@localhost:5432/dbname"
   ```

## Running the services

| Component | Command |
|---|---|
| Download service | `python ib_connect/skills/ib_download/download_service.py` |
| Data upload service | `python ib_connect/skills/data_upload/data_upload.py` |
| GUI | `python ib_connect/gui/app.py` |

## Project structure

- `shared/` — `IBConnection` wrapper shared by all skills
- `skills/ib_query/` — contract query CLI (`ib-query`)
- `skills/ib_download/` — async historical data downloader: CLI (`ib-download`) + background service
- `skills/data_upload/` — folder-watching upload service for PostgreSQL/TimescaleDB
- `gui/` — Flask web interface for querying contracts and submitting download jobs
