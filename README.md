# IB Connect

A modular Python toolkit for Interactive Brokers integration — query contracts, download historical market data, and upload to a TimescaleDB database.

## Architecture

```
ib_connect/
├── shared/           # IB connection management (shared by all skills)
├── skills/
│   ├── ib_query/     # CLI: qualify and inspect contracts
│   ├── ib_download/  # CLI + service: async historical data downloader
│   └── data_upload/  # Service: watch folder → PostgreSQL/TimescaleDB
└── gui/              # Flask web UI
```

## Prerequisites

- Python 3.9+
- [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway-stable.php) or TWS running with API connections enabled
  - Paper trading port: `7497` (default)
  - Live trading port: `7496`
- For `data_upload` and `gui`: PostgreSQL with [TimescaleDB](https://www.timescale.com/)

## Installation

```bash
pip install ib-connect
```

Or install from source for development:

```bash
git clone https://github.com/alacazar/ib-connect.git
cd ib_connect
pip install -e .
```

The `ib-query` and `ib-download` CLI commands are registered as entry points after installation.

## Configuration

The `ib_download` and `gui` skills require a `config.json`. Copy the provided examples and customize:

```bash
cp ib_connect/skills/ib_download/config.example.json ib_connect/skills/ib_download/config.json
cp ib_connect/gui/config.example.json ib_connect/gui/config.json
```

For `data_upload`, set the `PG_URI` environment variable:

```bash
export PG_URI="postgresql://user:password@localhost:5432/dbname"
```

## Skills

### ib-query

Query and qualify contract details from IB.

```bash
ib-query --symbol AAPL --sec-type STK
```

Output:
```json
{"conId": 265598, "symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD", "fullName": "APPLE INC"}
```

See [ib_connect/skills/ib_query/SKILL.md](ib_connect/skills/ib_query/SKILL.md) for all options.

---

### ib-download

Downloads historical OHLCV data from IB and writes CSV files. Consists of two parts:

**`download_service.py` — the background service**

IB enforces strict pacing limits: it throttles connections that issue too many data requests too quickly, and it only allows one active connection per client ID. Running a dedicated background service solves both problems — it owns a single, persistent IB connection and processes jobs one at a time, sleeping 20 seconds between data chunks to stay within IB's limits. Any number of callers can submit jobs without worrying about pacing or connection conflicts.

Start it once and keep it running (e.g. in `screen` or `tmux`, or as a system service):

```bash
python ib_connect/skills/ib_download/download_service.py
```

Long date ranges are split automatically into chunks sized to IB's per-request limits. Progress and errors are logged to `download_service.log` alongside the script. A PID lock file prevents duplicate instances.

**`ib-download` — the CLI client**

Submits a job to the service's SQLite queue and returns a `job_key` immediately. The caller can check status later without staying connected.

```bash
# Submit a job
ib-download -c 265598 -s 2023-01-01 -e 2023-12-31 -b "1 min"

# Check status
ib-download --status -k <job_key>
```

On completion the service can notify an agent via webhook (configure `webhook_url` and `token` in `config.json`).

See [ib_connect/skills/ib_download/SKILL.md](ib_connect/skills/ib_download/SKILL.md) for all options including batch mode and output format.

---

### data-upload

A file-watching service that validates and uploads contract JSON files and OHLCV CSV files to PostgreSQL/TimescaleDB. Drop files into the configured `input_folder` and they are processed automatically — contracts go to `finance.contracts`, OHLCV data to `finance.ohlcv_1m` or `finance.ohlcv_1d`. Processed files are moved to `processed/`; failures go to `errors/`.

```bash
python ib_connect/skills/data_upload/data_upload.py
```

Expected filename pattern for OHLCV: `<symbol>.<conid>.<barsize>.csv`.

See [ib_connect/skills/data_upload/SKILL.md](ib_connect/skills/data_upload/SKILL.md) for schema details.

---

### GUI

A Flask web interface that ties the toolkit together. From the browser you can:

- **Query contracts** — search by symbol and security type, inspect the qualified contract details returned by IB
- **Save contracts** — export selected contracts as JSON to the `data_upload` input folder, so they are picked up and stored in PostgreSQL automatically
- **Submit download jobs** — configure date range and bar size per contract and enqueue jobs with the download service
- **Monitor jobs** — check status and cancel pending jobs

```bash
python ib_connect/gui/app.py
```

Opens on `http://127.0.0.1:5000` by default (configurable in `gui/config.json`).

## License

[MIT](LICENSE)
