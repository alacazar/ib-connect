---
name: data-upload
description: Monitors folder for JSON/CSV data files, validates, and uploads to PostgreSQL TimescaleDB.
location: ib_connect/skills/data_upload/data_upload.py
---

## Data Upload Skill

File-watching service that monitors a folder for new data files, validates them, and inserts into PostgreSQL under the `finance` schema. Contracts arrive as `.json`, OHLCV data as `.csv`.

### Prerequisites
- PostgreSQL with TimescaleDB.
- `PG_URI` environment variable set (or `db_uri` key in `config.json` using `${PG_URI}` substitution).
- Python deps: `psycopg2`, `watchdog`, `pandas`, `pytz`.

### Usage

```bash
python ib_connect/skills/data_upload/data_upload.py
```

Config in `config.json`:
```json
{
    "input_folder": "./input",
    "processed_folder": "./processed",
    "error_folder": "./errors",
    "db_uri": "${PG_URI}",
    "schema": "finance",
    "contract_table": "contracts",
    "ohlcv_1m_table": "ohlcv_1m",
    "ohlcv_1d_table": "ohlcv_1d"
}
```

### Processing

#### Contracts (`.json`)
Single object or array of objects. Required fields: `symbol`, `exchange`, `currency`, `sec_type`, `min_tick`, `tick_value`, `multiplier`, `time_zone_id`. Optional: `conid`, `local_symbol`, `long_name`, `industry`, `category`, `sub_category`, `contract_month`, `expiration_date`, `under_conid`, `strike`, `right`.

`conid` is computed as a SHA-256 hash if missing. Inserted into `finance.contracts` with `ON CONFLICT (conid) DO NOTHING`.

#### OHLCV (`.csv`)

Expected filename format (4 dot-separated parts):
```
<symbol>.<conid>.<barsize>.<timezone>.csv
```

Examples:
- `AAPL.265598.1min.us-eastern.csv`
- `AAPL.265598.1day.us-eastern.csv`

Supported bar sizes: `1min` → `finance.ohlcv_1m`, `1day` → `finance.ohlcv_1d`.

Header columns are case-insensitive. Mandatory: a time column (`date`, `datetime`, `time`, or `timestamp`), `open`, `high`, `low`, `close`. Optional: `volume`, `trades`, `adjusted_close`.

Rows inserted in batches of 1000 with `ON CONFLICT (conid, time) DO NOTHING`.

Both 4-part (`symbol.conid.barsize.tz.csv`) and 5-part (`symbol.conid.barsize.tz.bartype.csv`) filenames are accepted; the bar-type segment is ignored during parsing.

### Behavior
- Processes files already present in `input_folder` on startup, then watches for new arrivals.
- Successfully processed files are moved to `processed_folder`; failures go to `error_folder`.
- Uses a PID lock file to prevent duplicate instances.
- Logs to `data_upload.log` alongside the script.
