---
name: ib-download
description: Asynchronous historical data downloader for Interactive Brokers. Submits download jobs and notifies on completion.
location: ib_connect/skills/ib_download/ib_download.py
---

## IB Download Skill

Download historical market data from Interactive Brokers asynchronously. Submit a job via CLI, and a background service processes it and saves a CSV or JSON file. Supports single contracts or batch mode via CSV config file.

### Prerequisites
- `ib_insync` installed.
- IB Gateway or TWS running and accessible.
- Python 3.9+.
- Background service running (see below).

### Running the Service

Start `download_service.py` once and keep it running (e.g. in `screen` or `tmux`):

```bash
python ib_connect/skills/ib_download/download_service.py
```

The service holds a single IB connection and processes jobs sequentially with pacing delays to comply with IB's rate limits. Configure `db_path`, `output_dir`, and IB connection details in `config.json`.

### Usage

#### Submit a Job
```bash
ib-download -c 265598 -s 2023-01-01 -e 2023-12-31 -b "1 min"
```

Returns `{"job_key": "<key>"}` immediately.

#### Check Job Status
```bash
ib-download --status -k <job_key>
```

Returns status (`pending`, `processing`, `completed`, `failed`) and result details.

### Options

#### Required for Submission
- `-c, --conid` (int): IB contract ID.
- `-s, --start` (str): Start date (`YYYY-MM-DD`).
- `-e, --end` (str): End date (`YYYY-MM-DD`).
- `-b, --bar-size` (str): Bar size. Must include the space — allowed values:
  `1 secs`, `5 secs`, `10 secs`, `15 secs`, `30 secs`,
  `1 min`, `2 mins`, `3 mins`, `4 mins`, `5 mins`, `10 mins`, `15 mins`, `20 mins`, `30 mins`,
  `1 hour`, `2 hours`, `3 hours`, `4 hours`, `8 hours`,
  `1 day`, `1W`, `1M`.

#### Optional
- `--show` (str): Data type — `TRADES` (default), `MIDPOINT`, `BID`, `ASK`.
- `--format` (str): Output format — `csv` (default) or `json`.
- `-f, --config-file` (str): Path to batch CSV (`conid,start,end,bar_size,show`). Overrides single-contract options.
- `-A, --agent` (str): Agent name/ID for webhook notification on completion.
- `-m, --msg` (str): Custom message included in the notification payload.
- `-H, --host` (str): IB host (default: `127.0.0.1`).
- `-p, --port` (int): IB port (default: `7497`).
- `-i, --client-id` (int): IB client ID (default: `99`).
- `--timeout` (float): Connection timeout in seconds (default: `4.0`).
- `--max-retries` (int): Max retries per chunk (default: `3`).
- `--use-rth`: Use regular trading hours only (default: off).
- `-v, --verbose`: Verbose output.

#### For Status Check
- `--status`: Enable status mode.
- `-k, --key` (str): Job key from a prior submission.

### Output Files

Files are saved to `output_dir` (configured in `config.json`) with the naming pattern:

```
<localSymbol>.<conid>.<barsize>.<timezone>.<bar-type>.csv
```

Example: `AAPL.265598.1min.us-eastern.trades.csv`

### Behavior
- **Submission**: Enqueues a job in the SQLite queue (`db_path` in config). Returns `job_key` immediately.
- **Processing**: Service downloads data in date chunks, sleeping 20s between chunks to respect IB pacing limits.
- **Notification**: On completion/failure, POSTs to `webhook_url` with job status, result path, and any custom message.
- **Errors**: Partial data coverage is flagged in the status message.

### Examples
```bash
# Single contract, 1-min bars
ib-download -c 265598 -s 2023-01-01 -e 2023-12-31 -b "1 min" -A myagent

# Daily bars
ib-download -c 265598 -s 2020-01-01 -e 2023-12-31 -b "1 day"

# Batch from file
ib-download -f contracts.csv -A myagent

# Check status
ib-download --status -k abc-123

# JSON output format
ib-download -c 265598 -s 2023-01-01 -e 2023-12-31 -b "1 day" --format json
```
