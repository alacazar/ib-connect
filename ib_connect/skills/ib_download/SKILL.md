---
name: ib-download
description: Asynchronous historical data downloader for Interactive Brokers. Submits download jobs and notifies on completion.
location: C:\Users\clawuser\.openclaw\shared\projects\ib-connect\skills\ib-download\ib_download.py
---

## IB Download Skill

Download historical market data from Interactive Brokers asynchronously. Submit a job, and you'll be notified when it's done. Supports single contracts or batch processing via config file.

### Prerequisites
- `ib_insync` library installed.
- IB Gateway or TWS running and accessible.
- Python 3.7+.
- Background service running (see below).

### Usage

#### Submit a Download Job
```bash
python ib_download.py [OPTIONS]
```

Creates a background job for downloading data. You'll receive a notification when complete.

#### Check Job Status
```bash
python ib_download.py --status --key <job_key>
```

Returns the current status (pending, processing, completed, failed) and result details.

### Options

#### Required for Submission
- `-c, --conid` (int): Contract ID (e.g., 265598 for AAPL). Use IB's contract ID for precision.
- `-s, --start` (str): Start date (YYYY-MM-DD).
- `-e, --end` (str): End date (YYYY-MM-DD).
- `-b, --bar-size` (str): Bar size. Allowed: `1sec`, `5secs`, `15secs`, `30secs`, `1min`, `2mins`, `3mins`, `5mins`, `15mins`, `30mins`, `1hour`, `2hours`, `3hours`, `4hours`, `8hours`, `1day`, `1week`, `1month`.
- `-A, --agent` (str): Agent name/ID for notifications (e.g., "apoc"). Required for notifications.
- `-m, --msg` (str): Custom message to include in the notification (e.g., "Load the CSV and run backtest on strategy X").

#### Optional
- `--show` (str): Data type. Allowed: `TRADES` (default), `MIDPOINT`, `BID`, `ASK`.
- `-o, --output-dir` (str): Output directory (default: "./data").
- `-f, --config-file` (str): Path to batch config file (CSV: `conid,start,end,bar_size,show`). Overrides single options.
- `-H, --host` (str): IB host (default: "127.0.0.1").
- `-p, --port` (int): IB port (default: 7497).
- `-i, --client-id` (int): IB client ID (default: 1).
- `--timeout` (float): Connection timeout in seconds (default: 4.0).
- `--max-retries` (int): Max retries per chunk (default: 3).
- `--use-rth` (bool): Use regular trading hours only (default: False).
- `-v, --verbose` (bool): Verbose output (default: False).

#### For Status Check
- `--status`: Enable status mode.
- `-k, --key` (str): Job key from submission.

### Behavior
- **Job Creation**: Submits a job for background processing. Returns a unique `job_key` immediately.
- **Processing**: Downloads data in chunks (respecting IB limits), saves as CSV in `output_dir` (e.g., `265598_1min.csv`).
- **Notification**: On completion/failure, notifies the specified agent with details, including any custom message.
- **Errors**: Check status for issues (e.g., connection failed, no data).
- **Agent Instructions**: When posting a download job, summarize what should be done when receiving completion notification (e.g., "Load data and analyze trends"). This helps agents remember tasks for asynchronous workflows.

### Examples
```bash
# Single contract, 1-min bars
python ib_download.py -c 265598 -s 2023-01-01 -e 2023-12-31 -b "1 min" -A apoc

# Daily bars for speed
python ib_download.py -c 265598 -s 2020-01-01 -e 2023-12-31 -b "1 day" -A apoc

# Batch from file
python ib_download.py -f contracts.csv -A apoc

# Check status
python ib_download.py --status -k abc-123
```

### Running the Service
Start the background service to process jobs:
```bash
python download_service.py
```
Run it in the background (e.g., via `screen`, `tmux`, or system service). It polls for jobs and handles downloads.