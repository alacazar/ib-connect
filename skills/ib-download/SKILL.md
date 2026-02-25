---
name: ib-download
description: Asynchronous historical data downloader for Interactive Brokers using ib_insync. Submits jobs to a queue, processes in background, notifies on completion.
location: C:\Users\clawuser\.openclaw\shared\projects\ib-connect\skills\ib-download\ib_download.py
---

## IB Download Skill

This skill provides asynchronous downloading of historical data from IB, respecting pacing limits.

### Prerequisites
- `ib_insync` installed.
- IB Gateway/TWS running.
- Python 3.7+.

### Usage
Submit jobs or query status:

```bash
python ib_download.py -c 265598 -s 2023-01-01 -e 2023-12-31 -b 1min -o ./data -S session_key
python ib_download.py --status -k job_key
```

### Parameters
- **Submit**: conid, start, end, bar_size, show, output_dir, config-file for batch.
- **Status**: --status --key.

### Behavior
- Submits to SQLite queue, returns job_key.
- Service processes jobs, downloads in chunks, saves CSV.
- Notifies via sessions_send on completion.

### Service
Run `python download_service.py` in background to process jobs.