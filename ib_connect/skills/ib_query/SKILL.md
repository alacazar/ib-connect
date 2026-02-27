---
name: ib-query
description: Query Interactive Brokers for contract details using ib_insync. Provides CLI tool to connect, qualify, and retrieve contract info.
location: C:\Users\clawuser\.openclaw\shared\projects\ib-connect\skills\ib-query\ib_query.py
---

## IB Query Skill

This skill enables querying Interactive Brokers for contract details via a CLI tool built with `ib_insync`.

### Prerequisites
- `ib_insync` installed.
- IB Gateway or TWS running and accessible.
- Python 3.7+.

### Usage
Run the script with required and optional parameters:

```bash
python ib_query.py --symbol AAPL --sec-type STK
```

#### Parameters
- **Required**:
  - `--symbol` / `-s`: Ticker symbol (e.g., "AAPL").
  - `--sec-type` / `-t`: Security type (e.g., "STK").

- **Optional**:
  - `--exchange` / `-e`: Exchange (default: "SMART").
  - `--currency` / `-c`: Currency (default: "USD").
  - `--host` / `-H`: Host (default: "127.0.0.1").
  - `--port` / `-p`: Port (default: 7497).
  - `--client-id` / `-i`: Client ID (default: 1).
  - `--timeout`: Timeout (default: 4.0).
  - `--readonly`: Read-only (default: True).
  - `--verbose` / `-v`: Verbose output.
  - `--output-format` / `-f`: Output format ("json" or "text", default: "json").

### Behavior
- Connects read-only by default.
- Outputs JSON or text; handles errors gracefully.

### Example Output
```json
{"conId": 265598, "symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD", "fullName": "APPLE INC"}
```