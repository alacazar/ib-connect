---
name: ib-query
description: Query Interactive Brokers for contract details using ib_insync. Provides CLI tool to connect, qualify, and retrieve contract info.
location: ib_connect/skills/ib_query/ib_query.py
---

## IB Query Skill

CLI tool for querying and qualifying contract details from Interactive Brokers via `ib_insync`.

### Prerequisites
- `ib_insync` installed.
- IB Gateway or TWS running and accessible.
- Python 3.9+.

### Usage

```bash
ib-query --symbol AAPL --sec-type STK
```

#### Required
- `-s, --symbol`: Ticker symbol (e.g., `AAPL`).
- `-t, --sec-type`: Security type. Choices: `STK`, `FUT`, `OPT`, `CASH`, `CFD`, `BOND`, `FOP`, `WAR`, `IOPT`, `CMDTY`, `BAG`, `NEWS`.

#### Optional — contract filtering
- `-e, --exchange`: Exchange (default: `SMART`).
- `-c, --currency`: Currency (default: `USD`).
- `-x, --expiry`: Expiry date for futures/options (`YYYYMMDD`).
- `-k, --strike`: Strike price(s) for options — single value, comma-separated, or range e.g. `100-150`.
- `-r, --right`: Option right — `PUT` or `CALL`.
- `-I, --include-expired`: Include expired contracts.

#### Optional — connection
- `-H, --host`: IB host (default: `127.0.0.1`).
- `-p, --port`: IB port (default: `7497`).
- `-i, --client-id`: Client ID (default: `1`).
- `--timeout`: Connection timeout in seconds (default: `4.0`).
- `--readonly`: Read-only connection (default: `True`).

#### Optional — output
- `-f, --output-format`: `json` (default) or `text`.
- `-v, --verbose`: Verbose output.

### Behavior
- Connects read-only by default.
- Returns a list when multiple contracts match; single object otherwise.
- Outputs JSON or formatted text.

### Example Output
```json
{"conId": 265598, "symbol": "AAPL", "secType": "STK", "exchange": "SMART", "currency": "USD", "fullName": "APPLE INC"}
```
