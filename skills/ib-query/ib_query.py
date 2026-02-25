#!/usr/bin/env python3
"""
IB Query CLI Tool

Query Interactive Brokers for contract details.
"""

import argparse
import json
import logging
import sys
from ib_insync import IB, Contract
from shared.ib_connection import IBConnection

def main():
    # Suppress ib_insync ambiguous contract warnings
    logging.getLogger('ib_insync').setLevel(logging.ERROR)
    
    parser = argparse.ArgumentParser(description="Query IB for contract details.")
    parser.add_argument('-s', '--symbol', required=True, help='Ticker symbol')
    parser.add_argument('-t', '--sec-type', required=True, choices=['STK', 'FUT', 'OPT', 'CASH', 'CFD', 'BOND', 'FOP', 'WAR', 'IOPT', 'CMDTY', 'BAG', 'NEWS'], help='Security type')
    parser.add_argument('-e', '--exchange', default='SMART', help='Exchange')
    parser.add_argument('-c', '--currency', default='USD', help='Currency')
    parser.add_argument('-x', '--expiry', help='Expiry date (YYYYMMDD)')
    parser.add_argument('-k', '--strike', type=float, help='Strike price (for options)')
    parser.add_argument('-r', '--right', choices=['PUT', 'CALL'], help='Option right')
    parser.add_argument('-I', '--include-expired', action='store_true', help='Include expired contracts')
    parser.add_argument('-H', '--host', default='127.0.0.1', help='IB host')
    parser.add_argument('-p', '--port', type=int, default=7497, help='IB port')
    parser.add_argument('-i', '--client-id', type=int, default=1, help='Client ID')
    parser.add_argument('--timeout', type=float, default=4.0, help='Connection timeout')
    parser.add_argument('--readonly', action='store_true', default=True, help='Read-only connection')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose output')
    parser.add_argument('-f', '--output-format', choices=['json', 'text'], default='json', help='Output format')

    args = parser.parse_args()

    try:
        with IBConnection.connect(host=args.host, port=args.port, clientId=args.client_id, timeout=args.timeout, readonly=args.readonly) as ib:
            contract = Contract(symbol=args.symbol, secType=args.sec_type, exchange=args.exchange, currency=args.currency)
            if args.expiry:
                contract.lastTradeDateOrContractMonth = args.expiry
            if args.strike is not None:
                contract.strike = args.strike
            if args.right:
                contract.right = 'P' if args.right == 'PUT' else 'C'
            if args.include_expired:
                contract.includeExpired = True
            
            if args.verbose:
                print(f"Qualifying contract: {contract}", file=sys.stderr)
            
            ib.qualifyContracts(contract)  # Qualify in place; proceed regardless
            
            if args.verbose:
                print(f"Querying details for: {contract}", file=sys.stderr)
            
            details = ib.reqContractDetails(contract)
            if not details:
                result = {"error": "No contract details found"}
            else:
                results = []
                for detail in details:
                    min_tick = detail.minTick or 0.01
                    multiplier = float(detail.contract.multiplier or 1)
                    tick_value = min_tick * multiplier
                    
                    raw = {
                        "conid": detail.contract.conId,
                        "symbol": detail.contract.symbol,
                        "local_symbol": detail.contract.localSymbol,
                        "exchange": detail.contract.exchange,
                        "currency": detail.contract.currency,
                        "sec_type": detail.contract.secType,
                        "long_name": detail.longName,
                        "industry": detail.industry,
                        "category": detail.category,
                        "sub_category": detail.subcategory,
                        "min_tick": min_tick,
                        "tick_value": tick_value,
                        "contract_month": detail.contractMonth,
                        "expiration_date": detail.contract.lastTradeDateOrContractMonth,
                        "under_conid": getattr(detail.contract, 'underConId', None),
                        "strike": detail.contract.strike,
                        "right": detail.contract.right,
                        "multiplier": multiplier,
                        "time_zone_id": detail.timeZoneId,
                    }
                    # Filter out None and empty strings
                    filtered = {k: v for k, v in raw.items() if v is not None and v != ""}
                    results.append(filtered)
                result = results if len(results) > 1 else results[0]
        
        if args.output_format == 'json':
            print(json.dumps(result, indent=2))
        else:
            if isinstance(result, list):
                print(f"Found {len(result)} contracts:")
                for i, r in enumerate(result, 1):
                    print(f"{i}. {r['symbol']} ({r['conId']}) - {r['fullName']} | {r['secType']} @ {r['exchange']} ({r['currency']})")
            elif "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"Contract: {result['symbol']} ({result['conId']}) - {result['fullName']}")
                print(f"Type: {result['secType']}, Exchange: {result['exchange']}, Currency: {result['currency']}")
    
    except ConnectionError as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Query Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()