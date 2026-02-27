#!/usr/bin/env python3
"""
IB Query CLI Tool

Query Interactive Brokers for contract details.
"""

import argparse
import asyncio
import json
import logging
import sys
from query import query_ib

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
        result = asyncio.run(query_ib(vars(args)))
        
        if args.output_format == 'json':
            print(json.dumps(result, indent=2))
        else:
            if isinstance(result, list):
                print(f"Found {len(result)} contracts:")
                for i, r in enumerate(result, 1):
                    print(f"{i}. {r['symbol']} ({r['conId']}) - {r.get('long_name', 'N/A')} | {r['sec_type']} @ {r['exchange']} ({r['currency']})")
            elif "error" in result:
                print(f"Error: {result['error']}")
            else:
                print(f"Contract: {result['symbol']} ({result['conId']}) - {result.get('long_name', 'N/A')}")
                print(f"Type: {result['sec_type']}, Exchange: {result['exchange']}, Currency: {result['currency']}")
    
    except ConnectionError as e:
        print(f"Connection Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Query Error: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == '__main__':
    main()