#!/usr/bin/env python3
"""
IB Download CLI Tool

Submit download jobs or query status.
"""

import argparse
import json
import sys
import csv
from datetime import datetime
from job_queue import JobQueue

def submit_single_job(queue, args):
    params = {
        'conid': args.conid,
        'start': args.start,
        'end': args.end,
        'bar_size': args.bar_size,
        'show': args.show,
        'output_dir': args.output_dir,
        'host': args.host,
        'port': args.port,
        'client_id': args.client_id,
        'timeout': args.timeout,
        'max_retries': args.max_retries,
        'use_rth': args.use_rth,
        'format': args.format,
        'agent': args.agent
    }
    job_key = queue.submit_job(params)
    print(json.dumps({'job_key': job_key}))

def submit_batch_jobs(queue, args):
    with open(args.config_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            params = {
                'conid': int(row['conid']),
                'start': row['start'],
                'end': row['end'],
                'bar_size': row['bar_size'],
                'show': row.get('show', 'TRADES'),
                'output_dir': args.output_dir,
                'host': args.host,
                'port': args.port,
                'client_id': args.client_id,
                'timeout': args.timeout,
                'max_retries': args.max_retries,
                'use_rth': args.use_rth,
                'format': args.format,
                'agent': args.agent
            }
            job_key = queue.submit_job(params)
            print(json.dumps({'job_key': job_key}))

def main():
    parser = argparse.ArgumentParser(description="IB Download: Submit jobs or query status.")
    
    # Status mode
    parser.add_argument('--status', action='store_true', help='Query job status')
    parser.add_argument('-k', '--key', help='Job key for status query')
    
    # Submit mode
    parser.add_argument('-c', '--conid', type=int, help='Contract ID')
    parser.add_argument('-s', '--start', help='Start date (YYYY-MM-DD)')
    parser.add_argument('-e', '--end', help='End date (YYYY-MM-DD)')
    parser.add_argument('-b', '--bar-size', choices=['1 secs', '5 secs', '10 secs', '15 secs', '30 secs', '1 min', '2 mins', '3 mins', '4 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours', '1 day', '1W', '1M'], help='Bar size')
    parser.add_argument('--show', default='TRADES', help='What to show')
    parser.add_argument('-o', '--output-dir', default='./data', help='Output directory')
    parser.add_argument('-f', '--config-file', help='Config file for batch')
    parser.add_argument('-H', '--host', default='127.0.0.1', help='IB host')
    parser.add_argument('-p', '--port', type=int, default=7497, help='IB port')
    parser.add_argument('-i', '--client-id', type=int, default=1, help='Client ID')
    parser.add_argument('--timeout', type=float, default=4.0, help='Timeout')
    parser.add_argument('--max-retries', type=int, default=3, help='Max retries')
    parser.add_argument('--use-rth', action='store_true', help='Use RTH')
    parser.add_argument('--format', default='csv', choices=['csv', 'json'], help='Output format')
    parser.add_argument('-A', '--agent', help='Agent name/ID for notifications')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose')

    args = parser.parse_args()

    # Validate dates
    try:
        datetime.strptime(args.start, '%Y-%m-%d')
        datetime.strptime(args.end, '%Y-%m-%d')
    except ValueError:
        print("Error: Dates must be in YYYY-MM-DD format", file=sys.stderr)
        sys.exit(1)

    # Validate conid if provided
    if hasattr(args, 'conid') and args.conid is not None and args.conid <= 0:
        print("Error: conid must be a positive integer", file=sys.stderr)
        sys.exit(1)

    queue = JobQueue()

    if args.status:
        if not args.key:
            print("Error: --key required for --status", file=sys.stderr)
            sys.exit(1)
        status = queue.get_status(args.key)
        print(json.dumps(status))
    else:
        if args.config_file:
            submit_batch_jobs(queue, args)
        elif args.conid and args.start and args.end and args.bar_size:
            submit_single_job(queue, args)
        else:
            print("Error: Provide --conid --start --end --bar-size or --config-file", file=sys.stderr)
            sys.exit(1)

if __name__ == '__main__':
    main()