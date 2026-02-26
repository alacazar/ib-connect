#!/usr/bin/env python3
"""
IB Download CLI Tool

Submit download jobs or query status.
"""

import argparse
import json
import sys
import csv
import os
import ipaddress
from datetime import datetime
from job_queue import JobQueue

def validate_conid(value):
    if not isinstance(value, int) or value <= 0:
        raise ValueError("conid must be a positive integer")

def validate_date(value):
    try:
        datetime.strptime(value, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Date must be in YYYY-MM-DD format")

def validate_bar_size(value):
    allowed = ['1 secs', '5 secs', '10 secs', '15 secs', '30 secs', '1 min', '2 mins', '3 mins', '4 mins', '5 mins', '10 mins', '15 mins', '20 mins', '30 mins', '1 hour', '2 hours', '3 hours', '4 hours', '8 hours', '1 day', '1W', '1M']
    if value not in allowed:
        raise ValueError(f"bar-size must be one of: {', '.join(allowed)}")

def validate_show(value):
    allowed = ['TRADES', 'MIDPOINT', 'BID', 'ASK']
    if value not in allowed:
        raise ValueError(f"show must be one of: {', '.join(allowed)}")

def validate_output_dir(value):
    if not os.path.isdir(value):
        try:
            os.makedirs(value, exist_ok=True)
        except OSError:
            raise ValueError(f"Cannot create output directory: {value}")

def validate_config_file(value):
    if not os.path.isfile(value):
        raise ValueError(f"Config file does not exist: {value}")

def validate_host(value):
    try:
        ipaddress.ip_address(value)
    except ValueError:
        # Check if hostname
        if not value or not all(c.isalnum() or c in '.-' for c in value):
            raise ValueError("host must be a valid IP address or hostname")

def validate_port(value):
    if not isinstance(value, int) or not (1 <= value <= 65535):
        raise ValueError("port must be an integer between 1 and 65535")

def validate_client_id(value):
    if not isinstance(value, int) or value < 0:
        raise ValueError("client-id must be a non-negative integer")

def validate_timeout(value):
    if not isinstance(value, (int, float)) or value <= 0:
        raise ValueError("timeout must be a positive number")

def validate_max_retries(value):
    if not isinstance(value, int) or value < 0:
        raise ValueError("max-retries must be a non-negative integer")

def validate_format(value):
    allowed = ['csv', 'json']
    if value not in allowed:
        raise ValueError(f"format must be one of: {', '.join(allowed)}")

def submit_single_job(queue, output_dir, args):
    params = {
        'conid': args.conid,
        'start': args.start,
        'end': args.end,
        'bar_size': args.bar_size,
        'show': args.show,
        'output_dir': output_dir,
        'host': args.host,
        'port': args.port,
        'client_id': args.client_id,
        'timeout': args.timeout,
        'max_retries': args.max_retries,
        'use_rth': args.use_rth,
        'format': args.format,
        'agent': args.agent,
        'msg': args.msg
    }
    job_key = queue.submit_job(params)
    print(json.dumps({'job_key': job_key}))

def submit_batch_jobs(queue, output_dir, args):
    with open(args.config_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Validate row values
            validate_conid(int(row['conid']))
            validate_date(row['start'])
            validate_date(row['end'])
            validate_bar_size(row['bar_size'])
            show = row.get('show', 'TRADES')
            validate_show(show)

            params = {
                'conid': int(row['conid']),
                'start': row['start'],
                'end': row['end'],
                'bar_size': row['bar_size'],
                'show': show,
                'output_dir': output_dir,
                'host': args.host,
                'port': args.port,
                'client_id': args.client_id,
                'timeout': args.timeout,
                'max_retries': args.max_retries,
                'use_rth': args.use_rth,
                'format': args.format,
                'agent': args.agent,
                'msg': args.msg
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
    parser.add_argument('--show', default='TRADES', choices=['TRADES', 'MIDPOINT', 'BID', 'ASK'], help='What to show')
    parser.add_argument('-f', '--config-file', help='Config file for batch')
    parser.add_argument('-H', '--host', default='127.0.0.1', help='IB host')
    parser.add_argument('-p', '--port', type=int, default=7497, help='IB port')
    parser.add_argument('-i', '--client-id', type=int, default=99, help='Client ID')
    parser.add_argument('--timeout', type=float, default=4.0, help='Timeout')
    parser.add_argument('--max-retries', type=int, default=3, help='Max retries')
    parser.add_argument('--use-rth', action='store_true', help='Use RTH')
    parser.add_argument('--format', default='csv', choices=['csv', 'json'], help='Output format')
    parser.add_argument('-A', '--agent', help='Agent name/ID for notifications')
    parser.add_argument('-m', '--msg', help='Custom message to include in notification')
    parser.add_argument('-v', '--verbose', action='store_true', help='Verbose')

    args = parser.parse_args()

    # Validate arguments
    try:
        if hasattr(args, 'conid') and args.conid is not None:
            validate_conid(args.conid)
        if hasattr(args, 'start') and args.start:
            validate_date(args.start)
        if hasattr(args, 'end') and args.end:
            validate_date(args.end)
        if hasattr(args, 'bar_size') and args.bar_size:
            validate_bar_size(args.bar_size)
        if hasattr(args, 'show') and args.show:
            validate_show(args.show)
        if hasattr(args, 'output_dir') and args.output_dir:
            validate_output_dir(args.output_dir)
        if hasattr(args, 'config_file') and args.config_file:
            validate_config_file(args.config_file)
        if hasattr(args, 'host') and args.host:
            validate_host(args.host)
        if hasattr(args, 'port') and args.port is not None:
            validate_port(args.port)
        if hasattr(args, 'client_id') and args.client_id is not None:
            validate_client_id(args.client_id)
        if hasattr(args, 'timeout') and args.timeout is not None:
            validate_timeout(args.timeout)
        if hasattr(args, 'max_retries') and args.max_retries is not None:
            validate_max_retries(args.max_retries)
        if hasattr(args, 'format') and args.format:
            validate_format(args.format)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    queue = JobQueue()

    with open(os.path.join(os.path.dirname(__file__), 'config.json'), 'r') as f:
        config = json.load(f)
    output_dir = config.get('output_dir', './data')

    if args.status:
        if not args.key:
            print("Error: --key required for --status", file=sys.stderr)
            sys.exit(1)
        status = queue.get_status(args.key)
        print(json.dumps(status))
    else:
        if args.config_file:
            submit_batch_jobs(queue, output_dir, args)
        elif args.conid and args.start and args.end and args.bar_size:
            submit_single_job(queue, output_dir, args)
        else:
            print("Error: Provide --conid --start --end --bar-size or --config-file", file=sys.stderr)
            sys.exit(1)

if __name__ == '__main__':
    main()