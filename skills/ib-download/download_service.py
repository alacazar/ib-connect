#!/usr/bin/env python3
"""
IB Download Service

Background service to process download jobs.
"""

import time
import json
import os
import sys
import re
from datetime import datetime, timedelta
import pytz
from ib_insync import IB, Contract, util
from queue import JobQueue

# Add shared to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from shared.ib_connection import IBConnection

def get_chunk_days(bar_size):
    # Parse bar_size, e.g., '1min' -> 1, '5mins' -> 5
    match = re.match(r'(\d+)', bar_size)
    if match:
        minutes = int(match.group(1))
        return 7 * minutes  # Scale linearly
    return 7  # Default

def download_data(ib, conid, start, end, bar_size, show, output_dir, max_retries=3, chunk_duration='7 D', use_rth=False, verbose=False):
    contract = Contract(conId=conid)
    ib.qualifyContracts(contract)
    
    eastern = pytz.timezone('US/Eastern')
    end_dt = eastern.localize(datetime.strptime(end, '%Y-%m-%d'))
    
    all_bars = []
    chunk_days = get_chunk_days(bar_size)
    duration_str = f'{chunk_days} D'
    
    while True:
        for attempt in range(max_retries):
            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_dt,
                    durationStr=duration_str,
                    barSizeSetting=bar_size,
                    whatToShow=show,
                    useRTH=use_rth,
                    formatDate=1,
                    timeout=30
                )
                if bars:
                    all_bars.extend(bars)
                    if verbose:
                        print(f"  + {len(bars)} bars (to {bars[0].date})")
                    end_dt = bars[0].date - timedelta(seconds=1)
                    time.sleep(20)  # Pacing
                    break
                else:
                    end_dt = None
                    break
            except Exception as e:
                if verbose:
                    print(f"  Retry {attempt+1}: {e}")
                time.sleep(30)
                if attempt == max_retries - 1:
                    raise e
        if not end_dt or end_dt < eastern.localize(datetime.strptime(start, '%Y-%m-%d')):
            break
    
    if all_bars:
        df = util.df(all_bars)
        df = df.sort_values('date').drop_duplicates()
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{conid}_{bar_size.replace(' ', '')}.csv"
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False)
        return filepath
    return None

def process_job(job_key, params):
    try:
        with IBConnection.connect(
            host=params['host'],
            port=params['port'],
            clientId=params['client_id'],
            timeout=params['timeout'],
            readonly=params['readonly']
        ) as ib:
            result_path = download_data(
                ib,
                params['conid'],
                params['start'],
                params['end'],
                params['bar_size'],
                params['show'],
                params['output_dir'],
                params['max_retries'],
                params['chunk_duration'],
                params['use_rth'],
                verbose=False  # Can add verbose to params
            )
            if result_path:
                status, result_path, error_msg = 'completed', result_path, None
            else:
                status, result_path, error_msg = 'completed', None, 'No data found'
    except Exception as e:
        status, result_path, error_msg = 'failed', None, str(e)
    
    # Notify via webhook if agent specified
    if params.get('agent'):
        try:
            import requests
            import json
            with open(os.path.join(os.path.dirname(__file__), 'config.json'), 'r') as f:
                config = json.load(f)
            webhook_url = config.get('webhook_url')
            if webhook_url:
                message = {
                    'event': 'download_complete',
                    'job_key': job_key,
                    'agent': params['agent'],
                    'status': status,
                    'result_path': result_path,
                    'error': error_msg
                }
                requests.post(webhook_url, json=message)
        except Exception as notify_e:
            # Log but don't fail job
            pass
    
    return status, result_path, error_msg

def main():
    queue = JobQueue()
    while True:
        job = queue.get_pending_job()
        if job:
            job_key, params = job
            status, result_path, error_msg = process_job(job_key, params)
            queue.update_status(job_key, status, result_path, error_msg)
            if params.get('callback_session'):
                message = {
                    'event': 'download_complete',
                    'job_key': job_key,
                    'status': status,
                    'result_path': result_path,
                    'error': error_msg
                }
                sessions_send(session_key=params['callback_session'], message=json.dumps(message))
        time.sleep(5)  # Poll interval

if __name__ == '__main__':
    main()