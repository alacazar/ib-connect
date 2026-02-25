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
import logging
from datetime import datetime, timedelta
import pytz
from ib_insync import IB, Contract, util
from job_queue import JobQueue

# Add shared to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from shared.ib_connection import IBConnection

def get_chunk_days(bar_size):
    # Parse bar_size, e.g., '1min' -> 1 min, '1hour' -> 60 min, '1day' -> 1440 min
    match = re.match(r'(\d+)\s*(min|hour|day)s?', bar_size, re.IGNORECASE)
    if match:
        num = int(match.group(1))
        unit = match.group(2).lower()
        if unit == 'min':
            minutes = num
        elif unit == 'hour':
            minutes = num * 60
        elif unit == 'day':
            minutes = num * 1440
        else:
            minutes = num  # fallback
        days = 7 * minutes
        return min(days, 365)  # Cap at 1 year to avoid excessive chunks
    return 7  # Default

def download_data(ib, conid, start, end, bar_size, show, output_dir, max_retries=3, chunk_duration='7 D', use_rth=False, verbose=False):
    # Format bar_size to IB's required format
    bar_size_formatted = (bar_size.replace('min', ' min')
                          .replace('hour', ' hour')
                          .replace('day', ' day')
                          .replace('sec', ' sec')
                          .replace('week', ' W')
                          .replace('month', ' M'))
    
    logging.info(f"Download params: conid={conid}, start={start}, end={end}, bar_size='{bar_size}' -> '{bar_size_formatted}', show={show}, duration={chunk_duration}")
    
    contract = Contract(conId=conid)
    contract.includeExpired = False
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
                    barSizeSetting=bar_size_formatted,
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
        filename = f"{conid}_{bar_size}.csv"
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False)
        return filepath
    return None

def process_job(job_key, params):
    try:
        chunk_days = get_chunk_days(params['bar_size'])
        chunk_duration = f"{chunk_days} D"
        
        with IBConnection.connect(
            host=params['host'],
            port=params['port'],
            clientId=params['client_id'],
            timeout=params['timeout'],
            readonly=True
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
                chunk_duration,
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
                instruction = f"Download job {job_key} {status}. Result path: {result_path or 'N/A'}. Error: {error_msg or 'None'}. Proceed with next steps."
                payload = {
                    "message": instruction,
                    "agentId": params['agent'],
                    "name": "DownloadComplete",
                    "sessionKey": f"hook:download:{job_key}",
                    "thinking": "low",
                    "deliver": True
                }
                headers = {}
                # If token in config, add headers
                if 'token' in config:
                    headers["Authorization"] = f"Bearer {config['token']}"
                response = requests.post(webhook_url, json=payload, headers=headers)
                logging.info(f"Notification sent for job {job_key}, response: {response.status_code}")
        except Exception as notify_e:
            logging.error(f"Notification failed for job {job_key}: {notify_e}")
    
    return status, result_path, error_msg

def main():
    logging.basicConfig(
        filename=os.path.join(os.path.dirname(__file__), 'download_service.log'),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Download service started")
    
    queue = JobQueue()
    while True:
        try:
            job = queue.get_pending_job()
            if job:
                job_key, params = job
                logging.info(f"Processing job {job_key} for conid {params['conid']}")
                status, result_path, error_msg = process_job(job_key, params)
                logging.info(f"Job {job_key} completed with status: {status}")
                if error_msg:
                    logging.error(f"Job {job_key} error: {error_msg}")
                queue.update_status(job_key, status, result_path, error_msg)
            time.sleep(5)  # Poll interval
        except Exception as e:
            logging.error(f"Service error: {e}")
            time.sleep(10)  # Longer sleep on error

if __name__ == '__main__':
    main()