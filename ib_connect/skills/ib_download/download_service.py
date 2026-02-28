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
import psutil
import pandas as pd
from ib_insync import IB, Contract, util
from job_queue import JobQueue

# Add shared to path
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
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

def download_data(ib, conid, start, end, bar_size, show, output_dir, max_retries=3, chunk_duration='7 D', use_rth=False, verbose=False, progress_callback=None, format='csv'):

    contract = Contract(conId=conid)
    contract.includeExpired = False
    ib.qualifyContracts(contract)
    # Get contract details for timezone
    details = ib.reqContractDetails(contract)
    if details:
        tz = details[0].timeZoneId  # e.g., 'US/Eastern'
        tz = tz.strip() if tz else ''
        if not tz:
            tz = 'US/Eastern'  # fallback for empty timezone
        tz_filename = tz.replace('/', '-').lower()  # 'us-eastern'
        contract_tz = pytz.timezone(tz)
    else:
        tz = 'US/Eastern'  # fallback
        tz_filename = 'us-eastern'
        contract_tz = pytz.timezone(tz)

    end_dt = contract_tz.localize(datetime.strptime(end, '%Y-%m-%d').replace(hour=16, minute=15))
    start_dt = contract_tz.localize(datetime.strptime(start, '%Y-%m-%d'))

    logging.info(f"Download params: conid={conid}, start={start}, end={end_dt}, bar_size='{bar_size}', show={show}, duration={chunk_duration}")
   
    all_bars = []
    chunk_count = 0

    if progress_callback:
        progress_callback(f"Starting data download for conid {conid}")

    while True:
        for attempt in range(max_retries):
            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_dt,
                    durationStr=chunk_duration,
                    barSizeSetting=bar_size,
                    whatToShow=show,
                    useRTH=use_rth,
                    formatDate=1,
                    timeout=30
                )
                if bars:
                    all_bars.extend(bars)
                    chunk_count += 1
                    if progress_callback:
                        progress_callback(f"Downloaded chunk {chunk_count}")
                    if verbose:
                        print(f"  + {len(bars)} bars (to {bars[0].date})")
                    # bars[0].date is naive datetime in exchange tz, localize to contract tz
                    if isinstance(bars[0].date, datetime):
                        naive_date = bars[0].date.replace(tzinfo=None) if bars[0].date.tzinfo else bars[0].date
                        end_dt = contract_tz.localize(naive_date) - timedelta(seconds=1)
                    else:
                        # If date, combine with time and localize
                        dt = datetime.combine(bars[0].date, datetime.min.time())
                        naive_date = dt.replace(tzinfo=None) if dt.tzinfo else dt
                        end_dt = contract_tz.localize(naive_date) - timedelta(seconds=1)
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
        if not end_dt or end_dt < start_dt:
            break
    
    if all_bars:
        df = util.df(all_bars)
        df = df.rename(columns={'barCount': 'trades'}).sort_values('date').drop_duplicates()
        os.makedirs(output_dir, exist_ok=True)
        # Format filename for data_upload: <localSymbol>.<conid>.<barsize>.<tz>.csv or .json (spaces in localSymbol replaced with _)
        bar_size_clean = bar_size.replace(' ', '').replace('min', 'min').replace('hour', 'hour').replace('day', 'day')
        local_symbol_clean = contract.localSymbol.replace(' ', '_')
        if format == 'json':
            filename = f"{local_symbol_clean}.{conid}.{bar_size_clean}.{tz_filename}.json"
            filepath = os.path.join(output_dir, filename)
            if progress_callback:
                progress_callback("Saving data to JSON")
            df.to_json(filepath, orient='records')
        else:
            filename = f"{local_symbol_clean}.{conid}.{bar_size_clean}.{tz_filename}.csv"
            filepath = os.path.join(output_dir, filename)
            if progress_callback:
                progress_callback("Saving data to CSV")
            df.to_csv(filepath, index=False)
        return filepath
    return None

def process_job(job_key, params):
    queue = JobQueue()  # To update status
    try:
        # Read output_dir from config
        with open(os.path.join(os.path.dirname(__file__), 'config.json'), 'r') as f:
            config = json.load(f)
        output_dir = config.get('output_dir', './data')

        chunk_days = get_chunk_days(params['bar_size'])
        chunk_duration = f"{chunk_days} D"

        queue.update_status(job_key, 'processing', message=f"Starting download for conid {params['conid']}")

        result_path = None
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                with IBConnection.connect(
                    host=config.get('ib_host', '127.0.0.1'),
                    port=config.get('ib_port', 7497),
                    clientId=config.get('ib_client_id', 1),
                    timeout=config.get('timeout', 4.0),
                    readonly=True
                ) as ib:
                    progress_callback = lambda msg: queue.update_status(job_key, 'processing', message=msg)
                    result_path = download_data(
                        ib,
                        params['conid'],
                        params['start'],
                        params['end'],
                        params['bar_size'],
                        params.get('show', 'TRADES'),
                        output_dir,
                        config.get('max_retries', 3),
                        chunk_duration,
                        params.get('use_rth', False),
                        verbose=False,  # Can add verbose to params
                        progress_callback=progress_callback,
                        format=params.get('format', 1)
                    )
                break  # Success
            except Exception as e:
                if attempt < max_attempts - 1:
                    queue.update_status(job_key, 'processing', message=f"Attempt {attempt + 1} failed, retrying in 60s: {e}")
                    time.sleep(60)
                else:
                    raise e

        queue.update_status(job_key, 'processing', message=f"Saving data to CSV for conid {params['conid']}")
        if result_path:
            # Check data coverage
            try:
                import pandas as pd
                df = pd.read_csv(result_path)
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    earliest = df['date'].min().date()
                    latest = df['date'].max().date()
                    requested_start = datetime.strptime(params['start'], '%Y-%m-%d').date()
                    requested_end = datetime.strptime(params['end'], '%Y-%m-%d').date()
                    if earliest > requested_start or latest < requested_end:
                        logging.warning(f"Partial data for job {job_key}: requested {requested_start} to {requested_end}, got {earliest} to {latest}")
                        message = f"Download completed (partial): {os.path.basename(result_path)} - data from {earliest} to {latest}"
                    else:
                        message = f"Download completed: {os.path.basename(result_path)}"
                else:
                    message = "Download completed: No data found"
            except Exception as check_e:
                logging.error(f"Data check failed for job {job_key}: {check_e}")
                message = f"Download completed: {os.path.basename(result_path)}"
            status, error_msg = 'completed', None
        else:
            status, result_path, error_msg = 'completed', None, 'No data found'
            message = "Download completed: No data found"
    except Exception as e:
        logging.error(f"Job {job_key} failed with exception: {type(e).__name__}: {e}", exc_info=True)
        error_msg = f"{type(e).__name__}: {str(e) or 'No message'}"
        status, result_path = 'failed', None
        message = f"Download failed: {error_msg}"
    
    # Notify via webhook if agent specified
    if params.get('agent'):
        try:
            import requests
            with open(os.path.join(os.path.dirname(__file__), 'config.json'), 'r') as f:
                config = json.load(f)
            webhook_url = config.get('webhook_url')
            if webhook_url:
                webhook_message = {
                    'event': 'download_complete',
                    'job_key': job_key,
                    'agent': params['agent'],
                    'status': status,
                    'result_path': result_path,
                    'error': error_msg
                }
                custom_msg = params.get('msg', '')
                instruction = f"Download job {job_key} {status}. Result path: {result_path or 'N/A'}. Error: {error_msg or 'None'}. {custom_msg}"
                payload = {
                    "message": instruction,
                    "agentId": params['agent'],
                    "name": "DownloadComplete",
                    "thinking": "low",
                    "deliver": True
                }
                headers = {}
                # If token in config, add headers
                if 'token' in config:
                    headers["Authorization"] = f"Bearer {config['token']}"
                response = requests.post(webhook_url, json=payload, headers=headers)
                logging.info(f"Notification sent for job {job_key}, response: {response.status_code} - {response.text}")
        except Exception as notify_e:
            logging.error(f"Notification failed for job {job_key}: {notify_e}")
    
    return status, result_path, error_msg, message

def main():
    lock_file = os.path.join(os.path.dirname(__file__), 'service.lock')
    if os.path.exists(lock_file):
        with open(lock_file, 'r') as f:
            pid = f.read().strip()
        try:
            pid_int = int(pid)
            if any(p.pid == pid_int for p in psutil.process_iter() if 'python' in p.name().lower()):
                logging.error("Another instance is running")
                return
        except (ValueError, psutil.NoSuchProcess):
            pass  # Lock file stale
    with open(lock_file, 'w') as f:
        f.write(str(os.getpid()))

    try:
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
                    status, result_path, error_msg, message = process_job(job_key, params)
                    logging.info(f"Job {job_key} completed with status: {status}")
                    if error_msg:
                        logging.error(f"Job {job_key} error: {error_msg}")
                    queue.update_status(job_key, status, result_path, error_msg, message)
                time.sleep(5)  # Poll interval
            except Exception as e:
                logging.error(f"Service error: {e}")
                time.sleep(10)  # Longer sleep on error

    except KeyboardInterrupt:
        pass
    finally:
        if os.path.exists(lock_file):
            os.remove(lock_file)

if __name__ == '__main__':
    main()