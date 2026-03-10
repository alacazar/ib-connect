#!/usr/bin/env python3
"""
IB Data Downloader GUI - Flask Web App
"""

from flask import Flask, render_template, request, jsonify
import subprocess
import json
import os
import sys
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


from ib_connect.skills.ib_query.query import query_ib
from ib_connect.skills.ib_download.job_queue import JobQueue

app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# Config
CONFIG_FILE = 'config.json'
DEFAULT_CONFIG = {
    'contracts_folder': 'C:\\Users\\clawuser\\.openclaw\\shared\\data\\contracts\\',
    'ib_host': '127.0.0.1',
    'ib_port': 7497,
    'ib_client_id': 77
}

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    return DEFAULT_CONFIG

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=2)

config = load_config()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/query', methods=['POST'])
def query_contracts():
    data = request.json
    # Include configured IB params to avoid conflicts
    data['host'] = config.get('ib_host', '127.0.0.1')
    data['port'] = config.get('ib_port', 7497)
    data['client_id'] = config.get('ib_client_id', 1)
    def run_query():
        return asyncio.run(query_ib(data))

    try:
        with ThreadPoolExecutor() as executor:
            future = executor.submit(run_query)
            result = future.result()
        if isinstance(result, dict) and 'error' in result:
            return jsonify({'success': False, 'error': result['error']})
        contracts = result if isinstance(result, list) else [result] if result else []
        contracts = [c for c in contracts if c is not None]  # Filter out nulls
        return jsonify({'success': True, 'contracts': contracts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/save_contracts', methods=['POST'])
def save_contracts():
    data = request.json
    selected = data.get('selected', [])
    # Add defaults for missing required fields
    for contract in selected:
        if 'time_zone_id' not in contract or not contract['time_zone_id']:
            contract['time_zone_id'] = 'US/Eastern'
        if 'min_tick' not in contract or not contract['min_tick']:
            contract['min_tick'] = 0.01
        if 'tick_value' not in contract or not contract['tick_value']:
            contract['tick_value'] = contract.get('min_tick', 0.01) * contract.get('multiplier', 1)
        if 'multiplier' not in contract or not contract['multiplier']:
            contract['multiplier'] = 1
    # Omit empty strings and zero values, keep None for db defaults
    cleaned_selected = []
    for contract in selected:
        cleaned = {k: v for k, v in contract.items() if v not in ('', 0)}
        cleaned_selected.append(cleaned)
    folder = config['contracts_folder']
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(folder, f'selected_contracts_{timestamp}.json')
    with open(file_path, 'w') as f:
        json.dump(cleaned_selected, f, indent=2)
    return jsonify({'success': True, 'file': file_path})

@app.route('/download', methods=['POST'])
def download_data():
    data = request.json
    contracts = data.get('contracts', [])
    downloads_folder = config.get('downloads_folder', r'C:\Users\clawuser\.openclaw\shared\data\downloads')
    job_queue_db = config.get('job_queue_db', r'C:\Users\clawuser\.openclaw\shared\services\ib_download_service\jobs.db')
    queue = JobQueue(job_queue_db)
    jobs = []
    for contract in contracts:
        params = {
            'conid': contract['conid'],
            'start': contract['start_date'],
            'end': contract['end_date'],
            'bar_size': contract['bar_size'],
            'show': contract.get('show', 'TRADES'),
            'msg': f"Download {contract['symbol']} data ({contract.get('show', 'TRADES')})"
        }
        try:
            job_key = queue.submit_job(params)
            jobs.append({'contract': contract['symbol'], 'job_key': job_key, 'status': 'submitted'})
        except Exception as e:
            jobs.append({'contract': contract['symbol'], 'error': str(e)})
    return jsonify({'jobs': jobs})

@app.route('/job_status/<job_key>')
def job_status(job_key):
    try:
        job_queue_db = config['job_queue_db']
        queue = JobQueue(job_queue_db)
        status_data = queue.get_status(job_key)
        if status_data['status'] == 'not_found':
            return jsonify({'status': 'error', 'details': 'Job not found'})
        details = f"Status: {status_data.get('status', 'unknown')}"
        if status_data.get('message'):
            details += f" - {status_data['message']}"
        if status_data.get('error'):
            details += f" - Error: {status_data['error']}"
        return jsonify({'status': 'ok', 'details': details})
    except Exception as e:
        return jsonify({'status': 'error', 'details': str(e)})

@app.route('/cancel_job/<job_key>', methods=['POST'])
def cancel_job(job_key):
    try:
        job_queue_db = config['job_queue_db']
        queue = JobQueue(job_queue_db)
        status_data = queue.get_status(job_key)
        if status_data['status'] == 'pending':
            queue.remove_job(job_key)
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': 'Job not pending'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host=config.get('host', '127.0.0.1'), port=config.get('port', 5000))