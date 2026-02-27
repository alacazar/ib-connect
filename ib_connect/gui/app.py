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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


from ib_connect.skills.ib_query.query import query_ib
from ib_connect.skills.ib_download.download import submit_download_job, get_job_status

app = Flask(__name__)

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
    def run_query():
        return asyncio.run(query_ib(data))

    try:
        with ThreadPoolExecutor() as executor:
            future = executor.submit(run_query)
            result = future.result()
        if isinstance(result, dict) and 'error' in result:
            return jsonify({'success': False, 'error': result['error']})
        return jsonify({'success': True, 'contracts': result if isinstance(result, list) else [result]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/save_contracts', methods=['POST'])
def save_contracts():
    data = request.json
    selected = data.get('selected', [])
    folder = config['contracts_folder']
    os.makedirs(folder, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = os.path.join(folder, f'selected_contracts_{timestamp}.json')
    with open(file_path, 'w') as f:
        json.dump(selected, f, indent=2)
    return jsonify({'success': True, 'file': file_path})

@app.route('/download', methods=['POST'])
def download_data():
    data = request.json
    contracts = data.get('contracts', [])
    downloads_folder = config['downloads_folder']
    jobs = []
    for contract in contracts:
        params = {
            'conid': contract['conid'],
            'start': contract['start_date'],
            'end': contract['end_date'],
            'bar_size': contract['bar_size'],
            'output_dir': downloads_folder,
            'agent': 'system_developer',
            'msg': f"Download {contract['symbol']} data"
        }
        try:
            job_key = submit_download_job(params)
            jobs.append({'contract': contract['symbol'], 'job_key': job_key, 'status': 'submitted'})
        except Exception as e:
            jobs.append({'contract': contract['symbol'], 'error': str(e)})
    return jsonify({'jobs': jobs})

@app.route('/job_status/<job_key>')
def job_status(job_key):
    try:
        status_data = get_job_status(job_key)
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

if __name__ == '__main__':
    app.run(debug=True, host=config.get('host', '127.0.0.1'), port=config.get('port', 5000))