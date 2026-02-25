#!/usr/bin/env python3
"""
Data Upload Service

Monitors folder for JSON/CSV files, processes contracts and OHLCV data, inserts to PostgreSQL.
"""

import os
import json
import logging
import hashlib
import shutil
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psycopg2
from psycopg2.extras import execute_values

# Expected contract schema keys
CONTRACT_KEYS = {
    'conid', 'symbol', 'local_symbol', 'exchange', 'currency', 'sec_type',
    'long_name', 'industry', 'category', 'sub_category', 'min_tick', 'tick_value',
    'contract_month', 'expiration_date', 'under_conid', 'strike', 'right', 'multiplier', 'time_zone_id'
}

def compute_conid(symbol, exchange, currency, sec_type):
    """Compute uint64 hash as conid for non-IB contracts."""
    key = f"{symbol}{exchange}{currency}{sec_type}".encode('utf-8')
    hash_obj = hashlib.sha256(key)
    # Take first 8 bytes as uint64
    return int.from_bytes(hash_obj.digest()[:8], byteorder='big', signed=False)

def validate_contract(data):
    """Validate contract JSON against schema."""
    if not isinstance(data, dict):
        return False
    # Check if all keys are present (allowing extras)
    if not CONTRACT_KEYS.issubset(data.keys()):
        missing = CONTRACT_KEYS - set(data.keys())
        logging.error(f"Missing keys: {missing}")
        return False
    # Basic type checks (can expand)
    if not isinstance(data['conid'], (int, type(None))):
        logging.error("conid must be int or None")
        return False
    return True

def process_contract_file(filepath, conn, schema, table):
    """Process a contract JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if not validate_contract(data):
            return False
        
        # Compute conid if missing
        if data['conid'] is None:
            data['conid'] = compute_conid(data['symbol'], data['exchange'], data['currency'], data['sec_type'])
        
        # Insert to DB
        with conn.cursor() as cur:
            columns = list(data.keys())
            values = [data[k] for k in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {schema}.{table} ({', '.join(columns)}) VALUES ({placeholders}) ON CONFLICT (conid) DO NOTHING"
            cur.execute(query, values)
        conn.commit()
        logging.info(f"Inserted contract {data['conid']}")
        return True
    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")
        return False

class UploadHandler(FileSystemEventHandler):
    def __init__(self, input_folder, processed_folder, error_folder, conn):
        self.input_folder = input_folder
        self.processed_folder = processed_folder
        self.error_folder = error_folder
        self.conn = conn
        os.makedirs(processed_folder, exist_ok=True)
        os.makedirs(error_folder, exist_ok=True)

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        filename = os.path.basename(filepath)
        
        if filename.endswith('.json'):
            success = process_contract_file(filepath, self.conn, 'finance', 'contracts')
            if success:
                shutil.move(filepath, os.path.join(self.processed_folder, filename))
            else:
                shutil.move(filepath, os.path.join(self.error_folder, filename))

def main():
    logging.basicConfig(
        filename='data_upload.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    input_folder = config['input_folder']
    processed_folder = config['processed_folder']
    error_folder = config['error_folder']
    db_uri = os.environ.get('PG_URI', config.get('db_uri', ''))
    
    if not db_uri:
        logging.error("No DB URI provided")
        return
    
    conn = psycopg2.connect(db_uri)
    
    observer = Observer()
    handler = UploadHandler(input_folder, processed_folder, error_folder, conn)
    observer.schedule(handler, input_folder, recursive=False)
    observer.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    conn.close()

if __name__ == '__main__':
    main()