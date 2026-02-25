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

def process_files(input_folder, processed_folder, error_folder, conn, processed_files):
    """Poll for new files and process them."""
    os.makedirs(processed_folder, exist_ok=True)
    os.makedirs(error_folder, exist_ok=True)
    
    for filename in os.listdir(input_folder):
        if filename in processed_files:
            continue
        filepath = os.path.join(input_folder, filename)
        if not os.path.isfile(filepath):
            continue
        
        processed_files.add(filename)
        
        if filename.endswith('.json'):
            success = process_contract_file(filepath, conn, 'finance', 'contracts')
            if success:
                shutil.move(filepath, os.path.join(processed_folder, filename))
                logging.info(f"Processed and moved {filename} to processed")
            else:
                shutil.move(filepath, os.path.join(error_folder, filename))
                logging.error(f"Moved {filename} to errors")

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
    
    processed_files = set()
    
    try:
        while True:
            process_files(input_folder, processed_folder, error_folder, conn, processed_files)
            time.sleep(5)  # Poll every 5 seconds
    except KeyboardInterrupt:
        pass
    conn.close()

if __name__ == '__main__':
    main()