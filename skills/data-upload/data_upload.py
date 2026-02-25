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
    """Compute hash as conid for non-IB contracts (fits in signed BIGINT)."""
    key = f"{symbol}{exchange}{currency}{sec_type}".encode('utf-8')
    hash_obj = hashlib.sha256(key)
    # Take first 7 bytes (56 bits) to fit in signed BIGINT
    return int.from_bytes(hash_obj.digest()[:7], byteorder='big', signed=False)

# Required contract schema keys
REQUIRED_KEYS = {'symbol', 'exchange', 'currency', 'sec_type', 'min_tick', 'tick_value', 'multiplier', 'time_zone_id'}

def validate_contract(data):
    """Validate contract JSON against schema."""
    if not isinstance(data, dict):
        return False
    # Check required keys are present
    if not REQUIRED_KEYS.issubset(data.keys()):
        missing = REQUIRED_KEYS - set(data.keys())
        logging.error(f"Missing required keys: {missing}")
        return False
    # Check no invalid keys
    invalid = set(data.keys()) - CONTRACT_KEYS
    if invalid:
        logging.error(f"Invalid keys: {invalid}")
        return False
    # Basic type checks
    if 'conid' in data and not isinstance(data['conid'], (int, type(None))):
        logging.error("conid must be int or None")
        return False
    if not isinstance(data['min_tick'], (int, float)):
        logging.error("min_tick must be number")
        return False
    if not isinstance(data['tick_value'], (int, float)):
        logging.error("tick_value must be number")
        return False
    if not isinstance(data['multiplier'], (int, float)):
        logging.error("multiplier must be number")
        return False
    return True

def process_contract_file(filepath, conn, schema, table):
    """Process a contract JSON file (single object or array)."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            # Array of contracts
            success = True
            for item in data:
                if not process_single_contract(item, conn, schema, table):
                    success = False
            return success
        else:
            # Single contract
            return process_single_contract(data, conn, schema, table)
    except Exception as e:
        logging.error(f"Error processing {filepath}: {e}")
        return False

def process_single_contract(data, conn, schema, table):
    """Process a single contract dict."""
    try:
        if not validate_contract(data):
            logging.error(f"Validation failed for {data.get('symbol', 'unknown')}")
            return False
        
        # Compute conid if missing
        if 'conid' not in data or data['conid'] is None:
            data['conid'] = compute_conid(data['symbol'], data['exchange'], data['currency'], data['sec_type'])
        
        # Insert to DB
        with conn.cursor() as cur:
            columns = list(data.keys())
            values = [data[k] for k in columns]
            placeholders = ', '.join(['%s'] * len(columns))
            quoted_columns = [f'"{col}"' for col in columns]
            query = f"INSERT INTO {schema}.{table} ({', '.join(quoted_columns)}) VALUES ({placeholders}) ON CONFLICT (conid) DO NOTHING"
            logging.debug(f"Executing query: {query} with values: {values}")
            cur.execute(query, values)
        conn.commit()
        logging.info(f"Inserted contract {json.dumps(data)}")
        return True
    except Exception as e:
        logging.error(f"Error inserting contract {data.get('symbol', 'unknown')}: {e}")
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
    with open('config.json', 'r') as f:
        config = json.load(f)
    
    input_folder = config['input_folder']
    processed_folder = config['processed_folder']
    error_folder = config['error_folder']
    db_uri = os.environ.get('PG_URI', os.path.expandvars(config.get('db_uri', '')))
    
    if not db_uri:
        print("No DB URI provided")
        return
    
    logging.basicConfig(
        filename='data_upload.log',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    try:
        conn = psycopg2.connect(db_uri)
        logging.info("Connected to DB")
    except Exception as e:
        logging.error(f"DB connection failed: {e}")
        return
    
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