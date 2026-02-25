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
import pandas as pd
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

def process_ohlcv_file(filepath, conn, schema):
    """Process an OHLCV CSV file."""
    try:
        # Extract from filename: <symbol>.<conid>.<barsize>.csv
        filename = os.path.basename(filepath)
        if not filename.endswith('.csv'):
            return False
        parts = filename[:-4].split('.')
        if len(parts) != 3:
            logging.error(f"Invalid filename format: {filename}")
            return False
        symbol, conid_str, barsize_file = parts
        try:
            conid = int(conid_str)
        except ValueError:
            logging.error(f"Invalid conid in filename: {conid_str}")
            return False
        
        # Map barsize
        if barsize_file == '1min':
            table = 'ohlcv_1m'
            expected_barsize = '1min'
        elif barsize_file == '1day':
            table = 'ohlcv_1d'
            expected_barsize = '1day'
        else:
            logging.error(f"Unsupported barsize in filename: {barsize_file}")
            return False
        
        # Detect separator
        with open(filepath, 'r') as f:
            sample = f.read(1024)
            if '\t' in sample and sample.count('\t') > sample.count(','):
                sep = '\t'
            else:
                sep = ','
        
        # Read CSV
        df = pd.read_csv(filepath, sep=sep)
        if df.empty:
            logging.error(f"Empty CSV: {filename}")
            return False
        
        # Map columns case-insensitively
        df.columns = df.columns.str.lower()
        col_map = {}
        time_cols = ['date', 'datetime', 'time', 'timestamp']
        for col in time_cols:
            if col in df.columns:
                col_map['time'] = col
                break
        else:
            logging.error(f"No time column found in {filename}")
            return False
        
        mandatory = ['open', 'high', 'low', 'close']
        for col in mandatory:
            if col not in df.columns:
                logging.error(f"Missing mandatory column {col} in {filename}")
                return False
            col_map[col] = col
        
        optional = ['volume', 'trades', 'adjusted_close', 'adj_close']
        for col in optional:
            if col in df.columns:
                col_map[col] = col
            elif col == 'adj_close' and 'adjusted_close' in df.columns:
                col_map['adjusted_close'] = 'adjusted_close'
        
        # Deduce bar size from data
        time_col = col_map['time']
        if len(df) < 2:
            deduced_barsize = expected_barsize  # Assume
        else:
            times = pd.to_datetime(df[time_col].head(10))
            diffs = times.diff().dropna()
            avg_diff = diffs.mean()
            if avg_diff < pd.Timedelta('2 min'):
                deduced_barsize = '1min'
            elif avg_diff >= pd.Timedelta('1 day'):
                deduced_barsize = '1day'
            else:
                logging.error(f"Could not deduce bar size from time diffs in {filename}")
                return False
        
        if deduced_barsize != expected_barsize:
            logging.error(f"Barsize mismatch: file={expected_barsize}, deduced={deduced_barsize} in {filename}")
            return False
        
        # Prepare data
        df['conid'] = conid
        df['symbol'] = symbol
        
        # Rename columns
        rename_map = {v: k for k, v in col_map.items()}
        if 'adj_close' in rename_map:
            rename_map['adjusted_close'] = 'adj_close'
        df = df.rename(columns=rename_map)
        
        # Select columns
        insert_cols = ['conid', 'symbol', 'time', 'open', 'high', 'low', 'close'] + [k for k in optional if k in df.columns]
        df = df[insert_cols]
        
        # Convert time to timestamp
        df['time'] = pd.to_datetime(df['time'])
        
        # Batch insert with conflict handling
        batch_size = 1000
        quoted_cols = [f'"{col}"' for col in insert_cols]
        conflict_cols = ['conid', 'time']  # Assuming primary key
        query = f"INSERT INTO {schema}.{table} ({', '.join(quoted_cols)}) VALUES %s ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"
        
        with conn.cursor() as cur:
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                values = [tuple(row) for row in batch.values]
                execute_values(cur, query, values)
        conn.commit()
        logging.info(f"Inserted {len(df)} OHLCV bars for {symbol} into {table}")
        return True
    except Exception as e:
        logging.error(f"Error processing OHLCV file {filepath}: {e}")
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
        
        try:
            if filename.endswith('.json'):
                success = process_contract_file(filepath, conn, 'finance', 'contracts')
                if success:
                    shutil.move(filepath, os.path.join(processed_folder, filename))
                    logging.info(f"Processed and moved {filename} to processed")
                else:
                    shutil.move(filepath, os.path.join(error_folder, filename))
                    logging.error(f"Moved {filename} to errors")
            elif filename.endswith('.csv'):
                success = process_ohlcv_file(filepath, conn, 'finance')
                if success:
                    shutil.move(filepath, os.path.join(processed_folder, filename))
                    logging.info(f"Processed and moved {filename} to processed")
                else:
                    shutil.move(filepath, os.path.join(error_folder, filename))
                    logging.error(f"Moved {filename} to errors")
        except Exception as e:
            logging.error(f"Unexpected error processing {filename}: {e}")
            # Move to errors if not already
            try:
                shutil.move(filepath, os.path.join(error_folder, filename))
            except:
                pass

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