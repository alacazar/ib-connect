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
import pytz
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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
        logging.error(f"Error processing {filepath}: {e}", exc_info=True)
        return False

def normalize_tz(tz_str):
    """Normalize timezone string to pytz format."""
    parts = tz_str.split('-')
    if len(parts) == 2 and parts[0].lower() == 'us':
        return f"US/{parts[1].capitalize()}"
    else:
        return tz_str.upper()

def parse_csv_header(filepath, barsize_tables):
    """Parse CSV header and return metadata."""
    filename = os.path.basename(filepath)
    if not filename.endswith('.csv'):
        raise ValueError("Not a CSV file")

    parts = filename[:-4].split('.')
    if len(parts) != 4:
        raise ValueError(f"Invalid filename format: {filename}")
    symbol, conid_str, barsize_file, tz_str = parts
    try:
        conid = int(conid_str)
    except ValueError:
        raise ValueError(f"Invalid conid in filename: {conid_str}")

    # Map barsize to table
    table = barsize_tables.get(barsize_file)
    if not table:
        raise ValueError(f"Unsupported barsize in filename: {barsize_file}")

    # Map barsize to time_col_name
    if barsize_file == '1min':
        time_col_name = 'time_1m'
    elif barsize_file == '1day':
        time_col_name = 'day'
    else:
        time_col_name = 'time'  # fallback

    # Normalize timezone
    tz = normalize_tz(tz_str)
    try:
        timezone = pytz.timezone(tz)
    except pytz.exceptions.UnknownTimeZoneError:
        raise ValueError(f"Invalid timezone: {tz_str}")

    # Detect separator
    with open(filepath, 'r') as f:
        sample = f.read(1024)
        sep = '\t' if '\t' in sample and sample.count('\t') > sample.count(',') else ','

    # Read CSV header
    df = pd.read_csv(filepath, sep=sep, nrows=0)
    columns = df.columns.str.lower().tolist()

    # Find time column
    time_cols = ['date', 'datetime', 'time', 'timestamp']
    time_col = next((col for col in time_cols if col in columns), None)
    if not time_col:
        raise ValueError(f"No time column found in {filename}")

    # Check mandatory columns
    mandatory = ['open', 'high', 'low', 'close']
    if not all(col in columns for col in mandatory):
        missing = [col for col in mandatory if col not in columns]
        raise ValueError(f"Missing mandatory columns: {missing}")

    # Map optional columns
    optional = ['volume', 'trades', 'adjusted_close', 'adj_close']
    col_map = {col: col for col in mandatory + [time_col]}
    for opt in optional:
        if opt in columns:
            col_map[opt] = opt
        elif opt == 'adj_close' and 'adjusted_close' in columns:
            col_map['adjusted_close'] = 'adjusted_close'

    insert_cols = ['conid', 'symbol', time_col_name] + mandatory + [k for k in optional if k in col_map]

    return symbol, conid, table, time_col_name, sep, time_col, col_map, insert_cols, timezone

def process_csv_data(filepath, sep, time_col, time_col_name, col_map, table, symbol, conid, timezone):
    """Process CSV data into DataFrame."""
    df = pd.read_csv(filepath, sep=sep)
    df.columns = df.columns.str.lower()

    # Rename columns
    rename_map = {v: k for k, v in col_map.items() if k != 'time'}
    rename_map[time_col] = time_col_name
    if 'adj_close' in rename_map:
        rename_map['adjusted_close'] = 'adj_close'
    df = df.rename(columns=rename_map)
    
    # Add conid and symbol
    df['conid'] = conid
    df['symbol'] = symbol
    
    # Localize time
    df[time_col_name] = pd.to_datetime(df[time_col_name])
    if df[time_col_name].dt.tz is None:
        df[time_col_name] = df[time_col_name].dt.tz_localize(timezone)
    else:
        df[time_col_name] = df[time_col_name].dt.tz_convert(timezone)

    # Cast types
    if table == 'ohlcv_1d':
        if 'volume' in df.columns:
            df['volume'] = df['volume'].astype(int)
        if 'trades' in df.columns:
            df['trades'] = df['trades'].astype(int)

    return df

def insert_ohlcv_data(df, conn, schema, table, insert_cols, time_col_name):
    """Insert DataFrame into database."""
    quoted_cols = [f'"{col}"' for col in insert_cols]
    conflict_cols = ['conid', time_col_name]
    query = f"INSERT INTO {schema}.{table} ({', '.join(quoted_cols)}) VALUES %s ON CONFLICT ({', '.join(conflict_cols)}) DO NOTHING"

    batch_size = 1000
    with conn.cursor() as cur:
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            values = [tuple(row) for row in batch[insert_cols].values]
            execute_values(cur, query, values)
    conn.commit()

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
        logging.error(f"Error inserting contract {data.get('symbol', 'unknown')}: {e}", exc_info=True)
        return False

def process_ohlcv_file(filepath, conn, schema, barsize_tables):
    """Process an OHLCV CSV file using parse_csv_header and process_csv_data."""
    try:
        symbol, conid, table, time_col_name, sep, time_col, col_map, insert_cols, timezone = parse_csv_header(filepath, barsize_tables)
        df = process_csv_data(filepath, sep, time_col, time_col_name, col_map, table, symbol, conid, timezone)
        insert_ohlcv_data(df, conn, schema, table, insert_cols, time_col_name)
        logging.info(f"Inserted {len(df)} OHLCV bars for {symbol} into {table}")
        return True
    except Exception as e:
        logging.error(f"Error processing OHLCV file {filepath}: {e}", exc_info=True)
        return False

class DataFileHandler(FileSystemEventHandler):
    """Watchdog handler for new data files."""
    def __init__(self, input_folder, processed_folder, error_folder, conn, config):
        self.input_folder = input_folder
        self.processed_folder = processed_folder
        self.error_folder = error_folder
        self.conn = conn
        self.schema = config.get('schema', 'finance')
        self.contract_table = config.get('contract_table', 'contracts')
        self.barsize_tables = {
            '1min': config.get('ohlcv_1m_table', 'ohlcv_1m'),
            '1day': config.get('ohlcv_1d_table', 'ohlcv_1d')
        }
        os.makedirs(self.processed_folder, exist_ok=True)
        os.makedirs(self.error_folder, exist_ok=True)

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        filename = os.path.basename(filepath)
        if not (filename.endswith('.json') or filename.endswith('.csv')):
            return

        try:
            if filename.endswith('.json'):
                success = process_contract_file(filepath, self.conn, self.schema, 'contracts')
                if success:
                    shutil.move(filepath, os.path.join(self.processed_folder, filename))
                    logging.info(f"Processed and moved {filename} to processed")
                else:
                    shutil.move(filepath, os.path.join(self.error_folder, filename))
                    logging.error(f"Moved {filename} to errors")
            elif filename.endswith('.csv'):
                success = process_ohlcv_file(filepath, self.conn, self.schema, self.barsize_tables)
                if success:
                    shutil.move(filepath, os.path.join(self.processed_folder, filename))
                    logging.info(f"Processed and moved {filename} to processed")
                else:
                    shutil.move(filepath, os.path.join(self.error_folder, filename))
                    logging.error(f"Moved {filename} to errors")
        except Exception as e:
            logging.error(f"Unexpected error processing {filename}: {e}", exc_info=True)
            # Move to errors if not already
            try:
                shutil.move(filepath, os.path.join(self.error_folder, filename))
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

    # Set up file watcher
    event_handler = DataFileHandler(input_folder, processed_folder, error_folder, conn, config)
    observer = Observer()
    observer.schedule(event_handler, input_folder, recursive=False)

    try:
        observer.start()
        logging.info("Data upload service started successfully - monitoring input folder for .json and .csv files")
        # Process any existing files on startup
        for filename in os.listdir(input_folder):
            filepath = os.path.join(input_folder, filename)
            if os.path.isfile(filepath) and (filename.endswith('.json') or filename.endswith('.csv')):
                event_handler.on_created(type('Event', (), {'is_directory': False, 'src_path': filepath})())
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    conn.close()

if __name__ == '__main__':
    main()