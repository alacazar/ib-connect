import sqlite3
import json
import uuid
import os
from datetime import datetime

class JobQueue:
    def __init__(self, db_path=None):
        if db_path is None:
            # Try to load from config.json in current dir
            config_path = os.path.join(os.path.dirname(__file__), 'config.json')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                db_path = config.get('db_path', 'jobs.db')
            else:
                db_path = r'C:\Users\clawuser\.openclaw\shared\services\ib_download_service\jobs.db'
        self.db_path = db_path
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY,
                    job_key TEXT UNIQUE,
                    status TEXT,
                    params TEXT,
                    submitted_at TEXT,
                    completed_at TEXT,
                    result_path TEXT,
                    error_msg TEXT,
                    message TEXT
                )
            ''')
            # Add message column if not exists (for older dbs)
            try:
                conn.execute('ALTER TABLE jobs ADD COLUMN message TEXT')
            except sqlite3.OperationalError:
                pass  # Column already exists

    def submit_job(self, params):
        job_key = str(uuid.uuid4())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO jobs (job_key, status, params, submitted_at)
                VALUES (?, ?, ?, ?)
            ''', (job_key, 'pending', json.dumps(params), datetime.now().isoformat()))
        return job_key

    def get_pending_job(self):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('''
                SELECT job_key, params FROM jobs WHERE status = 'pending' ORDER BY submitted_at LIMIT 1
            ''').fetchone()
            if row:
                job_key, params = row
                # Mark as processing
                conn.execute('UPDATE jobs SET status = ? WHERE job_key = ?', ('processing', job_key))
                return job_key, json.loads(params)
        return None

    def update_status(self, job_key, status, result_path=None, error_msg=None, message=None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE jobs SET status = ?, completed_at = ?, result_path = ?, error_msg = ?, message = ?
                WHERE job_key = ?
            ''', (status, datetime.now().isoformat() if status in ['completed', 'failed'] else None, result_path, error_msg, message, job_key))

    def get_status(self, job_key):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('''
                SELECT status, result_path, error_msg, message FROM jobs WHERE job_key = ?
            ''', (job_key,)).fetchone()
            if row:
                status, result_path, error_msg, message = row
                return {
                    'status': status,
                    'result_path': result_path,
                    'error': error_msg,
                    'message': message
                }
        return {'status': 'not_found'}