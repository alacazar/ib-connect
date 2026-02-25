import sqlite3
import json
import uuid
from datetime import datetime

class JobQueue:
    def __init__(self, db_path='jobs.db'):
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
                    error_msg TEXT
                )
            ''')

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

    def update_status(self, job_key, status, result_path=None, error_msg=None):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE jobs SET status = ?, completed_at = ?, result_path = ?, error_msg = ?
                WHERE job_key = ?
            ''', (status, datetime.now().isoformat() if status in ['completed', 'failed'] else None, result_path, error_msg, job_key))

    def get_status(self, job_key):
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('''
                SELECT status, result_path, error_msg FROM jobs WHERE job_key = ?
            ''', (job_key,)).fetchone()
            if row:
                status, result_path, error_msg = row
                return {
                    'status': status,
                    'result_path': result_path,
                    'error': error_msg
                }
        return {'status': 'not_found'}