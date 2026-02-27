#!/usr/bin/env python3
"""
IB Download Module
"""

from .job_queue import JobQueue

def submit_download_job(params_dict):
    """
    Submit a download job. Params as dict.
    Returns job_key.
    """
    queue = JobQueue()
    
    # Convert dict to namespace
    class Args:
        pass
    args = Args()
    for k, v in params_dict.items():
        setattr(args, k, v)
    
    # Set defaults
    if not hasattr(args, 'show') or not args.show:
        args.show = 'TRADES'
    if not hasattr(args, 'host') or not args.host:
        args.host = '127.0.0.1'
    if not hasattr(args, 'port'):
        args.port = 7497
    if not hasattr(args, 'client_id'):
        args.client_id = 1
    if not hasattr(args, 'timeout'):
        args.timeout = 4.0
    if not hasattr(args, 'max_retries'):
        args.max_retries = 3
    if not hasattr(args, 'use_rth'):
        args.use_rth = False
    if not hasattr(args, 'format'):
        args.format = 'ib'
    
    return submit_single_job(queue, args)

def submit_single_job(queue, args):
    params = {
        'conid': args.conid,
        'start': args.start,
        'end': args.end,
        'bar_size': args.bar_size,
        'show': args.show,
        'host': args.host,
        'port': args.port,
        'client_id': args.client_id,
        'timeout': args.timeout,
        'max_retries': args.max_retries,
        'use_rth': args.use_rth,
        'format': args.format,
        'agent': args.agent,
        'msg': args.msg
    }
    job_key = queue.submit_job(params)
    return job_key

def get_job_status(job_key):
    """
    Get status of a job.
    Returns dict.
    """
    queue = JobQueue()
    return queue.get_status(job_key)