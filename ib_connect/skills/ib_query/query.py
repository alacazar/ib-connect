#!/usr/bin/env python3
"""
IB Query Module
"""

import logging
import sys
from ib_insync import IB, Contract
from ib_connect.shared.ib_connection import IBConnection

async def query_ib(args_dict):
    """
    Query IB for contract details. Args as dict.
    Returns list of contracts or raises exception.
    """
    # Suppress ib_insync ambiguous contract warnings
    logging.getLogger('ib_insync').setLevel(logging.ERROR)
    
    # Convert dict to namespace
    class Args:
        pass
    args = Args()
    for k, v in args_dict.items():
        setattr(args, k, v)
    
    # Set defaults if not provided
    if not hasattr(args, 'exchange') or not args.exchange:
        args.exchange = 'SMART'
    if not hasattr(args, 'currency') or not args.currency:
        args.currency = 'USD'
    if not hasattr(args, 'host') or not args.host:
        args.host = '127.0.0.1'
    if not hasattr(args, 'port'):
        args.port = 7497
    if not hasattr(args, 'client_id'):
        args.client_id = 1
    if not hasattr(args, 'timeout'):
        args.timeout = 4.0
    args.readonly = True  # Default to readonly
    args.verbose = False
    args.output_format = 'json'
    
    try:
        ib = IB()
        await ib.connectAsync(host=args.host, port=args.port, clientId=args.client_id, timeout=args.timeout, readonly=args.readonly)
        if getattr(args, 'conid', None) and not getattr(args, 'symbol', ''):
            contract = Contract(conId=args.conid, secType=args.sec_type, exchange=args.exchange, currency=args.currency)
        else:
            contract = Contract(symbol=args.symbol, secType=args.sec_type, exchange=args.exchange, currency=args.currency)
            if getattr(args, 'conid', None):
                contract.conId = args.conid
        if getattr(args, 'expiry', None):
            contract.lastTradeDateOrContractMonth = args.expiry
        if getattr(args, 'strike', None) is not None:
            contract.strike = args.strike
            if getattr(args, 'right', None):
                contract.right = 'P' if args.right == 'PUT' else 'C'
            if getattr(args, 'include_expired', False):
                contract.includeExpired = True
            
            if args.verbose:
                print(f"Qualifying contract: {contract}", file=sys.stderr)
            
            await ib.qualifyContractsAsync(contract)  # Qualify in place; proceed regardless
            
            if args.verbose:
                print(f"Querying details for: {contract}", file=sys.stderr)
            
            details = await ib.reqContractDetailsAsync(contract)
            if not details:
                return {"error": "No contract details found"}
            else:
                results = []
                for detail in details:
                    min_tick = detail.minTick or 0.01
                    multiplier = float(detail.contract.multiplier or 1)
                    tick_value = min_tick * multiplier
                    exp = detail.contract.lastTradeDateOrContractMonth
                    if exp and len(str(exp)) == 8:
                        exp = f"{exp[:4]}-{exp[4:6]}-{exp[6:]}"
                    
                    raw = {
                        "conid": detail.contract.conId,
                        "symbol": detail.contract.symbol,
                        "local_symbol": detail.contract.localSymbol,
                        "exchange": detail.contract.exchange,
                        "currency": detail.contract.currency,
                        "sec_type": detail.contract.secType,
                        "long_name": detail.longName,
                        "industry": detail.industry,
                        "category": detail.category,
                        "sub_category": detail.subcategory,
                        "min_tick": min_tick,
                        "tick_value": tick_value,
                        "contract_month": detail.contractMonth,
                        "expiration_date": exp,
                        "under_conid": detail.underConId,
                        "strike": detail.contract.strike or '',
                        "right": detail.contract.right or '',
                        "multiplier": multiplier,
                        "time_zone_id": detail.timeZoneId or 'US/Eastern',
                    }
                    # Keep all fields, including None
                    filtered = raw
                    results.append(filtered)
                return results if len(results) > 1 else results[0]
    
    except Exception as e:
        raise Exception(f"Query Error: {e}")
    finally:
        ib.disconnect()