from ib_insync import *
import pandas as pd
from datetime import datetime, timedelta
import time
import pytz

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

eastern = pytz.timezone('US/Eastern')

def get_mes_expiries(start_year=2019):
    months = {'H': 3, 'M': 6, 'U': 9, 'Z': 12}
    expiries = []
    for year in range(start_year, 2026):
        for code, mon in months.items():
            if year == 2019 and mon < 6: continue
            expiry = f"{year}{mon:02d}"
            nt_symbol = f"MES{code}{str(year)[-2:]}"
            expiries.append((expiry, nt_symbol))
    return expiries

for expiry, nt_symbol in get_mes_expiries(2019):
    #contract = Future('MES', expiry, 'CME', currency='USD')
    contract = Future(symbol='MES', exchange='CME', currency='USD')
    contract.includeExpired = False
    qualified = ib.qualifyContracts(contract)
    details = ib.reqContractDetails(contract)

    if not details:
        print(f"Skipping {nt_symbol} — no contract details")
        continue

    specs = details[0]
    print(f"\nDownloading {nt_symbol} ({expiry})...")

    # Set end_dt = 4:15 PM ET on expiration day
    try:
        exp_date = specs.realExpirationDate  # 'YYYYMMDD'
        exp_dt = datetime.strptime(exp_date, '%Y%m%d')
        exp_dt = exp_dt.replace(hour=16, minute=15)
        end_dt = eastern.localize(exp_dt)
    except:
        print(f"  Bad expiry date: {specs.realExpirationDate}")
        continue

    all_bars = []
    max_retries = 3

    while True:
        for attempt in range(max_retries):
            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime=end_dt,
                    durationStr='7 D',
                    barSizeSetting='1 min',
                    whatToShow='TRADES',
                    useRTH=False,
                    formatDate=1,
                    timeout=30
                )
                if bars:
                    all_bars.extend(bars)
                    print(f"  + {len(bars)} bars (to {bars[0].date})")
                    # Move back 7 days from oldest bar
                    end_dt = bars[0].date - timedelta(seconds=1)
                    time.sleep(20)
                    break
                else:
                    end_dt = None
                    break
            except Exception as e:
                print(f"  Retry {attempt+1}: {e}")
                time.sleep(30)
                if attempt == max_retries - 1:
                    end_dt = None
        if not end_dt:
            break

    if all_bars:
        df = util.df(all_bars)
        df = df.sort_values('date').drop_duplicates()
        filename = f"{nt_symbol}_1min.csv"
        df.to_csv(filename, index=False)
        print(f"SAVED {len(df):,} bars → {filename}")
    else:
        print(f"No data for {nt_symbol}")

ib.disconnect()