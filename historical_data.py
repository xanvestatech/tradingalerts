from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import pandas as pd

from utils import zerodha_login
EXCHANGE = "MCX"
SEARCH_KEYWORD = "COPPER"   # look for instruments with this in tradingsymbol
INTERVAL = "5minute"        # 5-minute candles
DAYS = 60
kite = zerodha_login()


def find_copper_instruments(kite: KiteConnect, exchange: str="MCX", symbol_keyword: str="COPPER"):
    """
    Returns a list of instrument dicts from kite.instruments(exchange) whose tradingsymbol contains symbol_keyword.
    Each dict contains keys like: instrument_token, tradingsymbol, expiry, strike, tick_size, lot_size, exchange
    """
    # kite.instruments() returns a list of instrument dicts. You can pass exchange to limit results in many versions.
    instruments = kite.instruments(exchange)  # if this raises, try kite.instruments() and filter by 'exchange'
    matches = [inst for inst in instruments if symbol_keyword.upper() in inst["tradingsymbol"].upper() and inst.get("instrument_token")]
    # sort by expiry (nearest first) if expiry key exists (helpful when picking a specific contract)
    try:
        matches.sort(key=lambda x: x.get("expiry") or "")
    except Exception:
        pass
    return matches

def fetch_continuous_5min_last_n_days(kite: KiteConnect, instrument_token: int, days: int = 60, interval: str = "5minute"):
    """
    Fetches continuous historical 5-min candles from (now - days) to now for the given instrument_token.
    Returns a pandas DataFrame with timestamp, open, high, low, close, volume, oi (if available).
    """
    to_dt = datetime.now()
    from_dt = to_dt - timedelta(days=days)
    # kite.historical_data accepts datetimes or strings; using datetimes
    candles = kite.historical_data(instrument_token, from_dt, to_dt, interval, continuous=False, oi=True)
    # Convert to DataFrame
    df = pd.DataFrame(candles)
    # The API returns each candle as [timestamp, open, high, low, close, volume, oi] in some versions;
    # pykiteconnect typically returns dicts: {"date":..., "open":..., "high":..., "low":..., "close":..., "volume":..., "oi":...}
    # Normalize column names
    if not df.empty:
        # Try known field names mapping
        if "date" in df.columns:
            df = df.rename(columns={"date": "timestamp"})
        elif "timestamp" in df.columns:
            pass
        # Ensure timestamp is datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
    return df

def main():
     # 1) Find MCX Copper futures instruments
    matches = find_copper_instruments(kite, exchange=EXCHANGE, symbol_keyword=SEARCH_KEYWORD)
    if not matches:
        print("No MCX Copper instruments found in instruments list. Try removing 'exchange' filter or check your instruments CSV.")
        return

    # Option A: If you want continuous across expiries, pick any Copper FUT instrument_token and use continuous=True.
    # Here we pick the first FUT (nearest expiry) instrument with 'FUT' in tradingsymbol or expiry present.
    fut_candidates = [m for m in matches if "FUT" in m["tradingsymbol"].upper() ]
    chosen = fut_candidates[0] if fut_candidates else matches[0]
    token = int(chosen["instrument_token"])
    print(f"Using instrument {chosen['tradingsymbol']} (token={token}) to request continuous 5-min data.")

    # 2) Fetch continuous 5-min candles for last 60 days
    df = fetch_continuous_5min_last_n_days(kite, token, days=DAYS, interval=INTERVAL)

    # 3) Inspect / save
    if df is None or df.empty:
        print("No historical candles returned — check permissions, token, or date range.")
        return

    print(f"Fetched {len(df)} candles. Head:")
    print(df.head())
    out_csv = f"E:\\mcx_copper_5min_last_{DAYS}d.csv"

    df.to_csv(out_csv)
    print(f"Saved to {out_csv}")

if __name__ == "__main__":
    main()