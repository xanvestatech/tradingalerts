import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional
import logging

# Load environment variables
load_dotenv()

API_KEY = os.getenv("KITE_API_KEY")
API_SECRET = os.getenv("KITE_API_SECRET")
ACCESS_TOKEN_PATH = os.getenv("ACCESS_TOKEN_PATH", "access_token.txt")

logger = logging.getLogger(__name__)

def zerodha_login(
    api_key: Optional[str] = None,
    api_secret: Optional[str] = None,
    request_token: Optional[str] = None,
    access_token_path: str = ACCESS_TOKEN_PATH
) -> KiteConnect:
    """
    Login to Zerodha using API key/secret and access token.
    Raises Exception if credentials are missing or invalid.
    """
    api_key = api_key or API_KEY
    api_secret = api_secret or API_SECRET
    if not api_key or not api_secret:
        logger.error("API key/secret not provided. Set env variables or pass as arguments.")
        raise Exception("API key/secret not provided. Set env variables or pass as arguments.")
    kite = KiteConnect(api_key=api_key)
    # Try loading access token
    if os.path.exists(access_token_path):
        with open(access_token_path, "r") as f:
            access_token = f.read().strip()
        kite.set_access_token(access_token)
        try:
            kite.profile()  # Validate token
            logger.info("Access token loaded and verified.")
            return kite
        except Exception as e:
            logger.warning(f"Stored access token invalid: {e}. Returning kite without setting access token.")
            return kite  # Return the Kite object without access token
    return kite  # Return without access token set



def fetch_latest_data(
    kite: KiteConnect,
    instrument_token: int,
    interval: str = '15minute',
    lookback: int = 15*4
) -> pd.DataFrame:
    """
    Fetch the latest historical data for a given instrument.
    Args:
        kite: KiteConnect API instance
        instrument_token: Instrument token (int)
        interval: Data interval (default '15minute')
        lookback: Number of candles to look back (default 60)
    Returns:
        Pandas DataFrame with historical data.
    Raises:
        Exception if fetching data fails.
    """
    try:
        to_date = datetime.now()
        from_date = to_date - timedelta(minutes=lookback*15)
        data = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval=interval,
            continuous=False,
            oi=False
        )
        df = pd.DataFrame(data)
        if not df.empty:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
        return df
    except Exception as e:
        logger.error(f"Failed to fetch latest data: {e}")
        raise

def get_instrument_token(
    kite: KiteConnect,
    tradingsymbol: str
) -> Optional[int]:
    """
    Get the instrument token for a given trading symbol.
    Args:
        kite: KiteConnect API instance
        tradingsymbol: NSE trading symbol
    Returns:
        Instrument token (int) if found, else None.
    """
    try:
        instruments = kite.instruments("NSE")
        for inst in instruments:
            if inst['tradingsymbol'] == tradingsymbol:
                return inst['instrument_token']
        logger.warning(f"Instrument token not found for {tradingsymbol}")
        return None
    except Exception as e:
        logger.error(f"Error fetching instrument token: {e}")
        return None
