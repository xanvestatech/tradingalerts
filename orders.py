import logging
import os
from kiteconnect import KiteConnect
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import random

from redis_utils import get_instrument_cache, set_instrument_cache
from memory_manager import cleanup_dataframes
from database import order_db

logger = logging.getLogger(__name__)

# Forward testing configuration
FORWARD_TESTING_MODE = os.getenv("FORWARD_TESTING_MODE", "false").lower() == "true"


def get_top_3_futures_from_tv_symbol(tv_symbol: str, kite: KiteConnect, exchange: str = "NFO") -> List[Dict[str, Any]]:
    """
    Retrieve the top 3 nearest expiry futures contracts for a given symbol.
    Uses a Redis cache for the instrument list.
    """
    symbol = tv_symbol[:-1] if tv_symbol.endswith("!") else tv_symbol
    if symbol.endswith(("1", "2", "3")):
        symbol = symbol[:-1]
    
    try:
        df = get_instrument_cache(exchange)

        if df is None:
            logger.info(f"Instrument cache miss for {exchange}. Fetching from API.")
            instruments = kite.instruments(exchange=exchange)
            df = pd.DataFrame(instruments)
            set_instrument_cache(exchange, df)
            # Note: df will be cleaned up in the finally block below

        fut_df = df[(df['instrument_type'] == 'FUT') & (df['name'] == symbol.upper())]
        
        if fut_df.empty:
            logger.warning(f"No futures contracts found for {symbol} on {exchange}")
            cleanup_dataframes(fut_df)
            return []

        fut_df_sorted = fut_df.sort_values('expiry').head(3)
        
        contracts = []
        try:
            for _, row in fut_df_sorted.iterrows():
                contracts.append({
                    "tradingsymbol": row["tradingsymbol"],
                    "expiry": row["expiry"],
                    "lot_size": row["lot_size"]
                })
            return contracts
        finally:
            # Clean up DataFrames to prevent memory leaks
            cleanup_dataframes(fut_df, fut_df_sorted)
    except Exception as e:
        logger.error(f"Error fetching futures contracts: {e}", exc_info=True)
        return []
    finally:
        # Clean up the main DataFrame if it exists
        if 'df' in locals() and df is not None:
            cleanup_dataframes(df)


def check_existing_pending_orders(
    kite: KiteConnect,
    tradingsymbol: str,
    action: str,
    segment: str,
    quantity: int
) -> Tuple[bool, Optional[str]]:
    """
    Check if there are existing pending orders for the same symbol and action.
    
    Args:
        kite: Authenticated KiteConnect instance
        tradingsymbol: The trading symbol to check
        action: 'buy' or 'sell'
        segment: Exchange segment
        quantity: Order quantity
        
    Returns:
        Tuple of (has_duplicate, order_details)
        - has_duplicate: True if duplicate pending order exists
        - order_details: Details of the existing order if found
    """
    try:
        # Get all pending orders
        orders = kite.orders()
        
        # Define pending statuses (orders that are not yet executed or cancelled)
        pending_statuses = [
            "OPEN",           # Order is open/pending
            "TRIGGER PENDING", # Stop-loss order waiting for trigger
            "PENDING",        # General pending status
            "AMO REQ RECEIVED",
            "MODIFY PENDING", # Order modification pending
            "CANCEL PENDING"  # Order cancellation pending (still active until cancelled)
        ]
        
        # Convert action to transaction type for comparison
        target_transaction_type = "BUY" if action.lower() == "buy" else "SELL"
        
        # Check for duplicate orders
        for order in orders:
            # Check if this order matches our criteria
            if (order.get("tradingsymbol") == tradingsymbol and
                order.get("exchange") == segment and
                order.get("transaction_type") == target_transaction_type and
                order.get("status") in pending_statuses):
                
                # Found a duplicate pending order
                order_details = (
                    f"Order ID: {order.get('order_id')}, "
                    f"Type: {order.get('order_type')}, "
                    f"Status: {order.get('status')}, "
                    f"Quantity: {order.get('quantity')}, "
                    f"Price: {order.get('price', 'Market')}"
                )
                
                logger.warning(
                    f"🔄 Duplicate pending order found for {tradingsymbol} {action.upper()}: {order_details}"
                )
                
                return True, order_details
        
        # No duplicate found
        logger.debug(f"✅ No duplicate pending orders found for {tradingsymbol} {action.upper()}")
        return False, None
        
    except Exception as e:
        logger.error(f"❌ Error checking existing orders for {tradingsymbol}: {e}", exc_info=True)
        # On error, allow the order to proceed (fail-safe approach)
        return False, f"Error checking orders: {str(e)}"


def place_order(
    kite: KiteConnect,
    tradingsymbol: str,
    action: str,
    price: float,
    segment: str,
    quantity: int = 1,
    webhook_timestamp: str = None,
    tv_symbol: str = None,
    request_id: str = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Place a MARKET order via the KiteConnect API with duplicate order checking and market protection fallback.
    
    Supports forward testing mode where orders are logged but not actually placed.

    Features:
    - Order logging: Logs all order attempts to SQLite database before placement
    - Forward testing: Set FORWARD_TESTING_MODE=true to simulate orders without placing them
    - Duplicate order prevention: Checks for existing pending orders (only in live mode)
    - Market protection fallback: Automatically retries with protection for illiquid ETFs
    - Comprehensive error handling and logging

    Args:
        kite (KiteConnect): Authenticated KiteConnect instance.
        tradingsymbol (str): The trading symbol for the order.
        action (str): 'buy' or 'sell'.
        price (float): The price for the order (unused for market orders).
        segment (str): The exchange segment (e.g., 'NSE', 'NFO').
        quantity (int): The quantity to trade.
        webhook_timestamp (str): Original webhook timestamp for tracking.
        tv_symbol (str): TradingView symbol for reference.
        request_id (str): Request ID for tracking.

    Returns:
        A tuple containing the order ID and None on success,
        or (None, error_message) on failure.
        
    Forward Testing Mode:
        - Set environment variable FORWARD_TESTING_MODE=true
        - Orders will be logged to database with simulated order IDs
        - No actual orders will be placed to Zerodha
        - Perfect for strategy backtesting and validation
        
    Fallback Logic (Live Mode Only):
        1. Attempts regular MARKET order first
        2. If "illiquid ETF" error occurs, retries with market protection
        3. Returns appropriate success/error response
    """
    order_log_id = None
    
    try:
        # 🚀 STEP 1: Determine order parameters
        exchange = segment
        if segment == "NFO":
            product_type = "NRML"  # Use string for forward testing compatibility
        elif segment == "NSE":
            product_type = "CNC"
        else:
            product_type = "NRML"

        transaction_type = "BUY" if action == "buy" else "SELL"
        
        # 📝 STEP 2: Log order attempt to database BEFORE any processing
        try:
            order_log_id = order_db.log_order_attempt(
                tradingsymbol=tradingsymbol,
                exchange=exchange,
                transaction_type=transaction_type,
                quantity=quantity,
                price=price,
                order_type="MARKET",
                product=product_type,
                webhook_timestamp=webhook_timestamp,
                tv_symbol=tv_symbol,
                request_id=request_id
            )
            logger.info(f"📝 Order logged to database with ID: {order_log_id}")
        except Exception as db_error:
            logger.error(f"❌ Failed to log order to database: {db_error}")
            # Continue with order processing even if logging fails
        
        # 🧪 FORWARD TESTING MODE
        if FORWARD_TESTING_MODE:
            logger.info(f"🧪 FORWARD TESTING MODE: Simulating {action.upper()} order for {quantity} units of {tradingsymbol}")
            
            # Generate a simulated order ID for testing
            simulated_order_id = f"SIM_{random.randint(100000, 999999)}"
            
            # Update database with simulated successful order
            if order_log_id:
                order_db.update_order_result(order_log_id, simulated_order_id, 'FORWARD_TEST_SUCCESS')
            
            logger.info(f"✅ FORWARD TEST: {action.upper()} order simulated for {tradingsymbol}. Simulated Order ID: {simulated_order_id}")
            return (simulated_order_id, None)
        
        # 🔍 STEP 3: Check for existing pending orders (LIVE MODE ONLY)
        has_duplicate, order_details = check_existing_pending_orders(
            kite, tradingsymbol, action, segment, quantity
        )
        
        if has_duplicate:
            error_msg = f"Duplicate order prevented: Similar pending order already exists. {order_details}"
            logger.warning(f"🚫 {error_msg}")
            
            # Update database with duplicate prevention
            if order_log_id:
                order_db.update_order_result(order_log_id, None, 'DUPLICATE_PREVENTED', error_msg)
            
            return (None, error_msg)
        
        # 🎯 STEP 4: Prepare order parameters for live placement
        order_params = {
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "transaction_type": kite.TRANSACTION_TYPE_BUY if action == "buy" else kite.TRANSACTION_TYPE_SELL,
            "quantity": quantity,
            "order_type": kite.ORDER_TYPE_MARKET,
            "product": kite.PRODUCT_NRML if segment == "NFO" else kite.PRODUCT_CNC,
            "variety": kite.VARIETY_REGULAR
        }
        
        # 📝 Log order attempt
        logger.info(f"📤 Placing {action.upper()} order for {quantity} units of {tradingsymbol} on {segment}")
        
        try:
            # 🎯 ATTEMPT 1: Try placing regular MARKET order
            order_id = kite.place_order(**order_params)
            #order_id=1
            logger.info(f"✅ {action.upper()} order placed successfully for {tradingsymbol}. Order ID: {order_id}")
            
            # Update database with successful order
            if order_log_id:
                order_db.update_order_result(order_log_id, order_id, 'SUCCESS')
            
            return (order_id, None)
            
        except Exception as market_order_error:
            error_message = str(market_order_error).lower()
            
            # 🛡️ CHECK FOR ILLIQUID ETF ERROR
            if ("market orders are blocked for illiquid etfs" in error_message or 
                "market protection" in error_message or
                "illiquid" in error_message):
                
                logger.warning(f"⚠️ Market order blocked for illiquid ETF {tradingsymbol}: {market_order_error}")
                logger.info(f"🔄 Retrying with market protection enabled for {tradingsymbol}")
                
                try:                    
                    ltp = price
                    
                    buffer_multiplier = 0.005  # 0.5% buffer
                    if action.upper() == "BUY":
                        protected_price = ltp * (1 + buffer_multiplier)
                    elif action.upper() == "SELL":
                        protected_price = ltp * (1 - buffer_multiplier)
                    
                    # 🛡️ ATTEMPT 2: Retry with market protection enabled
                    order_params_with_protection = order_params.copy()
                    
                    # Add market protection parameters
                    order_params_with_protection.update({
                        "order_type": kite.ORDER_TYPE_LIMIT,     # Instead of MARKET
                        "price": protected_price,                # A price derived from LTP + buffer
                        "validity": kite.VALIDITY_DAY,  # Ensure day validity
                        "disclosed_quantity": 0,       # No disclosed quantity
                        "tag": "market_protection"     # Tag to identify protected orders
                    })
                    
                    logger.info(f"🛡️ Placing MARKET order with protection for {tradingsymbol}")
                    
                    protected_order_id = kite.place_order(**order_params_with_protection)
                    
                    logger.info(f"✅ {action.upper()} order with market protection placed successfully for {tradingsymbol}. Order ID: {protected_order_id}")
                    logger.info(f"🛡️ Market protection applied to prevent excessive slippage")
                    
                    # Update database with successful protected order
                    if order_log_id:
                        order_db.update_order_result(order_log_id, protected_order_id, 'SUCCESS')
                    
                    return (protected_order_id, None)
                    
                except Exception as protection_error:
                    # If market protection also fails, log both errors
                    logger.error(f"❌ Market protection order also failed for {tradingsymbol}: {protection_error}")
                    logger.error(f"❌ Original error: {market_order_error}")
                    
                    combined_error = (
                        f"Both regular and protected market orders failed. "
                        f"Original: {market_order_error}. "
                        f"Protected: {protection_error}"
                    )
                    
                    # Update database with failure
                    if order_log_id:
                        order_db.update_order_result(order_log_id, None, 'FAILED', combined_error)
                    
                    return (None, combined_error)
            else:
                # Re-raise the original exception if it's not related to illiquid ETFs
                # Update database with failure
                if order_log_id:
                    order_db.update_order_result(order_log_id, None, 'FAILED', str(market_order_error))
                
                raise market_order_error
        
    except Exception as e:
        logger.error(f"❌ Error placing order for {tradingsymbol}: {e}", exc_info=True)
        
        # Update database with failure
        if order_log_id:
            order_db.update_order_result(order_log_id, None, 'FAILED', str(e))
        
        return (None, str(e))
