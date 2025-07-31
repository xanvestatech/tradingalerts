from fastapi import FastAPI, Request, HTTPException, Query, Form, Header
from fastapi.responses import JSONResponse, HTMLResponse, PlainTextResponse
from fastapi import status
from contextlib import asynccontextmanager
from pydantic import BaseModel
import pandas as pd
import logging
import os
import asyncio
from datetime import datetime, timedelta, timezone
from orders import place_order, get_top_3_futures_from_tv_symbol
from utils import zerodha_login
from dotenv import load_dotenv
from memory_manager import memory_manager, cleanup_dataframes, force_gc
from logging_config import setup_logging
from redis_utils import get_instrument_cache, set_instrument_cache, is_duplicate
import pytz
from dateutil import parser as dtparser
import json
from typing import List, Optional, Dict, Any, Union, Callable
from fastapi.responses import FileResponse
import uuid  # For generating request IDs
import functools
import contextvars
import time  # For performance monitoring

# Setup centralized logging to prevent duplicate handlers
setup_logging()
logger = logging.getLogger(__name__)

# Request ID context
request_id = contextvars.ContextVar('request_id', default=str(uuid.uuid4()))

def log_with_request_id(level: str, message: str, **kwargs):
    """Helper function to log messages with request ID"""
    log_msg = f"[ReqID: {request_id.get()}] {message}"
    
    # Extract exc_info from kwargs to avoid conflicts
    exc_info = kwargs.pop('exc_info', False)
    extra = {'request_id': request_id.get(), **kwargs}
    
    if level.upper() == 'DEBUG':
        logger.debug(log_msg, extra=extra)
    elif level.upper() == 'INFO':
        logger.info(log_msg, extra=extra)
    elif level.upper() == 'WARNING':
        logger.warning(log_msg, extra=extra)
    elif level.upper() == 'ERROR':
        logger.error(log_msg, extra=extra, exc_info=exc_info)
    elif level.upper() == 'CRITICAL':
        logger.critical(log_msg, extra=extra, exc_info=exc_info)

def log_operation(operation_name: str):
    """Decorator to log function entry and exit with request ID"""
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            req_id = request_id.get()
            log_with_request_id('INFO', f"Starting operation: {operation_name}", operation=operation_name)
            try:
                result = await func(*args, **kwargs)
                log_with_request_id('INFO', f"Completed operation: {operation_name}", operation=operation_name)
                return result
            except Exception as e:
                log_with_request_id('ERROR', f"Operation failed: {operation_name} - {str(e)}", 
                                 operation=operation_name, error=str(e), exc_info=True)
                raise
        return async_wrapper
    return decorator

# Load environment variables
load_dotenv()

# File paths and secrets from env
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
ACCESS_TOKEN_FILE = os.getenv("ACCESS_TOKEN_PATH", "access_token.txt")
API_SECRET = os.getenv("KITE_API_SECRET")
API_KEY = os.getenv("KITE_API_KEY")

kite = zerodha_login()

class WebhookPayload(BaseModel):
    action: str
    symbol: str
    segment: str = "NSE"
    price: float = 0.0
    time: str = None
    quantity: float = 0.0

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event for FastAPI app. Starts the rollover background task for the account."""
    
    # Store background tasks to prevent memory leaks
    background_tasks = []
    
    # --- Validate Environment Variables ---
    required_vars = [
        'KITE_API_KEY', 'KITE_API_SECRET', 'ACCESS_TOKEN_PATH',
        'WEBHOOK_SECRET'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logging.critical(error_msg)
        raise RuntimeError(error_msg)

    # --- Check Account Configuration (Not Authentication) ---
    if not API_KEY or not API_SECRET:
        error_msg = "❌ Critical: Account is not properly configured. Check environment variables."
        logging.critical(error_msg)
        raise RuntimeError(error_msg)
    else:
        logging.info("✅ Account configured successfully")
        logging.info("Account will need authentication via /token endpoint")

    # --- Skip instrument cache building at startup (will be built on first webhook) ---
    logging.info("[Startup] Skipping instrument cache building - will be populated on first webhook request")
    logging.info("[Startup] Instrument cache will be built when account is authenticated and first trade occurs")

    async def rollover_check(kite, account_name):
        segments = ["NFO", "MCX"]
        while True:
            try:
                now = datetime.now()
                # Calculate the next 9:25 AM on a weekday
                next_run = now.replace(hour=9, minute=25, second=0, microsecond=0)
                if now >= next_run:
                    # If already past today's 9:25 AM, schedule for next weekday
                    next_run += timedelta(days=1)
                while next_run.weekday() >= 5:  # 5=Saturday, 6=Sunday
                    next_run += timedelta(days=1)

                # Sleep until next_run
                sleep_seconds = (next_run - now).total_seconds()
                logging.info(f"{account_name} rollover_check sleeping for {sleep_seconds/60:.2f} minutes until next weekday 9:25 AM")
                await asyncio.sleep(sleep_seconds)

                # --- Refresh instrument cache and create lookups ---
                instrument_lookups = {}
                df_cache = {}  # Store DataFrames for cleanup
                try:
                    for seg in segments:
                        try:
                            df = get_instrument_cache(seg)
                            if df is not None:
                                df_cache[seg] = df  # Store for cleanup
                                instrument_lookups[seg] = df.set_index('tradingsymbol')['name'].to_dict()
                                logging.info(f"{account_name} loaded instrument lookup for segment {seg}")
                            else:
                                logging.warning(f"{account_name} instrument cache missing for segment {seg}")
                        except Exception as e:
                            logging.error(f"{account_name} failed to load instrument cache for {seg}: {e}", exc_info=True)
                finally:
                    # Clean up DataFrames after creating lookups
                    if df_cache:
                        cleanup_dataframes(*df_cache.values())
                        df_cache.clear()

                # --- Rollover logic starts here ---
                today = datetime.now().date()
                
                # Check if account is authenticated before making API calls
                try:
                    positions = kite.positions()["net"]
                except Exception as e:
                    logging.warning(f"{account_name} rollover check skipped - account not authenticated: {e}")
                    continue  # Skip this iteration and try again next time

                # Use a set to track unique base symbols to roll over
                tracked_base_symbols = set()
                for pos in positions:
                    segment = pos.get("exchange")
                    if segment in instrument_lookups and pos.get("product") == "NRML" and  pos.get("quantity") > 0:
                        # Reliably find the base symbol using the instrument lookup
                        base_symbol = instrument_lookups[segment].get(pos["tradingsymbol"])
                        if base_symbol:
                            tracked_base_symbols.add(base_symbol)

                logging.info(f"{account_name} tracking symbols for rollover: {list(tracked_base_symbols)}")

                for base_symbol in sorted(list(tracked_base_symbols)):
                    contracts = get_top_3_futures_from_tv_symbol(base_symbol + "!", kite, 'NFO')
                    if not contracts:
                        contracts = get_top_3_futures_from_tv_symbol(base_symbol + "!", kite, 'MCX')
                    if len(contracts) < 2:
                        continue

                    current_contract = contracts[0]
                    next_contract = contracts[1]

                    expiry = current_contract['expiry']

                    if isinstance(expiry, int):
                        # Milliseconds to UTC date (safe & modern)
                        expiry = datetime.fromtimestamp(expiry / 1000, tz=timezone.utc).date()
                    elif isinstance(expiry, str):
                        try:
                            expiry = datetime.strptime(expiry, "%Y-%m-%d").date()
                        except ValueError:
                            expiry = datetime.fromisoformat(expiry).date()
                    elif isinstance(expiry, datetime):
                        expiry = expiry.date()
                    elif isinstance(expiry, pd.Timestamp):
                        expiry = expiry.date()

                    today = datetime.now().date()
                    days_left = (expiry - today).days

                    if days_left <= 7 and today.weekday() < 5:
                        current_symbol = current_contract['tradingsymbol']
                        next_symbol = next_contract['tradingsymbol']

                        existing_position = next((p for p in positions if p["tradingsymbol"] == current_symbol and p["exchange"] in ["NFO", "MCX"]), None)

                        qty_held = existing_position["quantity"] if existing_position else 0

                        if qty_held > 0:
                            segment = existing_position["exchange"]
                            #order_id, error = place_order(kite, current_symbol, "sell", 0, segment, quantity=qty_held)
                            if order_id:
                                #order_id2, error2 = place_order(kite, next_symbol, "buy", 0, segment, quantity=qty_held)
                                if order_id2:
                                    logging.info(f"✅ {account_name} Rolled over {base_symbol} from {current_symbol} to {next_symbol}")
                                else:
                                    logging.error(f"❌ {account_name} Failed to BUY roll over (BUY LEG FAILED) {base_symbol} from {current_symbol} to {next_symbol}: {error2}")
                            else:
                                logging.error(f"❌ {account_name} Failed to SELL roll over (SELL LEG FAILED) {base_symbol} from {current_symbol} to {next_symbol}: {error}")
                logging.info(f"{account_name} Rollover check completed.")
            except Exception as e:
                logging.exception(f"{account_name} Rollover scheduler failed")
                # Sleep to prevent tight error loop
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    # Start rollover check for account and store task reference
    try:
        rollover_task = asyncio.create_task(rollover_check(kite, "Account"))
        
        # Start memory monitoring task
        memory_task = asyncio.create_task(memory_manager.monitor_memory(interval_seconds=300))
        
        background_tasks.extend([rollover_task, memory_task])
        
        logging.info("Background rollover and memory monitoring tasks started successfully")
        yield
        
    finally:
        # Cleanup: Cancel all background tasks on shutdown
        logging.info("Shutting down background tasks...")
        for task in background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logging.info(f"Task {task.get_name() if hasattr(task, 'get_name') else 'unnamed'} cancelled successfully")
                except Exception as e:
                    logging.error(f"Error cancelling task: {e}")
        
        # Wait a bit for graceful shutdown
        await asyncio.sleep(0.1)
        logging.info("All background tasks cleaned up")

app = FastAPI(lifespan=lifespan)

# Note: Logging is now handled by setup_logging() called at the top
# The duplicate logging configuration has been removed to prevent memory leaks

from fastapi.responses import PlainTextResponse

@app.get("/logs", response_class=PlainTextResponse)
def get_logs(lines: int = 100):
    """Endpoint to fetch the last N lines of the log file for live monitoring."""
    log_path = "stock_scanner.log"  # Use the same log file as logging_config.py
    if not os.path.exists(log_path):
        return PlainTextResponse("Log file not found.", status_code=404)
    try:
        with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
            all_lines = f.readlines()
        # Return only the last N lines
        output = "".join(all_lines[-lines:])
        return PlainTextResponse(output, status_code=200)
    except Exception as e:
        logging.exception("Error reading log file")
        return PlainTextResponse("Error reading log file. Please check the server logs.", status_code=500)

@app.get("/performance", response_class=JSONResponse)
def get_performance_stats():
    """Endpoint to get webhook processing performance statistics."""
    try:
        from performance_optimizations import perf_monitor, perf_optimizer
        
        # Get performance stats
        perf_stats = perf_monitor.get_stats()
        
        # Get cache statistics
        cache_stats = perf_optimizer.get_cache_stats()
        
        # Combine both sets of statistics
        combined_stats = {
            **perf_stats,
            "cache_performance": cache_stats
        }
        
        return JSONResponse(content=combined_stats, status_code=200)
    except Exception as e:
        logging.exception("Error getting performance stats")
        return JSONResponse(content={"error": "Failed to get performance stats"}, status_code=500)

# account login form
from fastapi.responses import HTMLResponse

@app.get("/token", response_class=HTMLResponse)
def token_form() -> HTMLResponse:
    """Render the Zerodha login form for the account."""
    try:
        from kiteconnect import KiteConnect
        kite_temp = KiteConnect(api_key=API_KEY)
        login_url = kite_temp.login_url()
        return HTMLResponse(content=f"""
        <html>
            <body>
                <p>1. Click the link below to login to Zerodha and get your <b>request_token</b>:</p>
                <a href=\"{login_url}\" target=\"_blank\">{login_url}</a>
                <p>2. Paste the request_token below:</p>
                <form method=\"post\">
                  <input type=\"text\" name=\"token\" size=\"50\"/><br><br>
                  <input type=\"submit\" value=\"Submit\"/>
                </form>
            </body>
        </html>
        """, status_code=200)
    except Exception as e:
        logging.exception("Failed to generate login URL")
        return HTMLResponse(content=f"<p>Error: An internal error occurred. Please check the server logs.</p>", status_code=500)

@app.post("/token")
def save_and_refresh_token(token: str = Form(...)) -> HTMLResponse:
    """Save and refresh Zerodha access token for the account, and validate it."""
    global kite  # Access global kite object
    
    try:
        from kiteconnect import KiteConnect
        kite_temp = KiteConnect(api_key=API_KEY)
        access_token = kite_temp.generate_session(token, api_secret=API_SECRET)["access_token"]
        with open(ACCESS_TOKEN_FILE, "w") as f:
            f.write(access_token)
        
        # Validate token by making a simple API call
        try:
            kite_temp.set_access_token(access_token)
            profile = kite_temp.profile()  # Will raise if invalid
            
            # ✅ UPDATE GLOBAL KITE OBJECT WITH NEW ACCESS TOKEN
            kite.set_access_token(access_token)
            logging.info("✅ Global kite object updated with new access token")
            
            logging.info("Access token validated and saved successfully.")
            user_info = f"User: {profile.get('user_name', 'N/A')} ({profile.get('user_id', 'N/A')})"
            return HTMLResponse(content=f"<b>Token is valid! Login successful.</b><br>{user_info}<br>Account is now ready for trading.", status_code=200)
        except Exception as ve:
            logging.error(f"Token saved but validation failed: {ve}")
            return HTMLResponse(content=f"<b>Invalid token:</b> An internal error occurred. Please check the server logs.", status_code=400)
    except Exception as e:
        logging.exception("Failed to save and refresh token")
        return HTMLResponse(content=f"<b>Error:</b> An internal error occurred. Please check the server logs.", status_code=500)


import asyncio

async def place_order_async(kite, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, place_order, kite, *args, **kwargs)

@app.post("/webhook", response_model=None)
@log_operation("webhook_request")
async def webhook(
    payload: Request, 
    token: str = Query(...),
    x_request_id: Optional[str] = Header(None, alias='X-Request-ID')
) -> JSONResponse:

    # Step 1: Read raw body
    try:
        raw_body = await payload.body()
        body_str = raw_body.decode('utf-8')
        log_with_request_id('INFO', "Raw webhook payload received", raw=body_str)
    except Exception as e:
        log_with_request_id('ERROR', "Failed to read webhook request body", error=str(e), exc_info=True)
        return JSONResponse(content={"status": "error", "message": "Invalid request body"}, status_code=400)

    # Use Pydantic to validate and parse from JSON
    try:
        payload = WebhookPayload.model_validate_json(body_str)
        log_with_request_id('INFO', "Payload validated successfully", payload=payload.model_dump())
    except ValidationError as ve:
        log_with_request_id('ERROR', "Validation error in payload", error=str(ve), exc_info=True)
        return JSONResponse(status_code=200, content={"status": "error", "message": "Payload validation failed"})
    except Exception as e:
        log_with_request_id('ERROR', "Unexpected error in payload validation", error=str(e), exc_info=True)
        return JSONResponse(status_code=200, content={"status": "error", "message": "Unexpected error during validation"})
    
    req_id = x_request_id or str(uuid.uuid4())
    request_id.set(req_id)
    
    if token != WEBHOOK_SECRET:
        logging.warning("Unauthorized access attempt.")
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
    timestamp = payload.time
    symbol = payload.symbol
    if not timestamp:
        logging.error("No timestamp provided in payload. Cannot ensure idempotency.")
        return JSONResponse(content={"status": "error", "message": "No timestamp in payload."}, status_code=400)
    if is_duplicate(symbol, timestamp):
        logging.info(f"Duplicate alert received for symbol {symbol} at timestamp {timestamp}. Skipping processing.")
        return JSONResponse(content={"status": "duplicate", "message": "Alert already processed for this symbol and time."}, status_code=200)
    try:
        # Convert payload.time (UTC) to IST for logging
        utc_time = None
        ist_time_str = None
        if payload.time:
            try:
                utc_time = dtparser.parse(payload.time)
                if utc_time.tzinfo is None:
                    utc_time = pytz.utc.localize(utc_time)
                ist_time = utc_time.astimezone(pytz.timezone('Asia/Kolkata'))
                ist_time_str = ist_time.strftime('%Y-%m-%d %H:%M:%S %Z')
            except Exception as e:
                logging.warning(f"Could not parse payload.time: {payload.time} ({e})")
        logging.info(f"Received Webhook: {payload.model_dump_json()} | payload.time (UTC): {payload.time} | IST: {ist_time_str}")
        action = payload.action
        tv_symbol = payload.symbol
        segment = payload.segment
        price = payload.price
        time_received = payload.time
        # Extract quantity
        quantity = int(payload.quantity) if hasattr(payload, 'quantity') and payload.quantity else 1
        # For NFO/MCX, always force quantity to 1
        if segment in ["NFO", "MCX"]:
            quantity = 1

        if action not in ["buy", "sell"]:
            logging.error(f"Invalid action received: {action}")
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid action")

        # Import performance optimizations
        from performance_optimizations import process_account_optimized, perf_optimizer, perf_monitor
        
        # Clear request-level cache at start of each webhook
        perf_optimizer.clear_request_cache()
        
        webhook_start_time = time.time()

        # Process account with request ID context
        result = await process_account_optimized(kite, "account", tv_symbol, segment, action, price, quantity)
        
        log_with_request_id('INFO', 
            "Account processing completed",
            account="account",
            result=result
        )
        
        # Calculate total webhook processing time
        total_processing_time = (time.time() - webhook_start_time) * 1000
        
        # Record performance metrics
        perf_monitor.record_request(total_processing_time, req_id)
        
        response = {
            "account": result,
            "total_processing_time_ms": round(total_processing_time, 2),
            "request_id": req_id
        }
        
        # Log performance summary
        log_with_request_id('INFO', 
            f"Webhook processing completed in {total_processing_time:.1f}ms",
            total_time_ms=total_processing_time,
            account_time_ms=result.get('processing_time_ms', 0)
        )
        
        return JSONResponse(content=response, status_code=200)

    except HTTPException as he:
        raise he
    except Exception as e:
        logging.exception("Error processing webhook.")
        # Always return 200 OK to avoid TradingView retries, but log the error for review
        return JSONResponse(status_code=200, content={"status": "error", "message": "An internal error occurred. Please check the server logs."})
