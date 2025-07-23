"""
Memory management fixes for app.py
This file contains the specific fixes that need to be applied to app.py
"""


def build_instrument_cache_with_cleanup():
    """
    Replace the cache building section (lines ~140-165) with this memory-safe version:
    """
    segments = ["NFO", "MCX"]
    for seg in segments:
        df = None
        try:
            # Check cache first
            if get_instrument_cache(seg) is not None:
                logging.info(f"[Startup] Instrument cache for segment {seg} already exists.")
                continue
                
            # Try with account
            try:
                logging.info(f"[Startup] Building instrument cache for {seg}...")
                instruments = kite.instruments(exchange=seg)
                df = pd.DataFrame(instruments)
                set_instrument_cache(seg, df)
                logging.info(f"[Startup] Successfully built instrument cache for {seg}")
                continue
            except Exception as e:
                logging.error(f"[Startup] Failed to build instrument cache for {seg}: {str(e)}")
                
        except Exception as e:
            logging.error(f"[Startup] Unexpected error processing segment {seg}: {e}", exc_info=True)
        finally:
            # Always cleanup the DataFrame
            if df is not None:
                cleanup_dataframes(df)
                df = None
            # Force garbage collection after each segment
            force_gc()

def rollover_check_with_memory_management():
    """
    Replace the rollover_check function with this memory-safe version:
    """
    async def rollover_check(kite, account_name):
        segments = ["NFO", "MCX"]
        while True:
            instrument_lookups = {}
            df_cache = {}  # Store DataFrames for cleanup
            
            try:
                now = datetime.now()
                # Calculate the next 9:25 AM on a weekday
                next_run = now.replace(hour=9, minute=25, second=0, microsecond=0)
                if now >= next_run:
                    next_run += timedelta(days=1)
                while next_run.weekday() >= 5:
                    next_run += timedelta(days=1)

                sleep_seconds = (next_run - now).total_seconds()
                logging.info(f"{account_name} rollover_check sleeping for {sleep_seconds/60:.2f} minutes until next weekday 9:25 AM")
                await asyncio.sleep(sleep_seconds)

                # --- Refresh instrument cache and create lookups ---
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

                # --- Rollover logic (same as before) ---
                today = datetime.now().date()
                positions = kite.positions()["net"]

                tracked_base_symbols = set()
                for pos in positions:
                    segment = pos.get("exchange")
                    if segment in instrument_lookups and pos.get("product") == "NRML" and pos.get("quantity") > 0:
                        base_symbol = instrument_lookups[segment].get(pos["tradingsymbol"])
                        if base_symbol:
                            tracked_base_symbols.add(base_symbol)

                logging.info(f"{account_name} tracking symbols for rollover: {list(tracked_base_symbols)}")

                # Process rollover logic (same as original)...
                
                logging.info(f"{account_name} Rollover check completed.")
                
            except Exception as e:
                logging.exception(f"{account_name} Rollover scheduler failed")
                await asyncio.sleep(60)
            finally:
                # Clean up all DataFrames used in this iteration
                if df_cache:
                    cleanup_dataframes(*df_cache.values())
                    df_cache.clear()
                
                # Clear instrument lookups
                instrument_lookups.clear()
                
                # Force garbage collection
                force_gc()

def add_memory_monitoring_to_lifespan():
    """
    Add this to the lifespan function after starting rollover tasks:
    """
    # Start memory monitoring task
    memory_task = asyncio.create_task(memory_manager.monitor_memory(interval_seconds=300))
    background_tasks.append(memory_task)
    
    logging.info("Memory monitoring task started")

def add_missing_import():
    """
    Add this import at the top of app.py:
    """
    from redis_utils import get_instrument_cache, set_instrument_cache, is_duplicate

def add_context_cleanup():
    """
    Add periodic context cleanup to prevent accumulation:
    """
    async def cleanup_contexts():
        """Periodic cleanup of context variables"""
        while True:
            try:
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
                # Force garbage collection to clean up old contexts
                force_gc()
                
                logging.info("Periodic context cleanup completed")
                
            except Exception as e:
                logging.error(f"Context cleanup error: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes before retry

