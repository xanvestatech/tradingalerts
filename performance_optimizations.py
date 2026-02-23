"""
Performance optimizations for app.py to reduce slippage in dual-account trading.
These optimizations focus on reducing API calls and improving parallel processing.
"""

import asyncio
import time
from typing import Dict, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)

class PerformanceOptimizer:
    """Optimizations to reduce webhook processing latency."""
    
    def __init__(self):
        self.contract_cache = {}  # Cache contracts within request only
        # Cache hit/miss statistics (for monitoring only)
        self.cache_stats = {
            'contract_hits': 0,
            'contract_misses': 0
        }
        
    def clear_request_cache(self):
        """Clear cache at the start of each webhook request."""
        self.contract_cache.clear()
        # Note: cache_stats are NOT cleared - they accumulate for monitoring
        
    def get_cache_stats(self):
        """Get cache hit/miss statistics."""
        total_contract = self.cache_stats['contract_hits'] + self.cache_stats['contract_misses']
        
        return {
            'contract_cache': {
                'hits': self.cache_stats['contract_hits'],
                'misses': self.cache_stats['contract_misses'],
                'hit_rate': round(self.cache_stats['contract_hits'] / total_contract * 100, 1) if total_contract > 0 else 0
            }
        }

# Global optimizer instance
perf_optimizer = PerformanceOptimizer()

async def get_contracts_cached(tv_symbol: str, kite, segment: str) -> list:
    """Get contracts with request-level caching to avoid duplicate API calls."""
    cache_key = f"{tv_symbol}_{segment}"
    
    if cache_key in perf_optimizer.contract_cache:
        perf_optimizer.cache_stats['contract_hits'] += 1
        logger.debug(f"Contract cache hit: {cache_key}")
        return perf_optimizer.contract_cache[cache_key]
    
    # Cache miss - fetch from API
    perf_optimizer.cache_stats['contract_misses'] += 1
    logger.debug(f"Contract cache miss: {cache_key}")
    
    # Import here to avoid circular imports
    from orders import get_top_3_futures_from_tv_symbol
    
    contracts = get_top_3_futures_from_tv_symbol(tv_symbol, kite, segment)
    perf_optimizer.contract_cache[cache_key] = contracts
    return contracts

async def get_positions_and_holdings_direct(kite, segment: str, tradingsymbol: str) -> Tuple[int, Dict]:
    """Get positions and holdings directly from API without caching."""
    
    if segment in ["NFO", "MCX"]:
        # Only need positions for futures - with timeout handling
        try:
            # Add explicit timeout to prevent hanging
            positions = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: kite.positions()["net"]
                ),
                timeout=10.0  # 10 second timeout
            )
            existing_position = next(
                (p for p in positions if p["tradingsymbol"] == tradingsymbol and p["exchange"] == segment), 
                None
            )
            qty_held = existing_position["quantity"] if existing_position else 0
            result = (qty_held, existing_position or {})
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching positions for {segment} - API took longer than 10 seconds")
            # Return safe defaults on timeout
            result = (0, {})
        except Exception as e:
            logger.error(f"Error fetching positions for {segment}: {e}")
            # Return safe defaults on API failure
            result = (0, {})
        
    elif segment == "NSE":
        # Need both holdings and positions for NSE - fetch in parallel with timeout handling
        try:
            async def get_holdings():
                return await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, kite.holdings),
                    timeout=10.0  # 10 second timeout
                )
            
            async def get_positions():
                return await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, lambda: kite.positions()["net"]),
                    timeout=10.0  # 10 second timeout
                )
            
            # Parallel execution with timeout protection
            holdings, positions = await asyncio.gather(get_holdings(), get_positions())
            
            # Check holdings first
            existing_holdings = next((h for h in holdings if h["tradingsymbol"] == tradingsymbol), None)
            qty_held = existing_holdings["quantity"] if existing_holdings else 0
            qty_held += existing_holdings["t1_quantity"] if existing_holdings and "t1_quantity" in existing_holdings else 0

            existing_position = next(
                (p for p in positions if p["tradingsymbol"] == tradingsymbol and p["exchange"] == segment), 
                None
            )
            qty_positions = existing_position["quantity"] if existing_position else 0
            # Always add MIS positions (positive for intraday longs, negative for shorts).
            # This allows short-position detection via negative qty_held.
            qty_held += qty_positions

            result = (qty_held, existing_position or {})
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching holdings/positions for {segment} - API took longer than 10 seconds")
            # Return safe defaults on timeout
            result = (0, {})
        except Exception as e:
            logger.error(f"Error fetching holdings/positions for {segment}: {e}")
            # Return safe defaults on API failure
            result = (0, {})
    else:
        result = (0, {})
    
    return result

async def process_account_optimized(kite, account_name: str, tv_symbol: str, segment: str,
                                  action: str, price: float, quantity: int,
                                  webhook_timestamp: str = None, request_id: str = None) -> Dict[str, Any]:
    """Position-state-aware order processing using only 'buy' and 'sell' actions.

    Both actions handle all three states automatically by reading the live position:

    buy:
      qty > 0  → already long, skip
      qty < 0  → short position exists, cover it (BUY to close)
      qty == 0 → flat, open long (BUY to enter)

    sell:
      qty < 0  → already short, skip
      qty > 0  → long position exists, close it (SELL to close)
      qty == 0 → flat, open short (SELL to enter)
    """

    start_time = time.time()

    try:
        # ── Step 1: Resolve tradingsymbol, qty_held, lot_size ────────────────
        if segment in ["NFO", "MCX"]:
            contracts = await get_contracts_cached(tv_symbol, kite, segment)
            if not contracts:
                return {"status": "error", "error": f"No futures contracts found for {tv_symbol}"}

            # Scan contracts to find an existing position (non-zero qty)
            qty_held = 0
            tradingsymbol = None
            existing_position = {}
            lot_size = 1

            for contract in contracts:
                contract_qty, contract_pos = await get_positions_and_holdings_direct(
                    kite, segment, contract['tradingsymbol']
                )
                if contract_qty != 0:
                    qty_held = contract_qty
                    tradingsymbol = contract['tradingsymbol']
                    existing_position = contract_pos
                    lot_size = contract.get('lot_size', 1)
                    logger.info(f"{account_name}: Found position in {tradingsymbol}, qty={qty_held}")
                    break

            if qty_held == 0:
                # Flat — select the entry contract using rollover-aware logic.
                # Skip to the next contract if the front month expires within 7 calendar days.
                expiry = contracts[0].get('expiry')
                selected_contract = contracts[0]  # default
                if expiry:
                    from datetime import datetime, timezone
                    import pandas as pd

                    if isinstance(expiry, int):
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

                    days_left = (expiry - datetime.now().date()).days
                    if days_left <= 7 and datetime.now().date().weekday() < 5 and len(contracts) > 1:
                        selected_contract = contracts[1]

                tradingsymbol = selected_contract['tradingsymbol']
                lot_size = selected_contract.get('lot_size', 1)

            total_quantity = int(quantity * lot_size)

        else:
            # NSE stocks
            tradingsymbol = tv_symbol
            lot_size = 1
            total_quantity = quantity
            qty_held, existing_position = await get_positions_and_holdings_direct(
                kite, segment, tradingsymbol
            )

        # ── Step 2: Place order based on current position state ───────────────
        from orders import place_order

        if action == "buy":
            if qty_held > 0:
                # Already long — nothing to do
                processing_time = (time.time() - start_time) * 1000
                logger.info(f"{account_name}: Already holding {tradingsymbol} (qty={qty_held}). Skipping buy. ({processing_time:.1f}ms)")
                return {"status": "already holding, buy skipped", "quantity": qty_held, "processing_time_ms": processing_time}

            elif qty_held < 0:
                # Short position exists — cover it (BUY to close)
                cover_qty = abs(qty_held)
                # NSE shorts use MIS; NFO/MCX use NRML (default in place_order)
                product_override = "MIS" if segment == "NSE" else None
                order_id, error = await asyncio.get_event_loop().run_in_executor(
                    None, place_order, kite, tradingsymbol, action, price, segment,
                    cover_qty, webhook_timestamp, tv_symbol, request_id, product_override
                )
                processing_time = (time.time() - start_time) * 1000
                if order_id:
                    logger.info(f"{account_name}: Cover (buy) order placed for {cover_qty} units of {tradingsymbol} ({processing_time:.1f}ms)")
                    return {"status": "cover order placed", "order_id": order_id, "quantity": cover_qty, "processing_time_ms": processing_time}
                else:
                    logger.error(f"{account_name}: Cover (buy) order failed: {error} ({processing_time:.1f}ms)")
                    return {"status": "buy order failed", "error": "Order failed. Please check the server logs.", "processing_time_ms": processing_time}

            else:
                # Flat — enter long (BUY to open)
                order_id, error = await asyncio.get_event_loop().run_in_executor(
                    None, place_order, kite, tradingsymbol, action, price, segment,
                    total_quantity, webhook_timestamp, tv_symbol, request_id
                )
                processing_time = (time.time() - start_time) * 1000
                if order_id:
                    logger.info(f"{account_name}: Buy order placed for {total_quantity} units of {tradingsymbol} (lot size: {lot_size}) ({processing_time:.1f}ms)")
                    return {"status": "buy order placed", "order_id": order_id, "quantity": total_quantity, "processing_time_ms": processing_time}
                else:
                    logger.error(f"{account_name}: Buy order failed: {error} ({processing_time:.1f}ms)")
                    return {"status": "buy order failed", "error": "Order failed. Please check the server logs.", "processing_time_ms": processing_time}

        elif action == "sell":
            if qty_held < 0:
                # Already short — nothing to do
                processing_time = (time.time() - start_time) * 1000
                logger.info(f"{account_name}: Already short {tradingsymbol} (qty={qty_held}). Skipping sell. ({processing_time:.1f}ms)")
                return {"status": "already short, sell skipped", "quantity": qty_held, "processing_time_ms": processing_time}

            elif qty_held > 0:
                # Long position exists — close it (SELL to close)
                sell_qty = qty_held
                order_id, error = await asyncio.get_event_loop().run_in_executor(
                    None, place_order, kite, tradingsymbol, action, price, segment,
                    sell_qty, webhook_timestamp, tv_symbol, request_id
                )
                processing_time = (time.time() - start_time) * 1000
                if order_id:
                    logger.info(f"{account_name}: Sell order placed for {sell_qty} units of {tradingsymbol} ({processing_time:.1f}ms)")
                    return {"status": "sell order placed", "order_id": order_id, "quantity": sell_qty, "processing_time_ms": processing_time}
                else:
                    logger.error(f"{account_name}: Sell order failed: {error} ({processing_time:.1f}ms)")
                    return {"status": "sell order failed", "error": "Order failed. Please check the server logs.", "processing_time_ms": processing_time}

            else:
                # Flat — enter short (SELL to open)
                # NSE shorts need MIS product; NFO/MCX use NRML (default in place_order)
                product_override = "MIS" if segment == "NSE" else None
                order_id, error = await asyncio.get_event_loop().run_in_executor(
                    None, place_order, kite, tradingsymbol, action, price, segment,
                    total_quantity, webhook_timestamp, tv_symbol, request_id, product_override
                )
                processing_time = (time.time() - start_time) * 1000
                if order_id:
                    logger.info(f"{account_name}: Short (sell) order placed for {total_quantity} units of {tradingsymbol} (lot size: {lot_size}) ({processing_time:.1f}ms)")
                    return {"status": "short order placed", "order_id": order_id, "quantity": total_quantity, "processing_time_ms": processing_time}
                else:
                    logger.error(f"{account_name}: Short (sell) order failed: {error} ({processing_time:.1f}ms)")
                    return {"status": "sell order failed", "error": "Order failed. Please check the server logs.", "processing_time_ms": processing_time}

        processing_time = (time.time() - start_time) * 1000
        return {"status": "unknown error", "processing_time_ms": processing_time}

    except Exception as e:
        processing_time = (time.time() - start_time) * 1000
        logger.exception(f"{account_name}: Error processing order. ({processing_time:.1f}ms)")
        return {"status": "error", "error": "An internal error occurred. Please check the server logs.", "processing_time_ms": processing_time}

# Performance monitoring
class PerformanceMonitor:
    """Monitor webhook processing performance."""
    
    def __init__(self):
        self.request_times = []
        self.slow_requests = []
        
    def record_request(self, processing_time_ms: float, request_id: str):
        """Record request processing time."""
        self.request_times.append(processing_time_ms)
        
        # Keep only last 100 requests
        if len(self.request_times) > 100:
            self.request_times.pop(0)
            
        # Track slow requests (>500ms)
        if processing_time_ms > 500:
            self.slow_requests.append({
                'request_id': request_id,
                'processing_time_ms': processing_time_ms,
                'timestamp': time.time()
            })
            
        # Keep only last 20 slow requests
        if len(self.slow_requests) > 20:
            self.slow_requests.pop(0)
            
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        if not self.request_times:
            return {"message": "No requests recorded"}
            
        avg_time = sum(self.request_times) / len(self.request_times)
        max_time = max(self.request_times)
        min_time = min(self.request_times)
        
        return {
            "avg_processing_time_ms": round(avg_time, 2),
            "max_processing_time_ms": max_time,
            "min_processing_time_ms": min_time,
            "total_requests": len(self.request_times),
            "slow_requests_count": len(self.slow_requests),
            "recent_slow_requests": self.slow_requests[-5:] if self.slow_requests else []
        }

# Global performance monitor
perf_monitor = PerformanceMonitor()