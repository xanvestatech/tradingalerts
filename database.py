import sqlite3
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
import os

logger = logging.getLogger(__name__)

class OrderDatabase:
    """SQLite database manager for order logging and PnL tracking"""
    
    def __init__(self, db_path: str = "trading_orders.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize database with required tables"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Orders table - logs all orders before placement
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id TEXT,
                        tradingsymbol TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        transaction_type TEXT NOT NULL,
                        quantity INTEGER NOT NULL,
                        price REAL,
                        order_type TEXT NOT NULL,
                        product TEXT NOT NULL,
                        status TEXT DEFAULT 'PENDING',
                        timestamp DATETIME NOT NULL,
                        webhook_timestamp TEXT,
                        tv_symbol TEXT,
                        request_id TEXT,
                        error_message TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Trades table - for executed orders (filled from Zerodha API)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        order_id TEXT NOT NULL,
                        tradingsymbol TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        transaction_type TEXT NOT NULL,
                        quantity INTEGER NOT NULL,
                        price REAL NOT NULL,
                        trade_id TEXT,
                        execution_time DATETIME NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (order_id) REFERENCES orders (order_id)
                    )
                ''')
                
                # PnL summary table - calculated PnL for each symbol
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS pnl_summary (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        tradingsymbol TEXT NOT NULL,
                        exchange TEXT NOT NULL,
                        total_buy_qty INTEGER DEFAULT 0,
                        total_sell_qty INTEGER DEFAULT 0,
                        avg_buy_price REAL DEFAULT 0,
                        avg_sell_price REAL DEFAULT 0,
                        realized_pnl REAL DEFAULT 0,
                        unrealized_pnl REAL DEFAULT 0,
                        current_position INTEGER DEFAULT 0,
                        last_updated DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(tradingsymbol, exchange)
                    )
                ''')
                
                # Create indexes for better performance
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(tradingsymbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(timestamp)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(tradingsymbol)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_trades_execution_time ON trades(execution_time)')
                
                conn.commit()
                logger.info("✅ Database initialized successfully")
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize database: {e}", exc_info=True)
            raise
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}", exc_info=True)
            raise
        finally:
            if conn:
                conn.close()
    
    def log_order_attempt(self, 
                         tradingsymbol: str,
                         exchange: str,
                         transaction_type: str,
                         quantity: int,
                         price: float,
                         order_type: str,
                         product: str,
                         webhook_timestamp: str = None,
                         tv_symbol: str = None,
                         request_id: str = None) -> int:
        """Log order attempt before placing order"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    INSERT INTO orders (
                        tradingsymbol, exchange, transaction_type, quantity, 
                        price, order_type, product, timestamp, webhook_timestamp,
                        tv_symbol, request_id, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    tradingsymbol, exchange, transaction_type, quantity,
                    price, order_type, product, datetime.now(),
                    webhook_timestamp, tv_symbol, request_id, 'ATTEMPTING'
                ))
                
                order_log_id = cursor.lastrowid
                conn.commit()
                
                logger.info(f"📝 Order attempt logged: ID={order_log_id}, {transaction_type} {quantity} {tradingsymbol}")
                return order_log_id
                
        except Exception as e:
            logger.error(f"❌ Failed to log order attempt: {e}", exc_info=True)
            return None
    
    def update_order_result(self, 
                           order_log_id: int,
                           order_id: str = None,
                           status: str = 'SUCCESS',
                           error_message: str = None):
        """Update order result after placement attempt"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    UPDATE orders 
                    SET order_id = ?, status = ?, error_message = ?, updated_at = ?
                    WHERE id = ?
                ''', (order_id, status, error_message, datetime.now(), order_log_id))
                
                conn.commit()
                
                logger.info(f"📝 Order result updated: ID={order_log_id}, Status={status}, OrderID={order_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to update order result: {e}", exc_info=True)
    
    def get_recent_orders(self, limit: int = 50) -> List[Dict]:
        """Get recent orders for dashboard"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('''
                    SELECT * FROM orders 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limit,))
                
                orders = [dict(row) for row in cursor.fetchall()]
                return orders
                
        except Exception as e:
            logger.error(f"❌ Failed to get recent orders: {e}", exc_info=True)
            return []
    
    def get_orders_by_symbol(self, tradingsymbol: str, exchange: str = None) -> List[Dict]:
        """Get all orders for a specific symbol"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if exchange:
                    cursor.execute('''
                        SELECT * FROM orders 
                        WHERE tradingsymbol = ? AND exchange = ?
                        ORDER BY timestamp ASC
                    ''', (tradingsymbol, exchange))
                else:
                    cursor.execute('''
                        SELECT * FROM orders 
                        WHERE tradingsymbol = ?
                        ORDER BY timestamp ASC
                    ''', (tradingsymbol,))
                
                orders = [dict(row) for row in cursor.fetchall()]
                return orders
                
        except Exception as e:
            logger.error(f"❌ Failed to get orders by symbol: {e}", exc_info=True)
            return []
    
    def calculate_symbol_pnl(self, tradingsymbol: str, exchange: str) -> Dict[str, Any]:
        """Calculate PnL for a specific symbol using FIFO method"""
        try:
            orders = self.get_orders_by_symbol(tradingsymbol, exchange)
            
            # Filter successful orders (including forward test orders for analysis)
            successful_orders = [o for o in orders if o['status'] in ['SUCCESS', 'FORWARD_TEST_SUCCESS'] and o['order_id']]
            
            if not successful_orders:
                return {
                    'tradingsymbol': tradingsymbol,
                    'exchange': exchange,
                    'total_buy_qty': 0,
                    'total_sell_qty': 0,
                    'current_position': 0,
                    'realized_pnl': 0.0,
                    'avg_buy_price': 0.0,
                    'avg_sell_price': 0.0,
                    'trades': []
                }
            
            # Separate buy and sell orders
            buy_orders = [o for o in successful_orders if o['transaction_type'] == 'BUY']
            sell_orders = [o for o in successful_orders if o['transaction_type'] == 'SELL']
            
            # Calculate totals
            total_buy_qty = sum(o['quantity'] for o in buy_orders)
            total_sell_qty = sum(o['quantity'] for o in sell_orders)
            current_position = total_buy_qty - total_sell_qty
            
            # Calculate average prices
            avg_buy_price = 0.0
            avg_sell_price = 0.0
            
            if buy_orders:
                total_buy_value = sum(o['quantity'] * (o['price'] or 0) for o in buy_orders)
                avg_buy_price = total_buy_value / total_buy_qty if total_buy_qty > 0 else 0
            
            if sell_orders:
                total_sell_value = sum(o['quantity'] * (o['price'] or 0) for o in sell_orders)
                avg_sell_price = total_sell_value / total_sell_qty if total_sell_qty > 0 else 0
            
            # Calculate realized PnL using FIFO
            realized_pnl = self._calculate_fifo_pnl(buy_orders, sell_orders)
            
            return {
                'tradingsymbol': tradingsymbol,
                'exchange': exchange,
                'total_buy_qty': total_buy_qty,
                'total_sell_qty': total_sell_qty,
                'current_position': current_position,
                'realized_pnl': realized_pnl,
                'avg_buy_price': avg_buy_price,
                'avg_sell_price': avg_sell_price,
                'buy_orders': buy_orders,
                'sell_orders': sell_orders
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate PnL for {tradingsymbol}: {e}", exc_info=True)
            return {}
    
    def _calculate_fifo_pnl(self, buy_orders: List[Dict], sell_orders: List[Dict]) -> float:
        """Calculate realized PnL using FIFO (First In, First Out) method"""
        if not buy_orders or not sell_orders:
            return 0.0
        
        # Sort orders by timestamp
        buy_queue = sorted(buy_orders, key=lambda x: x['timestamp'])
        sell_queue = sorted(sell_orders, key=lambda x: x['timestamp'])
        
        realized_pnl = 0.0
        buy_idx = 0
        sell_idx = 0
        remaining_buy_qty = buy_queue[0]['quantity'] if buy_queue else 0
        
        while buy_idx < len(buy_queue) and sell_idx < len(sell_queue):
            sell_order = sell_queue[sell_idx]
            buy_order = buy_queue[buy_idx]
            
            # Determine quantity to match
            match_qty = min(remaining_buy_qty, sell_order['quantity'])
            
            # Calculate PnL for this match
            buy_price = buy_order['price'] or 0
            sell_price = sell_order['price'] or 0
            pnl = (sell_price - buy_price) * match_qty
            realized_pnl += pnl
            
            # Update remaining quantities
            remaining_buy_qty -= match_qty
            sell_order['quantity'] -= match_qty
            
            # Move to next orders if current ones are exhausted
            if remaining_buy_qty == 0:
                buy_idx += 1
                if buy_idx < len(buy_queue):
                    remaining_buy_qty = buy_queue[buy_idx]['quantity']
            
            if sell_order['quantity'] == 0:
                sell_idx += 1
        
        return realized_pnl
    
    def get_portfolio_summary(self) -> Dict[str, Any]:
        """Get overall portfolio summary"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get unique symbols
                cursor.execute('''
                    SELECT DISTINCT tradingsymbol, exchange 
                    FROM orders 
                    WHERE status IN ('SUCCESS', 'FORWARD_TEST_SUCCESS') AND order_id IS NOT NULL
                ''')
                
                symbols = cursor.fetchall()
                
                portfolio = {
                    'total_symbols': len(symbols),
                    'total_realized_pnl': 0.0,
                    'total_positions': 0,
                    'symbols': []
                }
                
                for symbol_row in symbols:
                    symbol = symbol_row[0]
                    exchange = symbol_row[1]
                    
                    pnl_data = self.calculate_symbol_pnl(symbol, exchange)
                    if pnl_data:
                        portfolio['symbols'].append(pnl_data)
                        portfolio['total_realized_pnl'] += pnl_data.get('realized_pnl', 0)
                        if pnl_data.get('current_position', 0) != 0:
                            portfolio['total_positions'] += 1
                
                return portfolio
                
        except Exception as e:
            logger.error(f"❌ Failed to get portfolio summary: {e}", exc_info=True)
            return {}
    
    def clear_all_data(self, confirm_token: str = None) -> Dict[str, Any]:
        """
        Clear all data from the database tables
        
        Args:
            confirm_token: Safety token to prevent accidental deletion
            
        Returns:
            Dictionary with operation results
        """
        if confirm_token != "CONFIRM_DELETE_ALL_DATA":
            return {
                "success": False,
                "error": "Invalid confirmation token. Use 'CONFIRM_DELETE_ALL_DATA' to confirm deletion.",
                "deleted_records": 0
            }
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Count records before deletion
                cursor.execute("SELECT COUNT(*) FROM orders")
                orders_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM trades")
                trades_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM pnl_summary")
                pnl_count = cursor.fetchone()[0]
                
                total_records = orders_count + trades_count + pnl_count
                
                # Delete all data
                cursor.execute("DELETE FROM trades")
                cursor.execute("DELETE FROM pnl_summary")
                cursor.execute("DELETE FROM orders")
                
                # Reset auto-increment counters
                cursor.execute("DELETE FROM sqlite_sequence WHERE name IN ('orders', 'trades', 'pnl_summary')")
                
                conn.commit()
                
                logger.warning(f"🗑️ Database cleared: {total_records} records deleted (Orders: {orders_count}, Trades: {trades_count}, PnL: {pnl_count})")
                
                return {
                    "success": True,
                    "message": "All data cleared successfully",
                    "deleted_records": {
                        "orders": orders_count,
                        "trades": trades_count,
                        "pnl_summary": pnl_count,
                        "total": total_records
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to clear database: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "deleted_records": 0
            }
    
    def clear_test_data(self) -> Dict[str, Any]:
        """
        Clear only forward testing data (orders with FORWARD_TEST_SUCCESS status)
        
        Returns:
            Dictionary with operation results
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Count test records before deletion
                cursor.execute("SELECT COUNT(*) FROM orders WHERE status = 'FORWARD_TEST_SUCCESS'")
                test_orders_count = cursor.fetchone()[0]
                
                cursor.execute("""
                    SELECT COUNT(*) FROM trades 
                    WHERE order_id IN (
                        SELECT order_id FROM orders WHERE status = 'FORWARD_TEST_SUCCESS'
                    )
                """)
                test_trades_count = cursor.fetchone()[0]
                
                # Delete test trades first (foreign key constraint)
                cursor.execute("""
                    DELETE FROM trades 
                    WHERE order_id IN (
                        SELECT order_id FROM orders WHERE status = 'FORWARD_TEST_SUCCESS'
                    )
                """)
                
                # Delete test orders
                cursor.execute("DELETE FROM orders WHERE status = 'FORWARD_TEST_SUCCESS'")
                
                # Clean up PnL summary for symbols that no longer have any orders
                cursor.execute("""
                    DELETE FROM pnl_summary 
                    WHERE (tradingsymbol, exchange) NOT IN (
                        SELECT DISTINCT tradingsymbol, exchange FROM orders 
                        WHERE status IN ('SUCCESS', 'FORWARD_TEST_SUCCESS') AND order_id IS NOT NULL
                    )
                """)
                
                conn.commit()
                
                total_deleted = test_orders_count + test_trades_count
                
                logger.info(f"🧪 Test data cleared: {total_deleted} records deleted (Orders: {test_orders_count}, Trades: {test_trades_count})")
                
                return {
                    "success": True,
                    "message": "Forward testing data cleared successfully",
                    "deleted_records": {
                        "test_orders": test_orders_count,
                        "test_trades": test_trades_count,
                        "total": total_deleted
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to clear test data: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "deleted_records": 0
            }
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Count records by status
                cursor.execute("""
                    SELECT status, COUNT(*) as count 
                    FROM orders 
                    GROUP BY status
                """)
                status_counts = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Total counts
                cursor.execute("SELECT COUNT(*) FROM orders")
                total_orders = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM trades")
                total_trades = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM pnl_summary")
                total_pnl_records = cursor.fetchone()[0]
                
                # Date range
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM orders")
                date_range = cursor.fetchone()
                
                return {
                    "total_orders": total_orders,
                    "total_trades": total_trades,
                    "total_pnl_records": total_pnl_records,
                    "orders_by_status": status_counts,
                    "date_range": {
                        "earliest": date_range[0],
                        "latest": date_range[1]
                    },
                    "database_file": self.db_path,
                    "file_size_mb": round(os.path.getsize(self.db_path) / (1024 * 1024), 2) if os.path.exists(self.db_path) else 0
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to get database stats: {e}", exc_info=True)
            return {}

# Global database instance
order_db = OrderDatabase()