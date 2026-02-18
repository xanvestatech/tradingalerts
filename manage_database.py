#!/usr/bin/env python3
"""
Database management script for trading system
Provides command-line interface for database operations
"""

import sys
import os
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import order_db

def show_stats():
    """Display database statistics"""
    print("📊 DATABASE STATISTICS")
    print("=" * 50)
    
    try:
        stats = order_db.get_database_stats()
        
        if not stats:
            print("❌ Failed to retrieve database statistics")
            return
        
        print(f"📁 Database File: {stats.get('database_file', 'N/A')}")
        print(f"💾 File Size: {stats.get('file_size_mb', 0)} MB")
        print(f"📋 Total Orders: {stats.get('total_orders', 0)}")
        print(f"💼 Total Trades: {stats.get('total_trades', 0)}")
        print(f"📈 PnL Records: {stats.get('total_pnl_records', 0)}")
        
        # Orders by status
        orders_by_status = stats.get('orders_by_status', {})
        if orders_by_status:
            print("\n📊 Orders by Status:")
            for status, count in orders_by_status.items():
                print(f"   {status}: {count}")
        
        # Date range
        date_range = stats.get('date_range', {})
        if date_range.get('earliest') and date_range.get('latest'):
            print(f"\n📅 Date Range:")
            print(f"   Earliest: {date_range['earliest']}")
            print(f"   Latest: {date_range['latest']}")
        
    except Exception as e:
        print(f"❌ Error retrieving statistics: {e}")

def clear_test_data():
    """Clear forward testing data"""
    print("🧪 CLEAR FORWARD TESTING DATA")
    print("=" * 50)
    
    # Show what will be deleted
    try:
        recent_orders = order_db.get_recent_orders(1000)
        test_orders = [o for o in recent_orders if o['status'] == 'FORWARD_TEST_SUCCESS']
        
        print(f"Found {len(test_orders)} forward testing orders to delete")
        
        if len(test_orders) == 0:
            print("✅ No forward testing data found")
            return
        
        # Show sample orders
        print("\nSample test orders:")
        for i, order in enumerate(test_orders[:5]):
            print(f"   {i+1}. {order['transaction_type']} {order['quantity']} {order['tradingsymbol']} @ ₹{order['price']} ({order['timestamp']})")
        
        if len(test_orders) > 5:
            print(f"   ... and {len(test_orders) - 5} more")
        
    except Exception as e:
        print(f"⚠️ Error checking test data: {e}")
    
    # Confirm deletion
    confirm = input(f"\n⚠️ Delete all forward testing data? (y/N): ").strip().lower()
    
    if confirm != 'y':
        print("❌ Operation cancelled")
        return
    
    try:
        result = order_db.clear_test_data()
        
        if result['success']:
            print("✅ Forward testing data cleared successfully!")
            print(f"   Test Orders: {result['deleted_records']['test_orders']}")
            print(f"   Test Trades: {result['deleted_records']['test_trades']}")
            print(f"   Total: {result['deleted_records']['total']} records deleted")
        else:
            print(f"❌ Failed to clear test data: {result['error']}")
            
    except Exception as e:
        print(f"❌ Error clearing test data: {e}")

def clear_all_data():
    """Clear all database data"""
    print("⚠️ CLEAR ALL DATABASE DATA")
    print("=" * 50)
    print("🚨 WARNING: This will permanently delete ALL data!")
    print("   • All orders (live and test)")
    print("   • All trades")
    print("   • All PnL records")
    print("   • All statistics")
    print("\n❌ THIS ACTION CANNOT BE UNDONE!")
    
    # Show current data
    try:
        stats = order_db.get_database_stats()
        total_records = stats.get('total_orders', 0) + stats.get('total_trades', 0) + stats.get('total_pnl_records', 0)
        print(f"\n📊 Current database contains {total_records} total records")
    except Exception as e:
        print(f"⚠️ Error checking current data: {e}")
    
    # Double confirmation
    print("\n🔐 SAFETY CHECK:")
    confirm1 = input("Type 'DELETE' to continue: ").strip()
    
    if confirm1 != 'DELETE':
        print("❌ Operation cancelled")
        return
    
    confirm2 = input("Type 'CONFIRM_DELETE_ALL_DATA' to proceed: ").strip()
    
    if confirm2 != 'CONFIRM_DELETE_ALL_DATA':
        print("❌ Operation cancelled - confirmation text did not match")
        return
    
    try:
        result = order_db.clear_all_data(confirm2)
        
        if result['success']:
            print("✅ All database data cleared successfully!")
            print(f"   Orders: {result['deleted_records']['orders']}")
            print(f"   Trades: {result['deleted_records']['trades']}")
            print(f"   PnL Records: {result['deleted_records']['pnl_summary']}")
            print(f"   Total: {result['deleted_records']['total']} records deleted")
        else:
            print(f"❌ Failed to clear database: {result['error']}")
            
    except Exception as e:
        print(f"❌ Error clearing database: {e}")

def backup_database():
    """Create a backup of the database"""
    print("💾 BACKUP DATABASE")
    print("=" * 50)
    
    try:
        import shutil
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"trading_orders_backup_{timestamp}.db"
        
        # Copy database file
        if os.path.exists(order_db.db_path):
            shutil.copy2(order_db.db_path, backup_filename)
            file_size = os.path.getsize(backup_filename) / (1024 * 1024)
            
            print(f"✅ Database backed up successfully!")
            print(f"   Original: {order_db.db_path}")
            print(f"   Backup: {backup_filename}")
            print(f"   Size: {file_size:.2f} MB")
        else:
            print(f"❌ Database file not found: {order_db.db_path}")
            
    except Exception as e:
        print(f"❌ Error creating backup: {e}")

def show_recent_orders(limit=10):
    """Show recent orders"""
    print(f"📋 RECENT ORDERS (Last {limit})")
    print("=" * 50)
    
    try:
        orders = order_db.get_recent_orders(limit)
        
        if not orders:
            print("No orders found")
            return
        
        print(f"{'Time':<20} {'Symbol':<15} {'Type':<4} {'Qty':<6} {'Price':<10} {'Status':<20}")
        print("-" * 80)
        
        for order in orders:
            timestamp = datetime.fromisoformat(order['timestamp']).strftime("%m/%d %H:%M:%S")
            symbol = order['tradingsymbol'][:14]
            tx_type = order['transaction_type'][:4]
            qty = str(order['quantity'])
            price = f"₹{order['price']:.2f}" if order['price'] else "Market"
            status = order['status'][:19]
            
            print(f"{timestamp:<20} {symbol:<15} {tx_type:<4} {qty:<6} {price:<10} {status:<20}")
            
    except Exception as e:
        print(f"❌ Error retrieving orders: {e}")

def show_help():
    """Show help information"""
    print("🛠️ DATABASE MANAGEMENT TOOL")
    print("=" * 50)
    print("Available commands:")
    print("  stats          - Show database statistics")
    print("  recent [N]     - Show recent N orders (default: 10)")
    print("  clear-test     - Clear forward testing data only")
    print("  clear-all      - Clear ALL database data (DANGEROUS)")
    print("  backup         - Create database backup")
    print("  help           - Show this help message")
    print("\nExamples:")
    print("  python manage_database.py stats")
    print("  python manage_database.py recent 20")
    print("  python manage_database.py clear-test")
    print("  python manage_database.py backup")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        show_help()
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == "stats":
            show_stats()
        elif command == "recent":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            show_recent_orders(limit)
        elif command == "clear-test":
            clear_test_data()
        elif command == "clear-all":
            clear_all_data()
        elif command == "backup":
            backup_database()
        elif command == "help":
            show_help()
        else:
            print(f"❌ Unknown command: {command}")
            print("Use 'python manage_database.py help' for available commands")
    
    except KeyboardInterrupt:
        print("\n\n👋 Operation cancelled by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()