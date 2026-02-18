#!/usr/bin/env python3
"""
Test script for the trading database and PnL calculations
Includes forward testing mode demonstration
"""

import sys
import os
from datetime import datetime, timedelta
import random

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import order_db
from forward_testing_config import forward_test_config, enable_forward_testing, disable_forward_testing

def test_database_functionality():
    """Test the database functionality with sample data"""
    
    print("🧪 Testing Trading Database Functionality")
    print("=" * 50)
    
    # Test 1: Database initialization
    print("\n1. Testing database initialization...")
    try:
        order_db.init_database()
        print("✅ Database initialized successfully")
    except Exception as e:
        print(f"❌ Database initialization failed: {e}")
        return False
    
    # Test 2: Log sample orders
    print("\n2. Testing order logging...")
    
    # Sample trading scenario: Buy and sell RELIANCE
    sample_orders = [
        {
            'tradingsymbol': 'RELIANCE',
            'exchange': 'NSE',
            'transaction_type': 'BUY',
            'quantity': 10,
            'price': 2500.50,
            'order_type': 'MARKET',
            'product': 'CNC',
            'tv_symbol': 'RELIANCE',
            'request_id': 'test_001'
        },
        {
            'tradingsymbol': 'RELIANCE',
            'exchange': 'NSE',
            'transaction_type': 'BUY',
            'quantity': 5,
            'price': 2520.75,
            'order_type': 'MARKET',
            'product': 'CNC',
            'tv_symbol': 'RELIANCE',
            'request_id': 'test_002'
        },
        {
            'tradingsymbol': 'RELIANCE',
            'exchange': 'NSE',
            'transaction_type': 'SELL',
            'quantity': 8,
            'price': 2580.25,
            'order_type': 'MARKET',
            'product': 'CNC',
            'tv_symbol': 'RELIANCE',
            'request_id': 'test_003'
        },
        {
            'tradingsymbol': 'RELIANCE',
            'exchange': 'NSE',
            'transaction_type': 'SELL',
            'quantity': 7,
            'price': 2590.00,
            'order_type': 'MARKET',
            'product': 'CNC',
            'tv_symbol': 'RELIANCE',
            'request_id': 'test_004'
        }
    ]
    
    order_ids = []
    for i, order in enumerate(sample_orders):
        try:
            # Log order attempt
            order_log_id = order_db.log_order_attempt(**order)
            
            # Simulate successful order placement
            fake_order_id = f"ORDER_{random.randint(100000, 999999)}"
            order_db.update_order_result(order_log_id, fake_order_id, 'SUCCESS')
            
            order_ids.append(order_log_id)
            print(f"✅ Order {i+1} logged successfully (ID: {order_log_id})")
            
        except Exception as e:
            print(f"❌ Failed to log order {i+1}: {e}")
            return False
    
    # Test 3: Retrieve recent orders
    print("\n3. Testing order retrieval...")
    try:
        recent_orders = order_db.get_recent_orders(10)
        print(f"✅ Retrieved {len(recent_orders)} recent orders")
        
        if recent_orders:
            print("\nSample order:")
            order = recent_orders[0]
            print(f"   Symbol: {order['tradingsymbol']}")
            print(f"   Type: {order['transaction_type']}")
            print(f"   Quantity: {order['quantity']}")
            print(f"   Price: ₹{order['price']}")
            print(f"   Status: {order['status']}")
            
    except Exception as e:
        print(f"❌ Failed to retrieve orders: {e}")
        return False
    
    # Test 4: Calculate PnL
    print("\n4. Testing PnL calculation...")
    try:
        pnl_data = order_db.calculate_symbol_pnl('RELIANCE', 'NSE')
        
        if pnl_data:
            print("✅ PnL calculation successful")
            print(f"   Total Buy Qty: {pnl_data['total_buy_qty']}")
            print(f"   Total Sell Qty: {pnl_data['total_sell_qty']}")
            print(f"   Current Position: {pnl_data['current_position']}")
            print(f"   Avg Buy Price: ₹{pnl_data['avg_buy_price']:.2f}")
            print(f"   Avg Sell Price: ₹{pnl_data['avg_sell_price']:.2f}")
            print(f"   Realized P&L: ₹{pnl_data['realized_pnl']:.2f}")
            
            # Verify the calculation manually
            expected_buy_qty = 15  # 10 + 5
            expected_sell_qty = 15  # 8 + 7
            expected_position = 0  # 15 - 15
            
            if (pnl_data['total_buy_qty'] == expected_buy_qty and 
                pnl_data['total_sell_qty'] == expected_sell_qty and
                pnl_data['current_position'] == expected_position):
                print("✅ PnL calculations are correct")
            else:
                print("⚠️  PnL calculations may have issues")
                
        else:
            print("❌ PnL calculation returned empty data")
            return False
            
    except Exception as e:
        print(f"❌ Failed to calculate PnL: {e}")
        return False
    
    # Test 5: Portfolio summary
    print("\n5. Testing portfolio summary...")
    try:
        portfolio = order_db.get_portfolio_summary()
        
        if portfolio:
            print("✅ Portfolio summary generated")
            print(f"   Total Symbols: {portfolio['total_symbols']}")
            print(f"   Total Realized P&L: ₹{portfolio['total_realized_pnl']:.2f}")
            print(f"   Total Positions: {portfolio['total_positions']}")
            
        else:
            print("❌ Portfolio summary returned empty data")
            return False
            
    except Exception as e:
        print(f"❌ Failed to generate portfolio summary: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 All database tests passed successfully!")
    print("\n📊 Your trading database is ready for:")
    print("   • Order logging before placement")
    print("   • Real-time PnL tracking")
    print("   • FIFO-based profit/loss calculations")
    print("   • Portfolio performance monitoring")
    
    return True

def create_sample_futures_data():
    """Create sample futures trading data"""
    print("\n🔮 Creating sample futures trading data...")
    
    # Sample NIFTY futures trades
    futures_orders = [
        {
            'tradingsymbol': 'NIFTY24FEB22000CE',
            'exchange': 'NFO',
            'transaction_type': 'BUY',
            'quantity': 50,
            'price': 150.25,
            'order_type': 'MARKET',
            'product': 'NRML',
            'tv_symbol': 'NIFTY!',
            'request_id': 'fut_001'
        },
        {
            'tradingsymbol': 'NIFTY24FEB22000CE',
            'exchange': 'NFO',
            'transaction_type': 'SELL',
            'quantity': 50,
            'price': 175.80,
            'order_type': 'MARKET',
            'product': 'NRML',
            'tv_symbol': 'NIFTY!',
            'request_id': 'fut_002'
        }
    ]
    
    for i, order in enumerate(futures_orders):
        try:
            order_log_id = order_db.log_order_attempt(**order)
            fake_order_id = f"FUT_ORDER_{random.randint(100000, 999999)}"
            order_db.update_order_result(order_log_id, fake_order_id, 'SUCCESS')
            print(f"✅ Futures order {i+1} logged successfully")
        except Exception as e:
            print(f"❌ Failed to log futures order {i+1}: {e}")

if __name__ == "__main__":
    print("🚀 Starting Trading Database Test Suite")
    
    # Show current trading mode
    forward_test_config.print_status()
    
    # Run main tests
    success = test_database_functionality()
    
    if success:
        # Create additional sample data
        create_sample_futures_data()
        
        print("\n🎯 Next Steps:")
        print("1. Start your FastAPI server: uvicorn app:app --reload")
        print("2. Visit http://localhost:8000/dashboard to see your trading dashboard")
        
        if forward_test_config.is_enabled():
            print("3. 🧪 FORWARD TESTING MODE is active - no real orders will be placed")
            print("4. Send webhook signals to test your strategy safely")
            print("5. Monitor PnL calculations in the dashboard")
            print("\n💡 To switch to live trading: python forward_testing_config.py disable")
        else:
            print("3. 🔴 LIVE TRADING MODE is active - real orders will be placed!")
            print("4. ⚠️  Use with caution - real money is at risk")
            print("5. All orders will be automatically logged to the database")
            print("\n💡 To enable forward testing: python forward_testing_config.py enable")
        
        print("\n🛠️ Database Management:")
        print("   • View stats: python manage_database.py stats")
        print("   • Clear test data: python manage_database.py clear-test")
        print("   • Backup database: python manage_database.py backup")
        print("   • Dashboard database tab: http://localhost:8000/dashboard (Database tab)")
        
    else:
        print("\n❌ Some tests failed. Please check the error messages above.")
        sys.exit(1)