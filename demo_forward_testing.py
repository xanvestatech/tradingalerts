#!/usr/bin/env python3
"""
Demo script for forward testing functionality
This script demonstrates how to use forward testing mode for strategy validation
"""

import os
import sys
import time
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from forward_testing_config import enable_forward_testing, disable_forward_testing, print_trading_mode
from database import order_db

def demo_forward_testing():
    """Demonstrate forward testing functionality"""
    
    print("🧪 FORWARD TESTING DEMO")
    print("=" * 50)
    
    # Enable forward testing mode
    print("\n1. Enabling forward testing mode...")
    enable_forward_testing()
    
    # Import after enabling forward testing to get the updated environment
    from orders import place_order, FORWARD_TESTING_MODE
    from utils import zerodha_login
    
    print(f"\n2. Forward testing mode status: {FORWARD_TESTING_MODE}")
    
    # Initialize kite (won't be used in forward testing)
    try:
        kite = zerodha_login()
        print("✅ Kite connection initialized (not used in forward testing)")
    except Exception as e:
        print(f"⚠️  Kite connection failed (expected in forward testing): {e}")
        kite = None
    
    # Demo orders for forward testing
    demo_orders = [
        {
            'tradingsymbol': 'RELIANCE',
            'action': 'buy',
            'price': 2500.0,
            'segment': 'NSE',
            'quantity': 10,
            'tv_symbol': 'RELIANCE',
            'request_id': 'demo_001'
        },
        {
            'tradingsymbol': 'RELIANCE',
            'action': 'buy',
            'price': 2520.0,
            'segment': 'NSE',
            'quantity': 5,
            'tv_symbol': 'RELIANCE',
            'request_id': 'demo_002'
        },
        {
            'tradingsymbol': 'RELIANCE',
            'action': 'sell',
            'price': 2580.0,
            'segment': 'NSE',
            'quantity': 8,
            'tv_symbol': 'RELIANCE',
            'request_id': 'demo_003'
        },
        {
            'tradingsymbol': 'TCS',
            'action': 'buy',
            'price': 3500.0,
            'segment': 'NSE',
            'quantity': 2,
            'tv_symbol': 'TCS',
            'request_id': 'demo_004'
        }
    ]
    
    print("\n3. Placing demo orders in forward testing mode...")
    
    for i, order in enumerate(demo_orders, 1):
        print(f"\n   Order {i}: {order['action'].upper()} {order['quantity']} {order['tradingsymbol']} @ ₹{order['price']}")
        
        try:
            order_id, error = place_order(
                kite=kite,
                tradingsymbol=order['tradingsymbol'],
                action=order['action'],
                price=order['price'],
                segment=order['segment'],
                quantity=order['quantity'],
                webhook_timestamp=datetime.now().isoformat(),
                tv_symbol=order['tv_symbol'],
                request_id=order['request_id']
            )
            
            if order_id:
                print(f"   ✅ Order simulated successfully - ID: {order_id}")
            else:
                print(f"   ❌ Order failed: {error}")
                
        except Exception as e:
            print(f"   ❌ Order error: {e}")
        
        # Small delay between orders
        time.sleep(0.5)
    
    print("\n4. Checking database records...")
    recent_orders = order_db.get_recent_orders(10)
    forward_test_orders = [o for o in recent_orders if o['status'] == 'FORWARD_TEST_SUCCESS']
    
    print(f"   📊 Total recent orders: {len(recent_orders)}")
    print(f"   🧪 Forward test orders: {len(forward_test_orders)}")
    
    if forward_test_orders:
        print("\n   Sample forward test order:")
        order = forward_test_orders[0]
        print(f"      Symbol: {order['tradingsymbol']}")
        print(f"      Type: {order['transaction_type']}")
        print(f"      Quantity: {order['quantity']}")
        print(f"      Price: ₹{order['price']}")
        print(f"      Status: {order['status']}")
        print(f"      Order ID: {order['order_id']}")
    
    print("\n5. Calculating PnL for RELIANCE...")
    pnl_data = order_db.calculate_symbol_pnl('RELIANCE', 'NSE')
    
    if pnl_data and pnl_data.get('total_buy_qty', 0) > 0:
        print(f"   📈 RELIANCE PnL Analysis:")
        print(f"      Buy Qty: {pnl_data['total_buy_qty']}")
        print(f"      Sell Qty: {pnl_data['total_sell_qty']}")
        print(f"      Position: {pnl_data['current_position']}")
        print(f"      Avg Buy: ₹{pnl_data['avg_buy_price']:.2f}")
        print(f"      Avg Sell: ₹{pnl_data['avg_sell_price']:.2f}")
        print(f"      Realized P&L: ₹{pnl_data['realized_pnl']:.2f}")
    else:
        print("   ⚠️  No PnL data available yet")
    
    print("\n6. Portfolio summary...")
    portfolio = order_db.get_portfolio_summary()
    
    if portfolio:
        print(f"   📊 Portfolio Overview:")
        print(f"      Total Symbols: {portfolio['total_symbols']}")
        print(f"      Total P&L: ₹{portfolio['total_realized_pnl']:.2f}")
        print(f"      Open Positions: {portfolio['total_positions']}")
    
    print("\n" + "=" * 50)
    print("🎉 Forward Testing Demo Complete!")
    print("\n📋 What happened:")
    print("   ✅ Orders were logged to database")
    print("   ✅ Simulated order IDs were generated")
    print("   ✅ PnL calculations worked normally")
    print("   ✅ No real orders were placed")
    print("   ✅ Dashboard will show all test data")
    
    print("\n🎯 Next steps:")
    print("   1. Visit http://localhost:8000/dashboard to see results")
    print("   2. Test your webhook endpoints safely")
    print("   3. Validate your trading strategy")
    print("   4. When ready, disable forward testing for live trading")
    
    return True

def cleanup_demo_data():
    """Clean up demo data (optional)"""
    print("\n🧹 Cleaning up demo data...")
    
    try:
        # Note: In a real implementation, you might want to add a method to delete test data
        # For now, we'll just show what would be cleaned
        recent_orders = order_db.get_recent_orders(50)
        demo_orders = [o for o in recent_orders if o.get('request_id', '').startswith('demo_')]
        
        print(f"   Found {len(demo_orders)} demo orders")
        print("   (In production, you might want to clean these up)")
        
    except Exception as e:
        print(f"   ❌ Cleanup error: {e}")

if __name__ == "__main__":
    print("🚀 Starting Forward Testing Demo")
    
    # Check command line arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "run":
            demo_forward_testing()
        elif command == "cleanup":
            cleanup_demo_data()
        elif command == "status":
            print_trading_mode()
        elif command == "enable":
            enable_forward_testing()
        elif command == "disable":
            disable_forward_testing()
        else:
            print("Usage: python demo_forward_testing.py [run|cleanup|status|enable|disable]")
    else:
        # Show current status and ask what to do
        print_trading_mode()
        
        print("\n🤔 What would you like to do?")
        print("   1. Run forward testing demo")
        print("   2. Check current status")
        print("   3. Enable forward testing")
        print("   4. Disable forward testing")
        
        try:
            choice = input("\nEnter choice (1-4): ").strip()
            
            if choice == "1":
                demo_forward_testing()
            elif choice == "2":
                print_trading_mode()
            elif choice == "3":
                enable_forward_testing()
            elif choice == "4":
                disable_forward_testing()
            else:
                print("Invalid choice. Exiting.")
                
        except KeyboardInterrupt:
            print("\n\n👋 Demo cancelled by user")
        except Exception as e:
            print(f"\n❌ Error: {e}")