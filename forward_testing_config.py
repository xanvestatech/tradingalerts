"""
Forward Testing Configuration

This module provides easy configuration for forward testing mode.
When forward testing is enabled, orders are logged to the database but not actually placed.
"""

import os
from typing import Dict, Any

class ForwardTestingConfig:
    """Configuration class for forward testing mode"""
    
    def __init__(self):
        self.enabled = os.getenv("FORWARD_TESTING_MODE", "false").lower() == "true"
        self.log_level = os.getenv("FORWARD_TESTING_LOG_LEVEL", "INFO")
        self.simulate_failures = os.getenv("FORWARD_TESTING_SIMULATE_FAILURES", "false").lower() == "true"
        self.failure_rate = float(os.getenv("FORWARD_TESTING_FAILURE_RATE", "0.05"))  # 5% failure rate
    
    def is_enabled(self) -> bool:
        """Check if forward testing mode is enabled"""
        return self.enabled
    
    def enable(self):
        """Enable forward testing mode"""
        self.enabled = True
        os.environ["FORWARD_TESTING_MODE"] = "true"
        print("🧪 Forward testing mode ENABLED")
        print("   • Orders will be logged but not placed")
        print("   • Simulated order IDs will be generated")
        print("   • PnL calculations will work normally")
    
    def disable(self):
        """Disable forward testing mode"""
        self.enabled = False
        os.environ["FORWARD_TESTING_MODE"] = "false"
        print("🔴 Forward testing mode DISABLED")
        print("   • Orders will be placed normally")
        print("   • Real order IDs will be used")
        print("   • Live trading is active")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current forward testing status"""
        return {
            "enabled": self.enabled,
            "log_level": self.log_level,
            "simulate_failures": self.simulate_failures,
            "failure_rate": self.failure_rate,
            "mode": "FORWARD TESTING" if self.enabled else "LIVE TRADING"
        }
    
    def print_status(self):
        """Print current configuration status"""
        status = self.get_status()
        print("\n" + "="*50)
        print("📊 TRADING MODE CONFIGURATION")
        print("="*50)
        print(f"Mode: {status['mode']}")
        print(f"Enabled: {status['enabled']}")
        print(f"Log Level: {status['log_level']}")
        
        if self.enabled:
            print("\n🧪 FORWARD TESTING FEATURES:")
            print("   ✅ Orders logged to database")
            print("   ✅ Simulated order IDs generated")
            print("   ✅ PnL calculations active")
            print("   ✅ Dashboard shows test results")
            print("   ❌ No real orders placed")
            
            if self.simulate_failures:
                print(f"   ⚠️  Simulating {self.failure_rate*100:.1f}% failure rate")
        else:
            print("\n🔴 LIVE TRADING FEATURES:")
            print("   ✅ Real orders placed to Zerodha")
            print("   ✅ Actual order IDs received")
            print("   ✅ Real money at risk")
            print("   ⚠️  Use with caution!")
        
        print("="*50)

# Global configuration instance
forward_test_config = ForwardTestingConfig()

def enable_forward_testing():
    """Convenience function to enable forward testing"""
    forward_test_config.enable()

def disable_forward_testing():
    """Convenience function to disable forward testing"""
    forward_test_config.disable()

def is_forward_testing_enabled() -> bool:
    """Convenience function to check if forward testing is enabled"""
    return forward_test_config.is_enabled()

def print_trading_mode():
    """Convenience function to print current trading mode"""
    forward_test_config.print_status()

if __name__ == "__main__":
    # Command line interface for configuration
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "enable":
            enable_forward_testing()
        elif command == "disable":
            disable_forward_testing()
        elif command == "status":
            print_trading_mode()
        else:
            print("Usage: python forward_testing_config.py [enable|disable|status]")
    else:
        print_trading_mode()