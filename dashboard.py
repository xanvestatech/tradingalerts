from fastapi import APIRouter, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from database import order_db
from typing import Optional
import logging

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/dashboard", response_class=HTMLResponse)
def trading_dashboard():
    """Main trading dashboard with orders and PnL tracking"""
    
    html_content = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Trading Dashboard - Orders & PnL</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                color: #333;
                line-height: 1.6;
            }
            
            .container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
            }
            
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 30px;
                text-align: center;
            }
            
            .header h1 {
                font-size: 2.5em;
                margin-bottom: 10px;
            }
            
            .stats-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                gap: 20px;
                margin-bottom: 30px;
            }
            
            .stat-card {
                background: white;
                padding: 20px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                text-align: center;
            }
            
            .stat-value {
                font-size: 2em;
                font-weight: bold;
                margin-bottom: 5px;
            }
            
            .stat-label {
                color: #666;
                font-size: 0.9em;
            }
            
            .positive { color: #27ae60; }
            .negative { color: #e74c3c; }
            .neutral { color: #3498db; }
            
            .section {
                background: white;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                margin-bottom: 30px;
                overflow: hidden;
            }
            
            .section-header {
                background: #34495e;
                color: white;
                padding: 15px 20px;
                font-size: 1.2em;
                font-weight: bold;
            }
            
            .section-content {
                padding: 20px;
            }
            
            .table-container {
                overflow-x: auto;
            }
            
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 10px;
            }
            
            th, td {
                padding: 12px;
                text-align: left;
                border-bottom: 1px solid #ddd;
            }
            
            th {
                background-color: #f8f9fa;
                font-weight: bold;
                color: #2c3e50;
            }
            
            tr:hover {
                background-color: #f8f9fa;
            }
            
            .status-success { 
                background-color: #d4edda; 
                color: #155724; 
                padding: 4px 8px; 
                border-radius: 4px; 
                font-size: 0.8em;
            }
            
            .status-failed { 
                background-color: #f8d7da; 
                color: #721c24; 
                padding: 4px 8px; 
                border-radius: 4px; 
                font-size: 0.8em;
            }
            
            .status-pending { 
                background-color: #fff3cd; 
                color: #856404; 
                padding: 4px 8px; 
                border-radius: 4px; 
                font-size: 0.8em;
            }
            
            .status-forward-test { 
                background-color: #cce5ff; 
                color: #004085; 
                padding: 4px 8px; 
                border-radius: 4px; 
                font-size: 0.8em;
            }
            
            .status-duplicate { 
                background-color: #e2e3e5; 
                color: #383d41; 
                padding: 4px 8px; 
                border-radius: 4px; 
                font-size: 0.8em;
            }
            
            .refresh-btn {
                background: #3498db;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 1em;
                margin-bottom: 20px;
            }
            
            .refresh-btn:hover {
                background: #2980b9;
            }
            
            .clear-btn {
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 5px;
                cursor: pointer;
                font-size: 1em;
                margin: 5px;
            }
            
            .clear-btn:hover {
                opacity: 0.8;
            }
            
            .clear-btn.danger:hover {
                background: #c0392b !important;
            }
            
            .loading {
                text-align: center;
                padding: 20px;
                color: #666;
            }
            
            .error {
                background-color: #f8d7da;
                color: #721c24;
                padding: 15px;
                border-radius: 5px;
                margin: 10px 0;
            }
            
            .tabs {
                display: flex;
                background: #ecf0f1;
                border-radius: 10px 10px 0 0;
            }
            
            .tab {
                flex: 1;
                padding: 15px;
                text-align: center;
                cursor: pointer;
                background: #ecf0f1;
                border: none;
                font-size: 1em;
            }
            
            .tab.active {
                background: white;
                border-bottom: 3px solid #3498db;
            }
            
            .tab-content {
                display: none;
            }
            
            .tab-content.active {
                display: block;
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 10px;
                }
                
                .header h1 {
                    font-size: 2em;
                }
                
                .stats-grid {
                    grid-template-columns: 1fr;
                }
                
                table {
                    font-size: 0.9em;
                }
                
                th, td {
                    padding: 8px;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📈 Trading Dashboard</h1>
                <p>Real-time Order Tracking & PnL Analysis</p>
            </div>
            
            <button class="refresh-btn" onclick="refreshData()">🔄 Refresh Data</button>
            
            <div class="stats-grid" id="statsGrid">
                <div class="loading">Loading statistics...</div>
            </div>
            
            <div class="section">
                <div class="tabs">
                    <button class="tab active" onclick="showTab('orders')">📋 Recent Orders</button>
                    <button class="tab" onclick="showTab('pnl')">💰 PnL Summary</button>
                    <button class="tab" onclick="showTab('positions')">📊 Current Positions</button>
                    <button class="tab" onclick="showTab('database')">🗄️ Database</button>
                </div>
                
                <div class="tab-content active" id="orders">
                    <div class="section-content">
                        <div class="table-container" id="ordersTable">
                            <div class="loading">Loading orders...</div>
                        </div>
                    </div>
                </div>
                
                <div class="tab-content" id="pnl">
                    <div class="section-content">
                        <div class="table-container" id="pnlTable">
                            <div class="loading">Loading PnL data...</div>
                        </div>
                    </div>
                </div>
                
                <div class="tab-content" id="positions">
                    <div class="section-content">
                        <div class="table-container" id="positionsTable">
                            <div class="loading">Loading positions...</div>
                        </div>
                    </div>
                </div>
                
                <div class="tab-content" id="database">
                    <div class="section-content">
                        <div id="databaseStats">
                            <div class="loading">Loading database statistics...</div>
                        </div>
                        
                        <div style="margin-top: 30px;">
                            <h3>🗑️ Database Management</h3>
                            <div style="margin-top: 20px;">
                                <button class="clear-btn" onclick="clearTestData()" style="background: #f39c12; margin-right: 10px;">
                                    🧪 Clear Test Data Only
                                </button>
                                <button class="clear-btn danger" onclick="clearAllData()" style="background: #e74c3c;">
                                    ⚠️ Clear All Data
                                </button>
                            </div>
                            <div style="margin-top: 15px; padding: 15px; background: #fff3cd; border-radius: 5px; color: #856404;">
                                <strong>⚠️ Warning:</strong>
                                <ul style="margin: 10px 0 0 20px;">
                                    <li><strong>Clear Test Data:</strong> Removes only forward testing orders (FORWARD_TEST_SUCCESS status)</li>
                                    <li><strong>Clear All Data:</strong> Removes ALL orders, trades, and PnL data permanently</li>
                                    <li>This action cannot be undone. Use with caution!</li>
                                </ul>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            let currentTab = 'orders';
            
            function showTab(tabName) {
                // Hide all tab contents
                document.querySelectorAll('.tab-content').forEach(content => {
                    content.classList.remove('active');
                });
                
                // Remove active class from all tabs
                document.querySelectorAll('.tab').forEach(tab => {
                    tab.classList.remove('active');
                });
                
                // Show selected tab content
                document.getElementById(tabName).classList.add('active');
                
                // Add active class to clicked tab
                event.target.classList.add('active');
                
                currentTab = tabName;
            }
            
            function formatCurrency(value) {
                if (value === null || value === undefined) return '₹0.00';
                return '₹' + parseFloat(value).toLocaleString('en-IN', {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2
                });
            }
            
            function formatDateTime(dateString) {
                if (!dateString) return '-';
                const date = new Date(dateString);
                return date.toLocaleString('en-IN', {
                    year: 'numeric',
                    month: 'short',
                    day: '2-digit',
                    hour: '2-digit',
                    minute: '2-digit'
                });
            }
            
            function getStatusClass(status) {
                if (status === 'SUCCESS') return 'status-success';
                if (status === 'FAILED') return 'status-failed';
                if (status === 'FORWARD_TEST_SUCCESS') return 'status-forward-test';
                if (status === 'DUPLICATE_PREVENTED') return 'status-duplicate';
                return 'status-pending';
            }
            
            function getPnLClass(value) {
                if (value > 0) return 'positive';
                if (value < 0) return 'negative';
                return 'neutral';
            }
            
            async function loadStats() {
                try {
                    const response = await fetch('/api/portfolio-summary');
                    const data = await response.json();
                    
                    const statsHtml = `
                        <div class="stat-card">
                            <div class="stat-value neutral">${data.total_symbols || 0}</div>
                            <div class="stat-label">Total Symbols</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value ${getPnLClass(data.total_realized_pnl || 0)}">${formatCurrency(data.total_realized_pnl || 0)}</div>
                            <div class="stat-label">Total Realized P&L</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value neutral">${data.total_positions || 0}</div>
                            <div class="stat-label">Open Positions</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value neutral">${data.total_orders || 0}</div>
                            <div class="stat-label">Total Orders</div>
                        </div>
                    `;
                    
                    document.getElementById('statsGrid').innerHTML = statsHtml;
                } catch (error) {
                    document.getElementById('statsGrid').innerHTML = '<div class="error">Failed to load statistics</div>';
                }
            }
            
            async function loadOrders() {
                try {
                    const response = await fetch('/api/recent-orders?limit=50');
                    const orders = await response.json();
                    
                    if (orders.length === 0) {
                        document.getElementById('ordersTable').innerHTML = '<p>No orders found</p>';
                        return;
                    }
                    
                    let tableHtml = `
                        <table>
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Symbol</th>
                                    <th>Type</th>
                                    <th>Qty</th>
                                    <th>Price</th>
                                    <th>Status</th>
                                    <th>Order ID</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;
                    
                    orders.forEach(order => {
                        tableHtml += `
                            <tr>
                                <td>${formatDateTime(order.timestamp)}</td>
                                <td>${order.tradingsymbol}</td>
                                <td><span class="${order.transaction_type === 'BUY' ? 'positive' : 'negative'}">${order.transaction_type}</span></td>
                                <td>${order.quantity}</td>
                                <td>${order.price ? formatCurrency(order.price) : 'Market'}</td>
                                <td><span class="${getStatusClass(order.status)}">${order.status}</span></td>
                                <td>${order.order_id || '-'}</td>
                            </tr>
                        `;
                    });
                    
                    tableHtml += '</tbody></table>';
                    document.getElementById('ordersTable').innerHTML = tableHtml;
                } catch (error) {
                    document.getElementById('ordersTable').innerHTML = '<div class="error">Failed to load orders</div>';
                }
            }
            
            async function loadPnL() {
                try {
                    const response = await fetch('/api/portfolio-summary');
                    const data = await response.json();
                    
                    if (!data.symbols || data.symbols.length === 0) {
                        document.getElementById('pnlTable').innerHTML = '<p>No PnL data available</p>';
                        return;
                    }
                    
                    let tableHtml = `
                        <table>
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Exchange</th>
                                    <th>Buy Qty</th>
                                    <th>Sell Qty</th>
                                    <th>Position</th>
                                    <th>Avg Buy</th>
                                    <th>Avg Sell</th>
                                    <th>Realized P&L</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;
                    
                    data.symbols.forEach(symbol => {
                        tableHtml += `
                            <tr>
                                <td>${symbol.tradingsymbol}</td>
                                <td>${symbol.exchange}</td>
                                <td>${symbol.total_buy_qty}</td>
                                <td>${symbol.total_sell_qty}</td>
                                <td><span class="${getPnLClass(symbol.current_position)}">${symbol.current_position}</span></td>
                                <td>${formatCurrency(symbol.avg_buy_price)}</td>
                                <td>${formatCurrency(symbol.avg_sell_price)}</td>
                                <td><span class="${getPnLClass(symbol.realized_pnl)}">${formatCurrency(symbol.realized_pnl)}</span></td>
                            </tr>
                        `;
                    });
                    
                    tableHtml += '</tbody></table>';
                    document.getElementById('pnlTable').innerHTML = tableHtml;
                } catch (error) {
                    document.getElementById('pnlTable').innerHTML = '<div class="error">Failed to load PnL data</div>';
                }
            }
            
            async function loadPositions() {
                try {
                    const response = await fetch('/api/portfolio-summary');
                    const data = await response.json();
                    
                    const openPositions = data.symbols ? data.symbols.filter(s => s.current_position !== 0) : [];
                    
                    if (openPositions.length === 0) {
                        document.getElementById('positionsTable').innerHTML = '<p>No open positions</p>';
                        return;
                    }
                    
                    let tableHtml = `
                        <table>
                            <thead>
                                <tr>
                                    <th>Symbol</th>
                                    <th>Exchange</th>
                                    <th>Position</th>
                                    <th>Avg Price</th>
                                    <th>Realized P&L</th>
                                </tr>
                            </thead>
                            <tbody>
                    `;
                    
                    openPositions.forEach(position => {
                        const avgPrice = position.current_position > 0 ? position.avg_buy_price : position.avg_sell_price;
                        tableHtml += `
                            <tr>
                                <td>${position.tradingsymbol}</td>
                                <td>${position.exchange}</td>
                                <td><span class="${getPnLClass(position.current_position)}">${position.current_position}</span></td>
                                <td>${formatCurrency(avgPrice)}</td>
                                <td><span class="${getPnLClass(position.realized_pnl)}">${formatCurrency(position.realized_pnl)}</span></td>
                            </tr>
                        `;
                    });
                    
                    tableHtml += '</tbody></table>';
                    document.getElementById('positionsTable').innerHTML = tableHtml;
                } catch (error) {
                    document.getElementById('positionsTable').innerHTML = '<div class="error">Failed to load positions</div>';
                }
            }
            
            async function loadDatabaseStats() {
                try {
                    const response = await fetch('/api/database-stats');
                    const stats = await response.json();
                    
                    let statusBreakdown = '';
                    if (stats.orders_by_status) {
                        statusBreakdown = Object.entries(stats.orders_by_status)
                            .map(([status, count]) => `<li>${status}: ${count}</li>`)
                            .join('');
                    }
                    
                    const statsHtml = `
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 20px;">
                            <div class="stat-card">
                                <div class="stat-value neutral">${stats.total_orders || 0}</div>
                                <div class="stat-label">Total Orders</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-value neutral">${stats.total_trades || 0}</div>
                                <div class="stat-label">Total Trades</div>
                            </div>
                            <div class="stat-card">
                                <div class="stat-value neutral">${stats.file_size_mb || 0} MB</div>
                                <div class="stat-label">Database Size</div>
                            </div>
                        </div>
                        
                        <div style="background: #f8f9fa; padding: 15px; border-radius: 5px;">
                            <h4>📊 Orders by Status</h4>
                            <ul style="margin: 10px 0 0 20px;">
                                ${statusBreakdown || '<li>No orders found</li>'}
                            </ul>
                            
                            <h4 style="margin-top: 15px;">📅 Date Range</h4>
                            <p style="margin: 5px 0;">
                                <strong>Earliest:</strong> ${stats.date_range?.earliest ? formatDateTime(stats.date_range.earliest) : 'N/A'}<br>
                                <strong>Latest:</strong> ${stats.date_range?.latest ? formatDateTime(stats.date_range.latest) : 'N/A'}
                            </p>
                            
                            <h4 style="margin-top: 15px;">💾 Database File</h4>
                            <p style="margin: 5px 0; font-family: monospace; background: #e9ecef; padding: 5px; border-radius: 3px;">
                                ${stats.database_file || 'trading_orders.db'}
                            </p>
                        </div>
                    `;
                    
                    document.getElementById('databaseStats').innerHTML = statsHtml;
                } catch (error) {
                    document.getElementById('databaseStats').innerHTML = '<div class="error">Failed to load database statistics</div>';
                }
            }
            
            async function clearTestData() {
                if (!confirm('🧪 Clear all forward testing data?\n\nThis will remove all orders with FORWARD_TEST_SUCCESS status.\nThis action cannot be undone.')) {
                    return;
                }
                
                try {
                    const response = await fetch('/api/clear-test-data', {
                        method: 'POST'
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        alert(`✅ Test data cleared successfully!\n\nDeleted:\n- Test Orders: ${result.deleted_records.test_orders}\n- Test Trades: ${result.deleted_records.test_trades}\n- Total: ${result.deleted_records.total} records`);
                        await refreshData();
                    } else {
                        alert(`❌ Failed to clear test data:\n${result.error}`);
                    }
                } catch (error) {
                    alert(`❌ Error clearing test data:\n${error.message}`);
                }
            }
            
            async function clearAllData() {
                const confirmText = prompt(
                    '⚠️ DANGER: Clear ALL database data?\n\n' +
                    'This will permanently delete:\n' +
                    '• All orders (live and test)\n' +
                    '• All trades\n' +
                    '• All PnL data\n' +
                    '• All statistics\n\n' +
                    'Type "CONFIRM_DELETE_ALL_DATA" to proceed:'
                );
                
                if (confirmText !== 'CONFIRM_DELETE_ALL_DATA') {
                    alert('❌ Deletion cancelled. Confirmation text did not match.');
                    return;
                }
                
                try {
                    const response = await fetch(`/api/clear-database?confirm_token=${encodeURIComponent(confirmText)}`, {
                        method: 'POST'
                    });
                    
                    const result = await response.json();
                    
                    if (result.success) {
                        alert(`✅ All data cleared successfully!\n\nDeleted:\n- Orders: ${result.deleted_records.orders}\n- Trades: ${result.deleted_records.trades}\n- PnL Records: ${result.deleted_records.pnl_summary}\n- Total: ${result.deleted_records.total} records`);
                        await refreshData();
                    } else {
                        alert(`❌ Failed to clear database:\n${result.error}`);
                    }
                } catch (error) {
                    alert(`❌ Error clearing database:\n${error.message}`);
                }
            }
            
            async function refreshData() {
                await loadStats();
                
                if (currentTab === 'orders') {
                    await loadOrders();
                } else if (currentTab === 'pnl') {
                    await loadPnL();
                } else if (currentTab === 'positions') {
                    await loadPositions();
                } else if (currentTab === 'database') {
                    await loadDatabaseStats();
                }
            }
            
            // Initial load
            document.addEventListener('DOMContentLoaded', async function() {
                await refreshData();
                
                // Auto-refresh every 30 seconds
                setInterval(refreshData, 30000);
            });
        </script>
    </body>
    </html>
    """
    
    return HTMLResponse(content=html_content)

@router.get("/api/recent-orders")
def get_recent_orders(limit: int = Query(50, ge=1, le=200)):
    """API endpoint to get recent orders"""
    try:
        orders = order_db.get_recent_orders(limit)
        return orders
    except Exception as e:
        logger.error(f"Failed to get recent orders: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to fetch orders"}, status_code=500)

@router.get("/api/portfolio-summary")
def get_portfolio_summary():
    """API endpoint to get portfolio summary with PnL"""
    try:
        portfolio = order_db.get_portfolio_summary()
        
        # Add total orders count
        recent_orders = order_db.get_recent_orders(1000)  # Get more orders for count
        portfolio['total_orders'] = len(recent_orders)
        
        return portfolio
    except Exception as e:
        logger.error(f"Failed to get portfolio summary: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to fetch portfolio summary"}, status_code=500)

@router.get("/api/symbol-pnl/{symbol}")
def get_symbol_pnl(symbol: str, exchange: Optional[str] = Query(None)):
    """API endpoint to get PnL for a specific symbol"""
    try:
        if not exchange:
            # Try to determine exchange from recent orders
            orders = order_db.get_orders_by_symbol(symbol)
            if orders:
                exchange = orders[0]['exchange']
            else:
                return JSONResponse(content={"error": "Symbol not found"}, status_code=404)
        
        pnl_data = order_db.calculate_symbol_pnl(symbol, exchange)
        return pnl_data
    except Exception as e:
        logger.error(f"Failed to get symbol PnL: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to fetch symbol PnL"}, status_code=500)

@router.get("/api/database-stats")
def get_database_stats():
    """API endpoint to get database statistics"""
    try:
        stats = order_db.get_database_stats()
        return stats
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to fetch database stats"}, status_code=500)

@router.post("/api/clear-database")
def clear_database(confirm_token: str = Query(...)):
    """
    API endpoint to clear all database data
    
    Requires confirmation token: CONFIRM_DELETE_ALL_DATA
    """
    try:
        result = order_db.clear_all_data(confirm_token)
        
        if result["success"]:
            return JSONResponse(content=result, status_code=200)
        else:
            return JSONResponse(content=result, status_code=400)
            
    except Exception as e:
        logger.error(f"Failed to clear database: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to clear database"}, status_code=500)

@router.post("/api/clear-test-data")
def clear_test_data():
    """API endpoint to clear only forward testing data"""
    try:
        result = order_db.clear_test_data()
        return JSONResponse(content=result, status_code=200)
    except Exception as e:
        logger.error(f"Failed to clear test data: {e}", exc_info=True)
        return JSONResponse(content={"error": "Failed to clear test data"}, status_code=500)