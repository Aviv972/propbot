#!/usr/bin/env python3
"""
Generate a simple dashboard for visualizing property investment metrics
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "propbot" / "data"
OUTPUT_DIR = DATA_DIR / "output"
REPORTS_DIR = OUTPUT_DIR / "reports"
VISUALIZATIONS_DIR = OUTPUT_DIR / "visualizations"

# Ensure directories exist
VISUALIZATIONS_DIR.mkdir(parents=True, exist_ok=True)

# Input files
RENTAL_REPORT = REPORTS_DIR / "rental_income_report_current.json"
METRICS_CSV = REPORTS_DIR / "investment_metrics_current.csv"

# Output files
DASHBOARD_HTML = VISUALIZATIONS_DIR / "investment_dashboard.html"
DASHBOARD_JSON = VISUALIZATIONS_DIR / "dashboard_data.json"

def generate_simple_dashboard():
    """Generate a simplified investment dashboard"""
    logger.info("Generating simple investment dashboard...")
    
    # Check if required files exist
    if not os.path.exists(RENTAL_REPORT):
        legacy_rental_report = DATA_DIR / "processed" / "rental_income_report_improved.json"
        if os.path.exists(legacy_rental_report):
            logger.info(f"Using legacy rental report: {legacy_rental_report}")
            rental_report_path = legacy_rental_report
        else:
            logger.error(f"Rental report not found at {RENTAL_REPORT} or {legacy_rental_report}")
            return False
    else:
        rental_report_path = RENTAL_REPORT
    
    # Load rental income report
    try:
        with open(rental_report_path, 'r') as f:
            rental_data_raw = json.load(f)
            
        # Check if the data is a dictionary with URLs as keys or a list of properties
        if isinstance(rental_data_raw, dict):
            # Convert dictionary to list format for easier processing
            rental_data = []
            for url, prop_data in rental_data_raw.items():
                # Ensure the URL is in the property data
                if 'url' not in prop_data:
                    prop_data['url'] = url
                rental_data.append(prop_data)
        else:
            # Already in list format
            rental_data = rental_data_raw
            
        logger.info(f"Loaded rental report with {len(rental_data)} properties")
    except Exception as e:
        logger.error(f"Error loading rental report: {e}")
        return False
    
    # Calculate summary statistics
    total_properties = len(rental_data)
    valid_estimates = sum(1 for p in rental_data if p.get('reason') == 'Valid estimate')
    
    valid_yields = [p.get('gross_rental_yield', 0) for p in rental_data 
                   if p.get('reason') == 'Valid estimate' and p.get('gross_rental_yield', 0) > 0]
    
    avg_yield = sum(valid_yields) / len(valid_yields) if valid_yields else 0
    min_yield = min(valid_yields) if valid_yields else 0
    max_yield = max(valid_yields) if valid_yields else 0
    
    # Create a simple HTML dashboard
    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Investment Dashboard</title>
    <style>
        body {{ 
            font-family: Arial, sans-serif; 
            margin: 20px; 
            background-color: #f5f8fa;
            color: #333;
        }}
        h1, h2 {{ 
            color: #2c3e50; 
            margin-bottom: 15px;
        }}
        .summary {{ 
            background-color: white; 
            padding: 20px; 
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            margin-bottom: 30px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-top: 15px;
        }}
        .stat-card {{
            background-color: #f8f9fa;
            border-radius: 5px;
            padding: 15px;
        }}
        .stat-label {{
            font-weight: bold;
            color: #555;
            margin-bottom: 5px;
        }}
        .stat-value {{
            font-size: 22px;
            color: #3498db;
        }}
        table {{ 
            border-collapse: collapse; 
            width: 100%; 
            margin-top: 20px;
            background-color: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        th, td {{ 
            text-align: left; 
            padding: 12px 15px; 
            border-bottom: 1px solid #ddd; 
        }}
        th {{ 
            background-color: #4CAF50; 
            color: white; 
            font-weight: 500;
        }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f1f5f8; }}
        .high-yield {{ color: #27ae60; font-weight: bold; }}
        .medium-yield {{ color: #f39c12; font-weight: bold; }}
        .low-yield {{ color: #e74c3c; font-weight: bold; }}
        .view-button {{
            display: inline-block;
            padding: 6px 12px;
            background-color: #3498db;
            color: white;
            text-decoration: none;
            border-radius: 4px;
            font-size: 14px;
        }}
        .view-button:hover {{
            background-color: #2980b9;
        }}
        .timestamp {{
            color: #7f8c8d;
            font-size: 14px;
            margin-bottom: 30px;
        }}
    </style>
</head>
<body>
    <h1>Property Investment Dashboard</h1>
    <div class="timestamp">Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    
    <div class="summary">
        <h2>Summary</h2>
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-label">Total Properties</div>
                <div class="stat-value">{total_properties}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Valid Estimates</div>
                <div class="stat-value">{valid_estimates}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Average Rental Yield</div>
                <div class="stat-value">{avg_yield:.2f}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Yield Range</div>
                <div class="stat-value">{min_yield:.2f}% - {max_yield:.2f}%</div>
            </div>
        </div>
    </div>
    
    <h2>Top Investment Properties</h2>
    <table>
        <tr>
            <th>Details</th>
            <th>Neighborhood</th>
            <th>Price (€)</th>
            <th>Size (m²)</th>
            <th>Rooms</th>
            <th>Estimated Rent (€)</th>
            <th>NOI (€)</th>
            <th>Cap Rate (%)</th>
            <th>Gross Yield (%)</th>
            <th>Cash on Cash Return (%)</th>
            <th>Monthly Cash Flow (€)</th>
            <th>Price per sqm (€)</th>
        </tr>
"""

    # Sort properties by yield in descending order
    sorted_properties = sorted(
        [p for p in rental_data if p.get('reason') == 'Valid estimate'], 
        key=lambda p: p.get('gross_rental_yield', 0), 
        reverse=True
    )
    
    # Add top 50 properties to the table
    for prop in sorted_properties[:50]:
        # Calculate additional metrics
        monthly_rent = prop.get('estimated_monthly_rent', 0)
        annual_rent = prop.get('estimated_annual_rent', monthly_rent * 12)
        price = prop.get('price', 0)
        size = prop.get('size', 1)
        
        # Financial metrics
        noi = annual_rent * 0.7  # Assuming 30% expenses
        cap_rate = noi / price * 100 if price else 0
        monthly_cash_flow = monthly_rent * 0.7 - (price * 0.8 * 0.035 / 12)  # Assuming 20% down payment, 3.5% interest
        price_per_sqm = price / size if size else 0
        down_payment = price * 0.2  # Assuming 20% down payment
        annual_cash_flow = monthly_cash_flow * 12
        cash_on_cash = annual_cash_flow / down_payment * 100 if down_payment else 0
        
        # Determine yield class
        yield_value = prop.get('gross_rental_yield', 0)
        if yield_value >= 7:
            yield_class = 'high-yield'
        elif yield_value >= 5:
            yield_class = 'medium-yield'
        else:
            yield_class = 'low-yield'
        
        html += f"""
        <tr>
            <td><a href="{prop.get('url', '#')}" target="_blank" class="view-button">View</a></td>
            <td>{prop.get('location', 'Unknown')}</td>
            <td>€{price:,.0f}</td>
            <td>{size:,.0f}</td>
            <td>{prop.get('num_rooms', 'N/A')}</td>
            <td>€{monthly_rent:,.0f}</td>
            <td>€{noi:,.0f}</td>
            <td>{cap_rate:.2f}%</td>
            <td class="{yield_class}">{yield_value:.2f}%</td>
            <td>{cash_on_cash:.2f}%</td>
            <td>€{monthly_cash_flow:,.0f}</td>
            <td>€{price_per_sqm:,.0f}</td>
        </tr>"""
    
    # Close HTML
    html += """
    </table>
</body>
</html>
"""
    
    # Save the HTML file
    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    logger.info(f"Generated dashboard HTML at {DASHBOARD_HTML}")
    
    # Copy to UI directory for legacy support
    ui_dir = PROJECT_ROOT / "propbot" / "ui"
    ui_dir.mkdir(exist_ok=True)
    ui_html = ui_dir / "investment_dashboard.html"
    
    with open(ui_html, 'w') as f:
        f.write(html)
    logger.info(f"Copied dashboard to UI directory: {ui_html}")
    
    return True

if __name__ == "__main__":
    generate_simple_dashboard() 