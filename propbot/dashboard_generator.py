#!/usr/bin/env python3
"""
Generate a dashboard for visualizing property investment metrics
"""

import os
import json
import logging
import pandas as pd
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
NEIGHBORHOOD_STATS = DATA_DIR / "metadata" / "neighborhood_stats.json"

# Output files
DASHBOARD_HTML = VISUALIZATIONS_DIR / "investment_dashboard.html"
DASHBOARD_JSON = VISUALIZATIONS_DIR / "dashboard_data.json"
VIZ_NEIGHBORHOOD_STATS = VISUALIZATIONS_DIR / "neighborhood_stats.json"

def generate_dashboard():
    """Generate the investment dashboard HTML and supporting files"""
    logger.info("Generating investment dashboard...")
    
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
    
    if not os.path.exists(METRICS_CSV):
        legacy_metrics = DATA_DIR / "processed" / "investment_metrics.csv"
        if os.path.exists(legacy_metrics):
            logger.info(f"Using legacy metrics file: {legacy_metrics}")
            metrics_path = legacy_metrics
        else:
            logger.error(f"Metrics file not found at {METRICS_CSV} or {legacy_metrics}")
            return False
    else:
        metrics_path = METRICS_CSV
    
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
    
    # Load investment metrics
    try:
        metrics_df = pd.read_csv(metrics_path)
        logger.info(f"Loaded investment metrics with {len(metrics_df)} properties")
    except Exception as e:
        logger.error(f"Error loading investment metrics: {e}")
        return False
    
    # Load neighborhood stats if available
    neighborhood_data = {}
    try:
        if os.path.exists(NEIGHBORHOOD_STATS):
            with open(NEIGHBORHOOD_STATS, 'r') as f:
                neighborhood_data = json.load(f)
            logger.info(f"Loaded neighborhood stats with {len(neighborhood_data)} neighborhoods")
            
            # Copy to visualizations directory
            with open(VIZ_NEIGHBORHOOD_STATS, 'w') as f:
                json.dump(neighborhood_data, f, indent=2)
            logger.info(f"Saved neighborhood stats to visualizations directory")
    except Exception as e:
        logger.warning(f"Error loading neighborhood stats: {e}")
    
    # Prepare data for the dashboard
    dashboard_data = {
        'properties': [],
        'summary': {
            'total_properties': len(rental_data),
            'valid_estimates': sum(1 for p in rental_data if p.get('reason') == 'Valid estimate'),
            'avg_yield': 0,
            'min_yield': 0,
            'max_yield': 0,
            'generated_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'neighborhoods': []
    }
    
    # Add property data
    valid_yields = []
    for prop in rental_data:
        # Make sure all required fields exist
        if not all(k in prop for k in ['url', 'price', 'gross_rental_yield', 'estimated_monthly_rent']):
            continue
            
        dashboard_prop = {
            'url': prop['url'],
            'price': prop.get('price', 0),
            'size': prop.get('size', 0),
            'location': prop.get('location', 'Unknown'),
            'num_rooms': prop.get('num_rooms', 0),
            'monthly_rent': prop.get('estimated_monthly_rent', 0),
            'annual_rent': prop.get('estimated_annual_rent', 0),
            'yield': prop.get('gross_rental_yield', 0),
            'comparable_count': prop.get('comparable_count', 0),
            'valid': prop.get('reason') == 'Valid estimate'
        }
        dashboard_data['properties'].append(dashboard_prop)
        
        if prop.get('reason') == 'Valid estimate' and prop.get('gross_rental_yield', 0) > 0:
            valid_yields.append(prop['gross_rental_yield'])
    
    # Calculate summary statistics
    if valid_yields:
        dashboard_data['summary']['avg_yield'] = sum(valid_yields) / len(valid_yields)
        dashboard_data['summary']['min_yield'] = min(valid_yields)
        dashboard_data['summary']['max_yield'] = max(valid_yields)
    
    # Add neighborhood data
    for name, data in neighborhood_data.items():
        dashboard_data['neighborhoods'].append({
            'name': name,
            'avg_rent_sqm': data.get('avg_rent_sqm', 0),
            'avg_price_sqm': data.get('avg_price_sqm', 0),
            'property_count': data.get('property_count', 0),
            'rental_count': data.get('rental_count', 0)
        })
    
    # Save dashboard data JSON
    with open(DASHBOARD_JSON, 'w') as f:
        json.dump(dashboard_data, f, indent=2)
    logger.info(f"Saved dashboard data to {DASHBOARD_JSON}")
    
    # Create a simple HTML dashboard template
    html_template = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PropBot Investment Dashboard</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f7fb;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #2c3e50;
            margin-bottom: 10px;
        }
        .date {
            color: #7f8c8d;
            margin-bottom: 20px;
        }
        .summary {
            background-color: #fff;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        .summary h2 {
            margin-top: 0;
            color: #2c3e50;
        }
        .metrics {
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
        }
        .metric {
            flex: 1;
            min-width: 200px;
        }
        .metric-label {
            font-weight: bold;
            margin-bottom: 5px;
        }
        .metric-value {
            font-size: 24px;
            color: #3498db;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background-color: #fff;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #f8f9fa;
            font-weight: bold;
            color: #2c3e50;
        }
        tr:hover {
            background-color: #f1f5f9;
        }
        .high-yield {
            color: #27ae60;
            font-weight: bold;
        }
        .medium-yield {
            color: #f39c12;
            font-weight: bold;
        }
        .low-yield {
            color: #e74c3c;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>PropBot Investment Dashboard</h1>
        <div class="date">Generated on %s</div>
        
        <div class="summary">
            <h2>Summary</h2>
            <div class="metrics">
                <div class="metric">
                    <div class="metric-label">Total Properties</div>
                    <div class="metric-value">%d</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Properties with Valid Estimates</div>
                    <div class="metric-value">%d</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Average Rental Yield</div>
                    <div class="metric-value">%.2f%%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Yield Range</div>
                    <div class="metric-value">%.2f%% - %.2f%%</div>
                </div>
            </div>
        </div>
        
        <h2>Investment Properties</h2>
        <table>
            <thead>
                <tr>
                    <th>Location</th>
                    <th>Price (€)</th>
                    <th>Size (m²)</th>
                    <th>Monthly Rent (€)</th>
                    <th>Annual Rent (€)</th>
                    <th>Gross Yield (%)</th>
                    <th>Cap Rate (%)</th>
                    <th>Cash on Cash (%)</th>
                    <th>Monthly Cash Flow (€)</th>
                    <th>Price/m² (€)</th>
                    <th>NOI (€)</th>
                    <th>Comparables</th>
                </tr>
            </thead>
            <tbody>
""" % (
        dashboard_data['summary']['generated_date'],
        dashboard_data['summary']['total_properties'],
        dashboard_data['summary']['valid_estimates'],
        dashboard_data['summary']['avg_yield'],
        dashboard_data['summary']['min_yield'],
        dashboard_data['summary']['max_yield']
    )
    
    # Add property rows
    for prop in dashboard_data['properties']:
        # Calculate additional metrics
        annual_rent = prop['annual_rent']
        cap_rate = annual_rent * 0.7 / prop['price'] * 100 if prop['price'] > 0 else 0
        cash_on_cash = annual_rent * 0.6 / (prop['price'] * 0.8) * 100 if prop['price'] > 0 else 0
        monthly_cash_flow = prop['monthly_rent'] * 0.7 - (prop['price'] * 0.8 * 0.035 / 12)
        noi = annual_rent * 0.7
        price_per_sqm = prop['price'] / prop['size'] if prop['size'] > 0 else 0
        
        # Determine yield class
        if prop['yield'] >= 7:
            yield_class = 'high-yield'
        elif prop['yield'] >= 5:
            yield_class = 'medium-yield'
        else:
            yield_class = 'low-yield'
        
        html_template += f"""                <tr>
                    <td>{prop['location']}</td>
                    <td>€{prop['price']:,.0f}</td>
                    <td>{prop['size']:,.0f if prop['size'] > 0 else 'N/A'}</td>
                    <td>€{prop['monthly_rent']:,.0f}</td>
                    <td>€{annual_rent:,.0f}</td>
                    <td class="{yield_class}">{prop['yield']:.2f}%</td>
                    <td>{cap_rate:.2f}%</td>
                    <td>{cash_on_cash:.2f}%</td>
                    <td>€{monthly_cash_flow:,.0f}</td>
                    <td>€{price_per_sqm:,.0f if price_per_sqm > 0 else 'N/A'}</td>
                    <td>€{noi:,.0f}</td>
                    <td>{prop['comparable_count']}</td>
                </tr>
"""
    
    # Close HTML template
    html_template += """            </tbody>
        </table>
    </div>
</body>
</html>
"""
    
    # Save the HTML file
    with open(DASHBOARD_HTML, 'w', encoding='utf-8') as f:
        f.write(html_template)
    logger.info(f"Generated dashboard HTML at {DASHBOARD_HTML}")
    
    # Copy to UI directory for legacy support
    ui_dir = PROJECT_ROOT / "propbot" / "ui"
    ui_dir.mkdir(exist_ok=True)
    ui_html = ui_dir / "investment_dashboard.html"
    
    with open(ui_html, 'w') as f:
        f.write(html_template)
    logger.info(f"Copied dashboard to UI directory: {ui_html}")
    
    return True

if __name__ == "__main__":
    generate_dashboard() 