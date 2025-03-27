#!/usr/bin/env python3
import json
import pandas as pd
import os
import statistics
from datetime import datetime
import logging
from pathlib import Path
import numpy as np

# Import configuration
from propbot.config import PROCESSED_DATA_DIR, UI_DIR, NEIGHBORHOOD_REPORT_HTML

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Define paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR.parent / "data"
UI_DIR = SCRIPT_DIR.parent / "ui"

def read_csv_data(file_path=None):
    """Read property data from CSV file with neighborhood information."""
    if file_path is None:
        # Use default path from processed data directory
        file_path = os.path.join(PROCESSED_DATA_DIR, "sales_current.csv")
    
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} properties from {file_path}")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {str(e)}")
        return None

def calculate_neighborhood_stats(df):
    """Calculate statistics for each neighborhood."""
    # Extract neighborhood from location - use the LAST part of the location string, which is typically the neighborhood name
    df['Neighborhood'] = df['location'].apply(lambda x: x.split(',')[-1].strip() if isinstance(x, str) and ',' in x else (x if isinstance(x, str) else 'Unknown'))
    
    # Make sure we have the required columns
    required_columns = ['Neighborhood', 'price', 'size', 'price_per_sqm']
    for col in required_columns:
        if col not in df.columns:
            logging.error(f"Missing required column: {col}")
            return None
    
    # Filter out rows with missing data
    df = df.dropna(subset=required_columns)
    
    # Group by neighborhood
    neighborhood_stats = {}
    
    for neighborhood, group in df.groupby('Neighborhood'):
        if neighborhood == 'Unknown' or pd.isna(neighborhood):
            continue
            
        # Calculate statistics
        stats = {
            'property_count': len(group),
            'avg_price_per_sqm': group['price_per_sqm'].mean(),
            'median_price_per_sqm': group['price_per_sqm'].median(),
            'min_price_per_sqm': group['price_per_sqm'].min(),
            'max_price_per_sqm': group['price_per_sqm'].max(),
            'price_range': group['price_per_sqm'].max() - group['price_per_sqm'].min(),
            'std_deviation': group['price_per_sqm'].std(),
            'total_size_sqm': group['size'].sum()
        }
        neighborhood_stats[neighborhood] = stats
    
    logging.info(f"Calculated statistics for {len(neighborhood_stats)} neighborhoods")
    return neighborhood_stats

def save_neighborhood_stats(stats, file_path=None):
    """Save neighborhood statistics to JSON file."""
    if file_path is None:
        file_path = os.path.join(PROCESSED_DATA_DIR, "neighborhood_stats.json")
        
    try:
        # Convert numpy types to Python native types
        json_stats = {}
        for neighborhood, n_stats in stats.items():
            json_stats[neighborhood] = {
                k: float(v) if isinstance(v, (np.float64, np.float32, np.int64, np.int32)) else v
                for k, v in n_stats.items()
            }
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(json_stats, f, indent=2)
        logging.info(f"Saved neighborhood statistics to {file_path}")
        return True
    except Exception as e:
        logging.error(f"Error saving statistics: {str(e)}")
        return False

def format_currency(value):
    """Format a number as Euro currency."""
    return f"€{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

def format_number(value, decimals=2):
    """Format a number with specified decimals."""
    format_str = f"{value:,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return format_str

def generate_html_report(stats, output_file=None):
    """Generate HTML report from neighborhood statistics."""
    if output_file is None:
        output_file = NEIGHBORHOOD_REPORT_HTML
        
    if not stats:
        logging.error("No statistics provided")
        return False
    
    # Sort neighborhoods by average price per sqm (descending)
    sorted_neighborhoods = sorted(stats.items(), key=lambda x: x[1]['avg_price_per_sqm'], reverse=True)
    
    # Calculate city-wide average
    all_properties_count = sum(n['property_count'] for _, n in sorted_neighborhoods)
    city_avg_price_per_sqm = sum(float(n['avg_price_per_sqm']) * n['property_count'] for _, n in sorted_neighborhoods) / all_properties_count
    
    # Highest and lowest average neighborhoods
    highest_avg = sorted_neighborhoods[0]
    lowest_avg = sorted_neighborhoods[-1]
    
    html_content = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PropBot Neighborhood Price Analysis</title>
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
            <style>
            body { 
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                margin: 0;
                padding: 20px;
                background-color: #f5f5f5;
                    color: #333;
                line-height: 1.5;
            }
            .container {
                    max-width: 1200px;
                    margin: 0 auto;
                background-color: white;
                    padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }
            h1, h2 {
                color: #333;
                text-align: center;
                    margin-bottom: 30px;
            }
            .summary-boxes {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 20px;
                    margin-bottom: 30px;
            }
            .stat-box {
                background-color: #f8f9fa;
                padding: 20px;
                border-radius: 8px;
                    text-align: center;
                transition: transform 0.2s;
            }
            .stat-box:hover {
                transform: translateY(-2px);
            }
            .stat-label {
                color: #666;
                font-size: 0.9em;
                margin-bottom: 10px;
            }
            .stat-value {
                font-size: 1.5em;
                font-weight: 600;
                color: #333;
            }
            table {
                    width: 100%;
                    border-collapse: collapse;
                margin-top: 20px;
                background-color: white;
            }
            th, td {
                    padding: 12px;
                    text-align: left;
                border-bottom: 1px solid #ddd;
            }
            th {
                font-weight: 600;
                color: var(--text-secondary);
                background-color: #b8cce4; /* Light blue color for headers */
                position: sticky;
                top: 0;
                cursor: pointer;
            }
            tr:hover {
                background-color: #f5f5f5;
            }
            .price-bar {
                    height: 20px;
                background-color: #e9ecef;
                    border-radius: 10px;
                    position: relative;
                    overflow: hidden;
            }
            .price-indicator {
                    height: 100%;
                background-color: #007bff;
                    border-radius: 10px;
                transition: width 0.3s ease;
            }
            .footer {
                margin-top: 30px;
                    text-align: center;
                color: #666;
                font-size: 0.8em;
            }
            .nav-link {
                display: inline-block;
                margin: 20px 0;
                padding: 10px 20px;
                background-color: #007bff;
                color: white;
                text-decoration: none;
                border-radius: 5px;
                transition: background-color 0.2s;
            }
            .nav-link:hover {
                background-color: #0056b3;
            }
            @media (max-width: 768px) {
                .container {
                    padding: 10px;
                }
                th, td {
                    padding: 8px;
                }
                .stat-box {
                    padding: 15px;
                }
            }
            </style>
        </head>
        <body>
        <div class="container">
    ''' + f'''
            <h1>PropBot Neighborhood Price Analysis</h1>
            
            <a href="/" class="nav-link">← Back to Investment Dashboard</a>
        
            <div class="summary-boxes">
                    <div class="stat-box">
                        <div class="stat-label">Total Neighborhoods</div>
                        <div class="stat-value">{len(stats)}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Total Properties Analyzed</div>
                        <div class="stat-value">{all_properties_count}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">City-wide Average Price per sqm</div>
                        <div class="stat-value">{format_currency(city_avg_price_per_sqm)}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Highest Average (Neighborhood)</div>
                    <div class="stat-value">{format_currency(float(highest_avg[1]['avg_price_per_sqm']))}</div>
                        <div class="stat-label">{highest_avg[0]}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Lowest Average (Neighborhood)</div>
                    <div class="stat-value">{format_currency(float(lowest_avg[1]['avg_price_per_sqm']))}</div>
                        <div class="stat-label">{lowest_avg[0]}</div>
                </div>
            </div>
            
            <h2>Neighborhood Price Comparison</h2>
            <table>
                <tr>
                    <th>Neighborhood</th>
                    <th>Properties</th>
                    <th>Avg Price per sqm</th>
                    <th>Median Price per sqm</th>
                    <th>Min Price per sqm</th>
                    <th>Max Price per sqm</th>
                </tr>
        '''
    
    # Add rows for each neighborhood
    for neighborhood, n_stats in sorted_neighborhoods:
        html_content += f'''
                <tr>
                    <td>{neighborhood}</td>
                    <td>{n_stats['property_count']}</td>
                    <td>{format_currency(float(n_stats['avg_price_per_sqm']))}</td>
                    <td>{format_currency(float(n_stats['median_price_per_sqm']))}</td>
                    <td>{format_currency(float(n_stats['min_price_per_sqm']))}</td>
                    <td>{format_currency(float(n_stats['max_price_per_sqm']))}</td>
                </tr>
            '''
    
    html_content += '''
            </table>
            
            <h2>Price Range by Neighborhood</h2>
            <table>
                <tr>
                    <th>Neighborhood</th>
                    <th>Price Range (€/sqm)</th>
                    <th>Visualization</th>
                </tr>
    '''
    
    # Find maximum price per sqm for scaling
    max_price = max(float(n['max_price_per_sqm']) for _, n in sorted_neighborhoods)
    
    # Add price range visualization
    for neighborhood, n_stats in sorted_neighborhoods:
        min_pct = (float(n_stats['min_price_per_sqm']) / max_price) * 100
        max_pct = (float(n_stats['max_price_per_sqm']) / max_price) * 100
        range_width = max_pct - min_pct
        
        html_content += f'''
                <tr>
                    <td>{neighborhood}</td>
                    <td>{format_currency(float(n_stats['min_price_per_sqm']))} - {format_currency(float(n_stats['max_price_per_sqm']))}</td>
                    <td>
                        <div class="price-bar">
                            <div class="price-indicator" style="width: {range_width}%; margin-left: {min_pct}%"></div>
                        </div>
                    </td>
                </tr>
        '''
    
    html_content += f'''
            </table>
            
            <div class="footer">
                Report generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </div>
            </div>
        </body>
        </html>
    '''
    
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"Generated HTML report saved to {output_file}")
        return True
    except Exception as e:
        logging.error(f"Error generating HTML report: {str(e)}")
        return False

def generate_neighborhood_report():
    """Generate a report comparing neighborhoods based on property metrics"""
    logger.info("Starting neighborhood report generation")
    
    # Create backup of existing report
    backup_report()
    
    # Load sales data
    sales_file = DATA_DIR / "processed" / "sales_current.csv"
    if not sales_file.exists():
        logger.error(f"Sales data file not found: {sales_file}")
        return
    
    logger.info(f"Loading sales data from {sales_file}")
    sales_df = pd.read_csv(sales_file)
    
    # Extract neighborhood from location - use the LAST part of the location string, which is typically the neighborhood name
    sales_df['neighborhood'] = sales_df['location'].apply(lambda x: x.split(',')[-1].strip() if isinstance(x, str) and ',' in x else (x if isinstance(x, str) else 'Unknown'))
    
    # Calculate price per square meter for each property
    sales_df['price_per_sqm'] = sales_df['price'] / sales_df['size']
    
    # Group by neighborhood and calculate statistics
    neighborhood_stats = sales_df.groupby('neighborhood').agg({
        'price_per_sqm': ['count', 'mean', 'median', 'min', 'max']
    }).round(2)
    
    # Flatten column names 
    neighborhood_stats.columns = ['Properties', 'Avg Price per sqm', 'Median Price per sqm', 
                               'Min Price per sqm', 'Max Price per sqm']
    
    # Add the Neighborhood column explicitly
    neighborhood_stats = neighborhood_stats.reset_index().rename(columns={'neighborhood': 'Neighborhood'})
    
    # Sort by average price per square meter (descending)
    neighborhood_stats = neighborhood_stats.sort_values('Avg Price per sqm', ascending=False)
    
    # Save statistics to JSON for potential future use
    stats_file = DATA_DIR / "processed" / "neighborhood_stats.json"
    neighborhood_stats.to_json(stats_file)
    logger.info(f"Saved neighborhood statistics to {stats_file}")
    
    # Generate HTML report
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>PropBot Neighborhood Analysis</title>
        <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap">
        <style>
            :root {{
                --primary-color: #2563eb;
                --primary-hover: #1d4ed8;
                --secondary-color: #64748b;
                --accent-color: #f59e0b;
                --danger-color: #dc2626;
                --danger-hover: #b91c1c;
                --success-color: #16a34a;
                --background-color: #f8fafc;
                --card-bg: #ffffff;
                --border-color: #e2e8f0;
                --text-primary: #1e293b;
                --text-secondary: #64748b;
                --shadow-sm: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
                --shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
                --shadow-lg: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
                --radius: 8px;
            }}
            
            * {{
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }}
            
            body {{
                font-family: 'Inter', sans-serif;
                line-height: 1.6;
                color: var(--text-primary);
                background-color: var(--background-color);
                padding: 0;
            }}
            
            .container {{
                max-width: 1280px;
                margin: 0 auto;
                padding: 0;
            }}
            
            /* Header Styles */
            .dashboard-header {{
                background-color: var(--card-bg);
                padding: 2rem;
                border-radius: var(--radius);
                box-shadow: var(--shadow);
                margin-bottom: 2rem;
                display: flex;
                flex-direction: column;
                gap: 1rem;
            }}
            
            .dashboard-header h1 {{
                font-size: 1.75rem;
                font-weight: 700;
                color: var(--text-primary);
                margin-bottom: 0.5rem;
            }}
            
            .dashboard-header p {{
                color: var(--text-secondary);
                max-width: 700px;
            }}
            
            /* Stats Summary */
            .stats-summary {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                gap: 1.5rem;
                margin-bottom: 2rem;
            }}
            
            .stat-card {{
                background-color: var(--card-bg);
                border-radius: var(--radius);
                padding: 1.5rem;
                box-shadow: var(--shadow);
                display: flex;
                flex-direction: column;
                gap: 0.5rem;
            }}
            
            .stat-label {{
                font-size: 0.875rem;
                color: var(--text-secondary);
            }}
            
            .stat-value {{
                font-size: 1.5rem;
                font-weight: 600;
                color: var(--primary-color);
            }}
            
            /* Table Styles */
            .table-container {{
                background-color: var(--card-bg);
                border-radius: var(--radius);
                box-shadow: var(--shadow);
                overflow: hidden;
                margin-bottom: 2rem;
            }}
            
            .table-header {{
                padding: 1.5rem 2rem;
                border-bottom: 1px solid var(--border-color);
            }}
            
            .table-header h2 {{
                font-size: 1.25rem;
                font-weight: 600;
                color: var(--text-primary);
                margin: 0;
            }}
            
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            
            th, td {{
                padding: 1rem 1.5rem;
                text-align: left;
                border-bottom: 1px solid var(--border-color);
            }}
            
            th {{
                font-weight: 600;
                color: var(--text-secondary);
                background-color: #b8cce4; /* Light blue color for headers */
                position: sticky;
                top: 0;
                cursor: pointer;
            }}
            
            th:hover {{
                background-color: #edf2f7;
            }}
            
            tr:last-child td {{
                border-bottom: none;
            }}
            
            tr:hover td {{
                background-color: rgba(37, 99, 235, 0.05);
            }}
            
            /* Navigation */
            .navbar {{
                position: fixed;
                bottom: 0;
                left: 0;
                right: 0;
                background-color: var(--card-bg);
                box-shadow: 0 -2px 10px rgba(0, 0, 0, 0.05);
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 1rem 2rem;
                z-index: 100;
            }}
            
            .navbar-brand {{
                font-weight: 600;
                color: var(--text-primary);
            }}
            
            .navbar-links {{
                display: flex;
                gap: 1.5rem;
            }}
            
            .nav-link {{
                color: var(--text-secondary);
                text-decoration: none;
                font-size: 0.875rem;
                transition: color 0.2s;
            }}
            
            .nav-link:hover {{
                color: var(--primary-color);
            }}
            
            /* Footer */
            .timestamp {{
                text-align: right;
                color: var(--text-secondary);
                font-size: 0.75rem;
                margin-top: 1rem;
                margin-bottom: 4rem;
                padding: 0 1rem;
            }}
            
            /* Responsive adjustments */
            @media (max-width: 768px) {{
                .dashboard-header {{
                    padding: 1.5rem;
                }}
                
                .stats-summary {{
                    grid-template-columns: 1fr;
                }}
                
                th, td {{
                    padding: 0.75rem 1rem;
                }}
            }}
        </style>
        <script>
            function sortTable(n) {{
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById("neighborhoodTable");
                switching = true;
                dir = "asc";
                
                while (switching) {{
                    switching = false;
                    rows = table.rows;
                    
                    for (i = 1; i < (rows.length - 1); i++) {{
                        shouldSwitch = false;
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        
                        if (dir == "asc") {{
                            if (isNaN(x.innerHTML.replace(/[^0-9.-]/g, ""))) {{
                                if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {{
                                    shouldSwitch = true;
                                    break;
                                }}
                            }} else {{
                                if (parseFloat(x.innerHTML.replace(/[^0-9.-]/g, "")) > parseFloat(y.innerHTML.replace(/[^0-9.-]/g, ""))) {{
                                    shouldSwitch = true;
                                    break;
                                }}
                            }}
                        }} else if (dir == "desc") {{
                            if (isNaN(x.innerHTML.replace(/[^0-9.-]/g, ""))) {{
                                if (x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase()) {{
                                    shouldSwitch = true;
                                    break;
                                }}
                            }} else {{
                                if (parseFloat(x.innerHTML.replace(/[^0-9.-]/g, "")) < parseFloat(y.innerHTML.replace(/[^0-9.-]/g, ""))) {{
                                    shouldSwitch = true;
                                    break;
                                }}
                            }}
                        }}
                    }}
                    
                    if (shouldSwitch) {{
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount++;
                    }} else {{
                        if (switchcount == 0 && dir == "asc") {{
                            dir = "desc";
                            switching = true;
                        }}
                    }}
                }}
            }}
        </script>
    </head>
    <body>
        <div class="container">
            <div class="dashboard-header">
                <h1>Neighborhood Price Comparison</h1>
                <p>Analysis of property prices per square meter across different neighborhoods in Lisbon. This report helps identify investment opportunities by comparing average and median prices.</p>
            </div>
            
            <div class="stats-summary">
                <div class="stat-card">
                    <div class="stat-label">Total Neighborhoods</div>
                    <div class="stat-value">{len(neighborhood_stats)}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Total Properties</div>
                    <div class="stat-value">{neighborhood_stats['Properties'].sum()}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Highest Avg Price</div>
                    <div class="stat-value">{neighborhood_stats['Avg Price per sqm'].max().astype(float):.2f}€</div>
                    <div class="stat-label">{neighborhood_stats['Avg Price per sqm'].idxmax()}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Lowest Avg Price</div>
                    <div class="stat-value">{neighborhood_stats['Avg Price per sqm'].min().astype(float):.2f}€</div>
                    <div class="stat-label">{neighborhood_stats['Avg Price per sqm'].idxmin()}</div>
                </div>
            </div>
            
            <div class="table-container">
                <div class="table-header">
                    <h2>Neighborhood Price Comparison</h2>
                </div>
                <table id="neighborhoodTable">
                    <thead>
                        <tr>
                            <th onclick="sortTable(0)">Neighborhood</th>
                            <th onclick="sortTable(1)">Properties</th>
                            <th onclick="sortTable(2)">Avg Price per sqm</th>
                            <th onclick="sortTable(3)">Median Price per sqm</th>
                            <th onclick="sortTable(4)">Min Price per sqm</th>
                            <th onclick="sortTable(5)">Max Price per sqm</th>
                        </tr>
                    </thead>
                    <tbody>
                        <!-- Generate rows for each neighborhood, ensuring proper formatting -->
                        {create_neighborhood_table_rows(neighborhood_stats)}
                    </tbody>
                </table>
            </div>
            
            <div class="timestamp">
                Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </div>
        </div>
        
        <div class="navbar">
            <div class="navbar-brand">PropBot Investment Dashboard</div>
            <div class="navbar-links">
                <a href="/" class="nav-link">Back to Dashboard</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Save the HTML report
    output_file = UI_DIR / "neighborhood_report_updated.html"
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    logger.info(f"Generated HTML report saved to {output_file}")
    logger.info("Neighborhood report generated successfully")

def backup_report():
    """Create a backup of the existing report"""
    report_file = UI_DIR / "neighborhood_report_updated.html"
    if report_file.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = report_file.with_suffix(f".bak_{timestamp}")
        report_file.rename(backup_file)
        logger.info(f"Created backup of existing report as {backup_file}")

def create_neighborhood_table_rows(stats_df):
    """Generate HTML table rows for the neighborhood statistics."""
    html_rows = ""
    
    # Sort by Avg Price per sqm in descending order
    stats_df = stats_df.sort_values('Avg Price per sqm', ascending=False)
    
    # Format each row with proper Euro formatting
    for _, row in stats_df.iterrows():
        neighborhood = row['Neighborhood']
        properties = int(row['Properties'])
        avg_price = f"€{row['Avg Price per sqm']:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
        median_price = f"€{row['Median Price per sqm']:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
        min_price = f"€{row['Min Price per sqm']:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
        max_price = f"€{row['Max Price per sqm']:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')
        
        html_rows += f"""
        <tr>
            <td>{neighborhood}</td>
            <td>{properties}</td>
            <td>{avg_price}</td>
            <td>{median_price}</td>
            <td>{min_price}</td>
            <td>{max_price}</td>
        </tr>
        """
    
    return html_rows

def main():
    """Command-line entry point"""
    generate_neighborhood_report()

if __name__ == "__main__":
    main() 