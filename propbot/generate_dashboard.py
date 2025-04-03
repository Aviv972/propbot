#!/usr/bin/env python3
"""
Generate a dashboard for visualizing property investment metrics
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import glob
import subprocess

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = SCRIPT_DIR / "data"
REPORTS_DIR = DATA_DIR / "reports"
UI_DIR = SCRIPT_DIR / "ui"

def get_latest_report():
    """Get the most recent investment summary report"""
    logger.info("Finding the latest investment summary report...")
    
    # Find all investment summary JSON files
    report_pattern = str(REPORTS_DIR / "investment_summary_*.json")
    report_files = glob.glob(report_pattern)
    
    if not report_files:
        logger.error(f"No investment summary reports found matching {report_pattern}")
        return None
    
    # Sort by modification time (most recent first)
    latest_report = max(report_files, key=os.path.getmtime)
    logger.info(f"Found latest report: {latest_report}")
    
    return latest_report

def load_investment_data(report_file):
    """Load investment data from the specified report file"""
    logger.info(f"Loading investment data from {report_file}")
    
    try:
        with open(report_file, 'r') as f:
            data = json.load(f)
        
        # Validate that data is a list of dictionaries
        if not isinstance(data, list):
            logger.error(f"Expected a list of properties, got {type(data)}")
            return []
        
        # Filter out non-dictionary items
        valid_properties = []
        for i, prop in enumerate(data):
            if not isinstance(prop, dict):
                logger.warning(f"Property at index {i} is not a dictionary: {type(prop)}")
                continue
            valid_properties.append(prop)
        
        if len(valid_properties) == 0:
            logger.error("No valid property dictionaries found in the data")
            return []
        
        logger.info(f"Filtered {len(data) - len(valid_properties)} invalid properties")
        data = valid_properties
        
        # Load additional data from sales_current.csv to get room_type and snapshot_date
        sales_file = DATA_DIR / "processed" / "sales_current.csv"
        if sales_file.exists():
            sales_df = pd.read_csv(sales_file)
            
            # Create a URL to data mapping
            sales_data = {row['url']: row for _, row in sales_df.iterrows()}
            
            # Enrich the investment data with room_type and snapshot_date
            for prop in data:
                url = prop.get('url', '')
                if url in sales_data:
                    prop['room_type'] = sales_data[url].get('room_type', '')
                    prop['snapshot_date'] = sales_data[url].get('snapshot_date', '')
                    
                    # Extract neighborhood from location
                    location = prop.get('location', '')
                    # Ensure location is a string
                    if not isinstance(location, str):
                        location = str(location) if location is not None else ''
                    neighborhood = location.split(', ')[-1] if location and ', ' in location else location
                    prop['neighborhood'] = neighborhood
                    
        logger.info(f"Loaded data for {len(data)} properties")
        return data
    except Exception as e:
        logger.error(f"Error loading investment data: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return []

def run_property_analysis():
    """Run the property analysis script"""
    try:
        logger.info("Running property analysis...")
        result = subprocess.run(
            ["python3", "-m", "propbot.run_investment_analysis"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Property analysis completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running property analysis: {str(e)}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def generate_html_dashboard(investment_data):
    """Generate an HTML dashboard from the investment data"""
    logger.info("Generating HTML dashboard...")
    
    if not investment_data:
        logger.error("No investment data to generate dashboard")
        return False
    
    # Calculate investment metrics
    if investment_data:
        # Filter out properties without valid rental estimates
        valid_properties = investment_data
        
        # Log how many properties would be filtered if we required valid rental estimates
        properties_without_rentals = [prop for prop in investment_data if prop.get('monthly_rent', 0) <= 0]
        filtered_count = len(properties_without_rentals)
        if filtered_count > 0:
            logger.info(f"Found {filtered_count} properties without valid rental estimates (showing all properties anyway)")
        
        # Use all properties for the dashboard
        investment_data = valid_properties
        
        # Calculate summaries
        total_properties = len(investment_data)
        
        # Only calculate averages if there are properties
        if total_properties > 0:
            # Get the current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Get rental data last update time
            rental_metadata_path = DATA_DIR / "processed" / "rental_metadata.json"
            rental_last_update = "Unknown"
            rental_update_frequency = "30 days"
            
            if rental_metadata_path.exists():
                try:
                    with open(rental_metadata_path, 'r') as f:
                        metadata = json.load(f)
                        rental_last_update_dt = datetime.fromisoformat(metadata.get('last_update', ''))
                        rental_last_update = rental_last_update_dt.strftime("%Y-%m-%d")
                        rental_update_frequency = metadata.get('update_frequency', '30 days')
                except Exception as e:
                    logger.warning(f"Error reading rental metadata: {str(e)}")
            
            # Convert to DataFrame for easier manipulation
            try:
                # Sort properties by snapshot_date (newest first)
                sorted_data = sorted(
                    investment_data,
                    key=lambda x: x.get('snapshot_date', ''),
                    reverse=True
                )
                
                df = pd.DataFrame(sorted_data)
                
                # Calculate statistics
                avg_price = df['price'].mean() if 'price' in df.columns else 0
                avg_size = df['size'].mean() if 'size' in df.columns else 0
                avg_cap_rate = df['cap_rate'].mean() if 'cap_rate' in df.columns else 0
                avg_cash_on_cash = df['coc_return'].mean() if 'coc_return' in df.columns else 0
                
                # Calculate gross yield for each property
                for prop in sorted_data:
                    annual_rent = prop.get('monthly_rent', 0) * 12
                    if annual_rent > 0 and prop.get('price', 0) > 0:
                        prop['gross_yield'] = (annual_rent / prop.get('price', 0)) * 100
                    else:
                        prop['gross_yield'] = 0
                
                # Recalculate DataFrame with gross yield
                df = pd.DataFrame(sorted_data)
                avg_gross_yield = df['gross_yield'].mean() if 'gross_yield' in df.columns else 0
                
                # Get the rows as HTML
                table_rows = ""
                for i, prop in enumerate(sorted_data):
                    url = prop.get('url', f"Property #{i+1}")
                    price = f"€{prop.get('price', 0):,.0f}"
                    size = f"{prop.get('size', 0)} m²"
                    room_type = prop.get('room_type', 'N/A')
                    neighborhood = prop.get('neighborhood', 'N/A')
                    monthly_rent = f"€{prop.get('monthly_rent', 0):,.0f}"
                    gross_yield = f"{prop.get('gross_yield', 0):.2f}%"
                    cap_rate = f"{prop.get('cap_rate', 0):.2f}%"
                    cash_on_cash = f"{prop.get('coc_return', 0):.2f}%"
                    monthly_cash_flow = f"€{prop.get('monthly_cash_flow', 0):,.0f}"
                    
                    # Calculate price per sqm
                    if prop.get('size', 0) > 0:
                        price_per_sqm = prop.get('price', 0) / prop.get('size', 0)
                        price_per_sqm_formatted = f"€{price_per_sqm:,.0f}"
                    else:
                        price_per_sqm_formatted = "N/A"
                    
                    table_rows += f"""
                    <tr>
                        <td><a href="{url}" target="_blank">{url.split('/')[-2]}</a></td>
                        <td>{price}</td>
                        <td>{size}</td>
                        <td>{room_type}</td>
                        <td>{neighborhood}</td>
                        <td>{monthly_rent}</td>
                        <td>{price_per_sqm_formatted}</td>
                        <td>{gross_yield}</td>
                        <td>{cap_rate}</td>
                        <td>{cash_on_cash}</td>
                        <td>{monthly_cash_flow}</td>
                    </tr>
                    """
                
            except Exception as e:
                logger.error(f"Error processing investment data: {str(e)}")
                avg_price = 0
                avg_size = 0
                avg_cap_rate = 0
                avg_cash_on_cash = 0
                avg_gross_yield = 0
                table_rows = "<tr><td colspan='11'>No valid property data available</td></tr>"
            
            # Create the HTML content
            html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>PropBot Investment Dashboard</title>
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
                        border-bottom: 1px solid var(--border-color);
                        padding: 1.5rem 2rem;
                        box-shadow: var(--shadow-sm);
                    }}
                    
                    .dashboard-header h1 {{
                        font-size: 1.75rem;
                        font-weight: 700;
                        color: var(--text-primary);
                        margin-bottom: 0.5rem;
                    }}
                    
                    .dashboard-info {{
                        display: flex;
                        justify-content: space-between;
                        align-items: center;
                        flex-wrap: wrap;
                        gap: 1rem;
                    }}
                    
                    .dashboard-info p {{
                        margin: 0;
                        color: var(--text-secondary);
                    }}
                    
                    .button-container {{
                        display: flex;
                        align-items: center;
                        gap: 0.75rem;
                    }}
                    
                    .button-help {{
                        font-size: 0.875rem;
                        color: var(--text-secondary);
                    }}
                    
                    /* Section Styles */
                    .dashboard-section {{
                        padding: 1.5rem 2rem;
                        margin-bottom: 1.5rem;
                        background-color: var(--card-bg);
                        border-radius: var(--radius);
                        box-shadow: var(--shadow);
                    }}
                    
                    h2 {{
                        font-size: 1.25rem;
                        font-weight: 600;
                        color: var(--text-primary);
                        margin-bottom: 1.25rem;
                        padding-bottom: 0.5rem;
                        border-bottom: 1px solid var(--border-color);
                    }}
                    
                    /* Cards Styles */
                    .summary-cards {{
                        display: grid;
                        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
                        gap: 1.25rem;
                        margin-bottom: 0.5rem;
                    }}
                    
                    .card {{
                        background-color: var(--card-bg);
                        border-radius: var(--radius);
                        box-shadow: var(--shadow);
                        padding: 1.25rem;
                        border: 1px solid var(--border-color);
                        transition: transform 0.2s, box-shadow 0.2s;
                    }}
                    
                    .card:hover {{
                        transform: translateY(-3px);
                        box-shadow: var(--shadow-lg);
                    }}
                    
                    .card-title {{
                        font-size: 0.875rem;
                        font-weight: 500;
                        color: var(--text-secondary);
                        margin-bottom: 0.5rem;
                    }}
                    
                    .card-value {{
                        font-size: 1.5rem;
                        font-weight: 700;
                        color: var(--primary-color);
                    }}
                    
                    /* Filters Styles */
                    .filters {{
                        display: grid;
                        grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
                        gap: 1rem;
                        margin-bottom: 1.5rem;
                        align-items: end;
                    }}
                    
                    .filter-group {{
                        display: flex;
                        flex-direction: column;
                        gap: 0.375rem;
                    }}
                    
                    .filter-label {{
                        font-size: 0.875rem;
                        font-weight: 500;
                        color: var(--text-secondary);
                    }}
                    
                    .filter-actions {{
                        display: flex;
                        gap: 0.5rem;
                        grid-column: 1 / -1;
                        justify-content: flex-end;
                        padding-top: 0.5rem;
                    }}
                    
                    select, input {{
                        padding: 0.5rem 0.75rem;
                        border: 1px solid var(--border-color);
                        border-radius: var(--radius);
                        font-family: inherit;
                        font-size: 0.875rem;
                        color: var(--text-primary);
                        background-color: var(--card-bg);
                        transition: border-color 0.2s, box-shadow 0.2s;
                    }}
                    
                    select:focus, input:focus {{
                        outline: none;
                        border-color: var(--primary-color);
                        box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1);
                    }}
                    
                    .dual-input {{
                        display: flex;
                        gap: 0.5rem;
                    }}
                    
                    .dual-input input {{
                        flex: 1;
                    }}
                    
                    button {{
                        padding: 0.5rem 1rem;
                        border: none;
                        border-radius: var(--radius);
                        font-family: inherit;
                        font-size: 0.875rem;
                        font-weight: 500;
                        cursor: pointer;
                        transition: background-color 0.2s;
                    }}
                    
                    button.primary {{
                        background-color: var(--primary-color);
                        color: white;
                    }}
                    
                    button.primary:hover {{
                        background-color: var(--primary-hover);
                    }}
                    
                    button.secondary {{
                        background-color: white;
                        color: var(--text-primary);
                        border: 1px solid var(--border-color);
                    }}
                    
                    button.secondary:hover {{
                        background-color: var(--background-color);
                    }}
                    
                    button.danger {{
                        background-color: var(--danger-color);
                        color: white;
                    }}
                    
                    button.danger:hover {{
                        background-color: var(--danger-hover);
                    }}
                    
                    /* Table Styles */
                    .table-container {{
                        overflow-x: auto;
                        border-radius: var(--radius);
                        box-shadow: var(--shadow);
                    }}
                    
                    table {{
                        width: 100%;
                        border-collapse: separate;
                        border-spacing: 0;
                        font-size: 0.875rem;
                    }}
                    
                    th, td {{
                        padding: 0.75rem 1rem;
                        text-align: left;
                    }}
                    
                    th {{
                        background-color: var(--background-color);
                        font-weight: 600;
                        color: var(--text-secondary);
                        position: sticky;
                        top: 0;
                        cursor: pointer;
                        border-bottom: 1px solid var(--border-color);
                        white-space: nowrap;
                    }}
                    
                    th:first-child {{
                        border-top-left-radius: var(--radius);
                    }}
                    
                    th:last-child {{
                        border-top-right-radius: var(--radius);
                    }}
                    
                    th:hover {{
                        background-color: #edf2f7;
                    }}
                    
                    td {{
                        border-bottom: 1px solid var(--border-color);
                    }}
                    
                    tr:last-child td:first-child {{
                        border-bottom-left-radius: var(--radius);
                    }}
                    
                    tr:last-child td:last-child {{
                        border-bottom-right-radius: var(--radius);
                    }}
                    
                    tr:hover td {{
                        background-color: rgba(37, 99, 235, 0.05);
                    }}
                    
                    /* Link Styles */
                    a {{
                        color: var(--primary-color);
                        text-decoration: none;
                        transition: color 0.2s;
                    }}
                    
                    a:hover {{
                        color: var(--primary-hover);
                        text-decoration: underline;
                    }}
                    
                    /* Timestamp Styles */
                    .timestamp {{
                        margin-top: 1.5rem;
                        text-align: right;
                        color: var(--text-secondary);
                        font-size: 0.75rem;
                    }}
                    
                    /* Utility Classes */
                    .positive {{
                        color: var(--success-color);
                    }}
                    
                    .negative {{
                        color: var(--danger-color);
                    }}
                    
                    /* Responsive adjustments */
                    @media (max-width: 768px) {{
                        .dashboard-header,
                        .dashboard-section {{
                            padding: 1rem;
                        }}
                        
                        .filters {{
                            grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
                        }}
                        
                        .summary-cards {{
                            grid-template-columns: repeat(auto-fill, minmax(150px, 1fr));
                        }}
                        
                        .card-value {{
                            font-size: 1.25rem;
                        }}
                    }}
                </style>
                <script>
                    // Sort table function
                    function sortTable(n) {{
                        var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                        table = document.getElementById("propertyTable");
                        switching = true;
                        // Set the sorting direction to ascending
                        dir = "asc";
                        
                        while (switching) {{
                            switching = false;
                            rows = table.rows;
                            
                            for (i = 1; i < (rows.length - 1); i++) {{
                                shouldSwitch = false;
                                x = rows[i].getElementsByTagName("TD")[n];
                                y = rows[i + 1].getElementsByTagName("TD")[n];
                                
                                // Check if the two rows should switch place
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
                        
                        // Update all headers to remove active class
                        var headers = table.getElementsByTagName("TH");
                        for (i = 0; i < headers.length; i++) {{
                            headers[i].classList.remove("active-sort");
                            headers[i].classList.remove("asc");
                            headers[i].classList.remove("desc");
                        }}
                        
                        // Add active class to the clicked header
                        headers[n].classList.add("active-sort");
                        headers[n].classList.add(dir);
                    }}
                    
                    // Filter function
                    function filterProperties() {{
                        // Get filter values
                        var minPrice = parseFloat(document.getElementById("minPrice").value) || 0;
                        var maxPrice = parseFloat(document.getElementById("maxPrice").value) || 10000000;
                        var minSize = parseFloat(document.getElementById("minSize").value) || 0;
                        var maxSize = parseFloat(document.getElementById("maxSize").value) || 10000;
                        var roomType = document.getElementById("roomType").value;
                        var neighborhood = document.getElementById("neighborhood").value;
                        var minCashFlow = parseFloat(document.getElementById("minCashFlow").value) || -10000;
                        var minGrossYield = parseFloat(document.getElementById("minGrossYield").value) || 0;
                        var minCapRate = parseFloat(document.getElementById("minCapRate").value) || 0;
                        var minPricePerSqm = parseFloat(document.getElementById("minPricePerSqm").value) || 0;
                        var maxPricePerSqm = parseFloat(document.getElementById("maxPricePerSqm").value) || 100000;
                        
                        // Get all rows in the table
                        var table = document.getElementById("propertyTable");
                        var rows = table.getElementsByTagName("tr");
                        var visibleCount = 0;
                        
                        // Loop through rows starting from index 1 (to skip header)
                        for (var i = 1; i < rows.length; i++) {{
                            var showRow = true;
                            var cells = rows[i].getElementsByTagName("td");
                            
                            if (cells.length > 0) {{
                                // Get values from cells
                                var price = parseFloat(cells[1].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                var size = parseFloat(cells[2].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                var rowRoomType = cells[3].textContent.trim();
                                var rowNeighborhood = cells[4].textContent.trim();
                                var grossYield = parseFloat(cells[7].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                var capRate = parseFloat(cells[8].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                var cashFlow = parseFloat(cells[10].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                var pricePerSqm = parseFloat(cells[6].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                
                                // Check each filter condition
                                if (price < minPrice || price > maxPrice) showRow = false;
                                if (size < minSize || size > maxSize) showRow = false;
                                if (roomType !== "all" && rowRoomType !== roomType) showRow = false;
                                if (neighborhood !== "all" && rowNeighborhood !== neighborhood) showRow = false;
                                if (cashFlow < minCashFlow) showRow = false;
                                if (grossYield < minGrossYield) showRow = false;
                                if (capRate < minCapRate) showRow = false;
                                if (pricePerSqm < minPricePerSqm || pricePerSqm > maxPricePerSqm) showRow = false;
                            }}
                            
                            // Show or hide the row
                            rows[i].style.display = showRow ? "" : "none";
                            if (showRow) visibleCount++;
                        }}
                        
                        // Update the results count
                        document.getElementById("resultsCount").textContent = visibleCount;
                    }}
                    
                    // Reset filters
                    function resetFilters() {{
                        document.getElementById("minPrice").value = "";
                        document.getElementById("maxPrice").value = "";
                        document.getElementById("minSize").value = "";
                        document.getElementById("maxSize").value = "";
                        document.getElementById("roomType").value = "all";
                        document.getElementById("neighborhood").value = "all";
                        document.getElementById("minCashFlow").value = "";
                        document.getElementById("minGrossYield").value = "";
                        document.getElementById("minCapRate").value = "";
                        document.getElementById("minPricePerSqm").value = "";
                        document.getElementById("maxPricePerSqm").value = "";
                        
                        // Show all rows
                        var table = document.getElementById("propertyTable");
                        var rows = table.getElementsByTagName("tr");
                        var count = 0;
                        
                        for (var i = 1; i < rows.length; i++) {{
                            rows[i].style.display = "";
                            count++;
                        }}
                        
                        // Update the results count
                        document.getElementById("resultsCount").textContent = count;
                    }}
                    
                    // Function to populate neighborhood and room type dropdowns
                    function populateFilters() {{
                        var table = document.getElementById("propertyTable");
                        var rows = table.getElementsByTagName("tr");
                        
                        var neighborhoods = new Set();
                        var roomTypes = new Set();
                        
                        // Start from index 1 to skip header
                        for (var i = 1; i < rows.length; i++) {{
                            var cells = rows[i].getElementsByTagName("td");
                            if (cells.length > 0) {{
                                neighborhoods.add(cells[4].textContent.trim());
                                roomTypes.add(cells[3].textContent.trim());
                            }}
                        }}
                        
                        // Populate neighborhood dropdown
                        var neighborhoodSelect = document.getElementById("neighborhood");
                        var neighborhoodArray = Array.from(neighborhoods);
                        neighborhoodArray.sort();
                        
                        neighborhoodArray.forEach(function(neighborhood) {{
                            if (neighborhood) {{
                                var option = document.createElement("option");
                                option.value = neighborhood;
                                option.text = neighborhood;
                                neighborhoodSelect.add(option);
                            }}
                        }});
                        
                        // Populate room type dropdown
                        var roomTypeSelect = document.getElementById("roomType");
                        var roomTypeArray = Array.from(roomTypes);
                        roomTypeArray.sort();
                        
                        roomTypeArray.forEach(function(roomType) {{
                            if (roomType && roomType !== "N/A") {{
                                var option = document.createElement("option");
                                option.value = roomType;
                                option.text = roomType;
                                roomTypeSelect.add(option);
                            }}
                        }});
                        
                        // Initialize counter
                        document.getElementById("resultsCount").textContent = rows.length - 1;
                    }}
                    
                    // Run analysis
                    function runAnalysis() {{
                        var button = document.getElementById("runAnalysisBtn");
                        button.disabled = true;
                        button.textContent = "Starting analysis...";
                        
                        fetch('http://localhost:9000/run-analysis', {{
                            method: 'POST',
                            headers: {{
                                'Content-Type': 'application/json'
                            }}
                        }})
                        .then(response => {{
                            if (response.ok) {{
                                return response.json();
                            }} else {{
                                throw new Error("Server responded with status: " + response.status);
                            }}
                        }})
                        .then(data => {{
                            if (data.success) {{
                                // Show the full workflow progress messages
                                button.textContent = "Running complete workflow...";
                                
                                // Create a status element below the button
                                var statusContainer = document.createElement('div');
                                statusContainer.id = "analysisStatus";
                                statusContainer.style.marginTop = "10px";
                                statusContainer.style.padding = "15px";
                                statusContainer.style.backgroundColor = "#f8f9fa";
                                statusContainer.style.borderRadius = "var(--radius)";
                                statusContainer.style.borderLeft = "4px solid var(--primary-color)";
                                
                                statusContainer.innerHTML = `
                                    <p style="font-weight: 500; margin-bottom: 10px;">Analysis in progress:</p>
                                    <ol style="margin-left: 20px; margin-bottom: 10px;">
                                        <li>Scraping new property listings...</li>
                                        <li>Processing and consolidating data</li>
                                        <li>Running rental and investment analysis</li>
                                        <li>Generating updated dashboard</li>
                                    </ol>
                                    <p>This process may take several minutes. The dashboard will automatically refresh when complete.</p>
                                    <p style="margin-top: 10px; font-size: 0.875rem; color: var(--text-secondary);">Last update: ${{new Date().toLocaleTimeString()}}</p>
                                `;
                                
                                // Insert status container after the button
                                button.parentNode.appendChild(statusContainer);
                                
                                // Set up a timer to refresh the page after some time
                                setTimeout(function() {{
                                    // Update button text
                                    button.textContent = "Complete Refresh";
                                    button.onclick = function() {{ location.reload(); }};
                                    button.disabled = false;
                                    
                                    // Update status message
                                    var statusElement = document.getElementById("analysisStatus");
                                    if (statusElement) {{
                                        statusElement.innerHTML += `
                                            <p style="color: var(--success-color); font-weight: 500; margin-top: 10px;">
                                                Analysis should be complete. Click "Complete Refresh" to load the latest data.
                                            </p>
                                        `;
                                    }}
                                }}, 180000); // 3 minutes (adjust based on typical processing time)
                            }} else {{
                                alert("Error starting analysis: " + (data.message || "Unknown error"));
                                button.disabled = false;
                                button.textContent = "Run Property Analysis";
                            }}
                        }})
                        .catch(error => {{
                            alert("Error running analysis: " + error);
                            button.disabled = false;
                            button.textContent = "Run Property Analysis";
                        }});
                    }}
                    
                    // Format cell values
                    function formatCells() {{
                        // Add color to positive/negative values
                        var table = document.getElementById("propertyTable");
                        var rows = table.getElementsByTagName("tr");
                        
                        for (var i = 1; i < rows.length; i++) {{
                            var cells = rows[i].getElementsByTagName("td");
                            if (cells.length > 0) {{
                                // Cash flow cell - index 10
                                var cashFlow = parseFloat(cells[10].textContent.replace(/[^0-9.-]/g, "")) || 0;
                                if (cashFlow > 0) {{
                                    cells[10].classList.add("positive");
                                }} else if (cashFlow < 0) {{
                                    cells[10].classList.add("negative");
                                }}
                            }}
                        }}
                    }}
                    
                    // Initialize on page load
                    window.onload = function() {{
                        populateFilters();
                        formatCells();
                        // Sort by price descending initially
                        sortTable(1);
                        sortTable(1);
                    }};
                </script>
            </head>
            <body>
                <div class="container">
                    <div class="dashboard-header">
                        <h1>PropBot Investment Dashboard</h1>
                        <div class="dashboard-info">
                            <p>
                                Analyzing <strong>{total_properties}</strong> properties in Lisbon • 
                                <span id="resultsCount">{total_properties}</span> properties shown
                            </p>
                            <div class="button-container">
                                <button id="runAnalysisBtn" onclick="runAnalysis()" class="primary">Run Property Analysis</button>
                            </div>
                        </div>
                    </div>
                    
                    <div class="dashboard-section">
                        <h2>Investment Overview</h2>
                        <div class="summary-cards">
                            <div class="card">
                                <div class="card-title">Average Property Price</div>
                                <div class="card-value">€{avg_price:,.0f}</div>
                            </div>
                            <div class="card">
                                <div class="card-title">Average Property Size</div>
                                <div class="card-value">{avg_size:.1f} m²</div>
                            </div>
                            <div class="card">
                                <div class="card-title">Average Gross Yield</div>
                                <div class="card-value">{avg_gross_yield:.2f}%</div>
                            </div>
                            <div class="card">
                                <div class="card-title">Average Cap Rate</div>
                                <div class="card-value">{avg_cap_rate:.2f}%</div>
                            </div>
                            <div class="card">
                                <div class="card-title">Average Cash on Cash Return</div>
                                <div class="card-value">{avg_cash_on_cash:.2f}%</div>
                            </div>
                        </div>
                        
                        <div style="margin-top: 1.5rem;">
                            <a href="neighborhood_report_latest.html" class="primary" style="display: inline-flex; align-items: center; gap: 0.5rem; padding: 0.5rem 1rem; background-color: var(--primary-color); color: white; border-radius: var(--radius); text-decoration: none;">
                                <span>View Neighborhood Analysis Report</span>
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M5 12h14M12 5l7 7-7 7"/>
                                </svg>
                            </a>
                        </div>
                    </div>
                    
                    <div class="dashboard-section">
                        <h2>Investment Properties</h2>
                        
                        <div class="filters">
                            <div class="filter-group">
                                <div class="filter-label">Price (€)</div>
                                <div class="dual-input">
                                    <input type="number" id="minPrice" placeholder="Min">
                                    <input type="number" id="maxPrice" placeholder="Max">
                                </div>
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Size (m²)</div>
                                <div class="dual-input">
                                    <input type="number" id="minSize" placeholder="Min">
                                    <input type="number" id="maxSize" placeholder="Max">
                                </div>
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Room Type</div>
                                <select id="roomType">
                                    <option value="all">All</option>
                                </select>
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Neighborhood</div>
                                <select id="neighborhood">
                                    <option value="all">All</option>
                                </select>
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Min Cash Flow (€)</div>
                                <input type="number" id="minCashFlow" placeholder="Min">
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Min Gross Yield (%)</div>
                                <input type="number" id="minGrossYield" placeholder="Min" step="0.1">
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Min Cap Rate (%)</div>
                                <input type="number" id="minCapRate" placeholder="Min" step="0.1">
                            </div>
                            
                            <div class="filter-group">
                                <div class="filter-label">Price/m² (€)</div>
                                <div class="dual-input">
                                    <input type="number" id="minPricePerSqm" placeholder="Min">
                                    <input type="number" id="maxPricePerSqm" placeholder="Max">
                                </div>
                            </div>
                            
                            <div class="filter-actions">
                                <button onclick="resetFilters()" class="secondary">Reset</button>
                                <button onclick="filterProperties()" class="primary">Apply Filters</button>
                            </div>
                        </div>
                        
                        <div class="table-container">
                            <table id="propertyTable">
                                <thead>
                                    <tr>
                                        <th onclick="sortTable(0)">Property</th>
                                        <th onclick="sortTable(1)">Price</th>
                                        <th onclick="sortTable(2)">Size</th>
                                        <th onclick="sortTable(3)">Rooms</th>
                                        <th onclick="sortTable(4)">Neighborhood</th>
                                        <th onclick="sortTable(5)">Monthly Rent</th>
                                        <th onclick="sortTable(6)">Price/m²</th>
                                        <th onclick="sortTable(7)">Gross Yield</th>
                                        <th onclick="sortTable(8)">Cap Rate</th>
                                        <th onclick="sortTable(9)">Cash on Cash</th>
                                        <th onclick="sortTable(10)">Monthly Cash Flow</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {table_rows}
                                </tbody>
                            </table>
                        </div>
                        
                        <div class="timestamp">
                            Generated on {timestamp}
                            <br>
                            <span>Rental data last updated: {rental_last_update} (updates every {rental_update_frequency})</span>
                        </div>
                    </div>
                </div>
                <div class="navbar">
                    <div class="navbar-brand">PropBot Investment Dashboard</div>
                    <div class="navbar-links">
                        <a href="neighborhood_report_updated.html" class="nav-link">Neighborhood Analysis</a>
                    </div>
                </div>
            </body>
            </html>
            """
            
            # Save the HTML file
            output_file = UI_DIR / "investment_dashboard_latest.html"
            with open(output_file, 'w') as f:
                f.write(html_content)
            
            logger.info(f"Dashboard saved to {output_file}")
            return output_file
        else:
            logger.info("No properties to generate dashboard")
            return False
    else:
        logger.info("No properties to generate dashboard")
        return False

def main():
    """Main entry point for the script"""
    logger.info("Starting dashboard generation...")
    
    # Run new property analysis by default
    skip_analysis = os.environ.get('SKIP_ANALYSIS', '').lower() == 'true'
    
    if not skip_analysis:
        success = run_property_analysis()
        if not success:
            logger.warning("Property analysis failed. Attempting to use existing data.")
    
    # Get the latest investment summary report
    report_file = get_latest_report()
    if not report_file:
        logger.error("No investment summary report found. Cannot generate dashboard.")
        return False
    
    # Load investment data
    investment_data = load_investment_data(report_file)
    if not investment_data:
        logger.error("Failed to load investment data. Cannot generate dashboard.")
        return False
    
    # Generate HTML dashboard
    dashboard_html = generate_html_dashboard(investment_data)
    if not dashboard_html:
        logger.error("Failed to generate dashboard HTML.")
        return False
    
    # Convert dashboard_html to string if it's a Path object
    if isinstance(dashboard_html, Path):
        try:
            with open(dashboard_html, 'r') as f:
                dashboard_content = f.read()
        except Exception as e:
            logger.error(f"Error reading dashboard HTML from path: {str(e)}")
            return False
    else:
        dashboard_content = dashboard_html
    
    # Write to files
    latest_dashboard_file = UI_DIR / "investment_dashboard_latest.html"
    updated_dashboard_file = UI_DIR / "investment_dashboard_updated.html"
    
    try:
        with open(latest_dashboard_file, 'w') as f:
            f.write(dashboard_content)
        logger.info(f"Saved latest dashboard to {latest_dashboard_file}")
        
        # Also create the updated dashboard file
        with open(updated_dashboard_file, 'w') as f:
            f.write(dashboard_content)
        logger.info(f"Saved updated dashboard to {updated_dashboard_file}")
        
        # Copy to the standard dashboard location for backward compatibility
        standard_dashboard_file = SCRIPT_DIR / "data" / "output" / "visualizations" / "investment_dashboard.html"
        os.makedirs(os.path.dirname(standard_dashboard_file), exist_ok=True)
        with open(standard_dashboard_file, 'w') as f:
            f.write(dashboard_content)
        logger.info(f"Copied dashboard to standard location: {standard_dashboard_file}")
        
        return True
    except Exception as e:
        logger.error(f"Error saving dashboard: {str(e)}")
        return False

if __name__ == "__main__":
    main() 