#!/usr/bin/env python3
import pandas as pd
import re
import os
from datetime import datetime
import numpy as np

# Change the input file to use our fixed metrics
#csv_file = "investment_summary_with_neighborhoods.csv"  # Original line
csv_file = "investment_summary_with_metrics_fixed.csv"  # Updated to use fixed metrics

print(f"Starting to update table rows with data from {csv_file}")

# Load the CSV data
df = pd.read_csv(csv_file)
print(f"Loaded CSV with {len(df)} rows")

# Create a mapping of property URLs to their row data
property_dict = {}
for _, row in df.iterrows():
    property_dict[row['Property URL']] = row
print(f"Created property mapping dictionary for {len(property_dict)} properties")

# HTML file path
html_file = "investment_summary.html"
print(f"Loading HTML file {html_file}")

# Create a backup of the HTML file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_file = f"{html_file}.bak_{timestamp}"
os.rename(html_file, backup_file)
print(f"Created backup of HTML file as {backup_file}")

# Load the HTML content from the backup
with open(backup_file, 'r', encoding='utf-8') as f:
    html_content = f.read()
print("Loaded HTML content")

# Update the header - replace the existing header with our improved one
header_pattern = r'<tr>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*<th[^>]*>.*?</th>\s*</tr>'
new_header = '''<tr>
<th>Property</th>
<th>Price (€)</th>
<th>Neighborhood</th>
<th>Size (sqm)</th>
<th>Monthly Rent (€)</th>
<th>Annual Rent (€)</th>
<th>Recurring Expenses (€)</th>
<th>NOI (€)</th>
<th>Cap Rate (%)</th>
<th>Gross Yield (%)</th>
<th>Price per sqm (€)</th>
<th>Classification</th>
</tr>'''
html_content = re.sub(header_pattern, new_header, html_content)
print("Updated table header")

# Update the statistics section with neighborhood counts
neighborhood_counts = df['Neighborhood'].value_counts()
neighborhoods_stats = f"{len(neighborhood_counts)} neighborhoods identified"
html_content = re.sub(r'<div id="stats">.*?</div>', f'<div id="stats">{neighborhoods_stats}</div>', html_content, flags=re.DOTALL)
print(f"Updated statistics section with {neighborhoods_stats}")

# Add some improved CSS styling for the statistics grid and table
style_pattern = r'<style>.*?</style>'
new_style = '''<style>
    body {
        font-family: Arial, sans-serif;
        margin: 0;
        padding: 20px;
        background-color: #f5f5f5;
    }
    h1 {
        color: #2c3e50;
        text-align: center;
        margin-bottom: 30px;
    }
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px;
        margin-bottom: 30px;
    }
    .stat-card {
        background-color: white;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    .stat-card h3 {
        margin-top: 0;
        color: #2c3e50;
    }
    .stat-card p {
        font-size: 24px;
        font-weight: bold;
        color: #3498db;
        margin: 10px 0 0;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        background-color: white;
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        border-radius: 8px;
        overflow: hidden;
    }
    th, td {
        padding: 12px 15px;
        text-align: left;
        border-bottom: 1px solid #ddd;
    }
    th {
        background-color: #3498db;
        color: white;
        position: sticky;
        top: 0;
    }
    tr:hover {
        background-color: #f1f1f1;
    }
    tr.high {
        background-color: #d4edda;
    }
    tr.medium {
        background-color: #fff3cd;
    }
    tr.low {
        background-color: #f8d7da;
    }
    .controls {
        margin-bottom: 20px;
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        align-items: center;
    }
    select, input {
        padding: 8px;
        border-radius: 4px;
        border: 1px solid #ddd;
    }
    .btn {
        display: inline-block;
        padding: 8px 16px;
        background-color: #3498db;
        color: white;
        border: none;
        border-radius: 4px;
        cursor: pointer;
        text-decoration: none;
    }
    .btn:hover {
        background-color: #2980b9;
    }
    .footer {
        margin-top: 30px;
        text-align: center;
        color: #7f8c8d;
    }
    #neighborhoodReport {
        text-align: center;
        margin: 20px 0;
    }
</style>'''
html_content = re.sub(style_pattern, new_style, html_content, flags=re.DOTALL)
print("Updated CSS styling")

# Rebuild the table body with our fixed metrics
table_body_pattern = r'<tbody>.*?</tbody>'
new_table_body = '<tbody>'

# Extract property rows and create updated rows
for _, row in df.iterrows():
    # Extract values
    property_url = row.get('Property URL', '')
    price = row.get('Price (€)', '')
    neighborhood = row.get('Neighborhood', 'Unknown')
    size = row.get('Size (sqm)', '')
    monthly_rent = row.get('Monthly Rent (€)', '')
    annual_rent = row.get('Annual Rent (€)', '')
    total_recurring_expenses = row.get('Total Recurring Expenses (€)', '')
    noi = row.get('NOI (€)', '')
    cap_rate = row.get('Cap Rate (%)', '')
    gross_yield = row.get('Gross Yield (%)', '')
    price_per_sqm = row.get('Price per sqm (€)', '')
    
    # Format values
    price_formatted = f"€{price:,.2f}" if pd.notna(price) and isinstance(price, (int, float)) else "€nan"
    neighborhood_formatted = neighborhood if pd.notna(neighborhood) else "Unknown"
    size_formatted = f"{size:,.2f} sqm" if pd.notna(size) and isinstance(size, (int, float)) else "nan sqm"
    monthly_rent_formatted = f"€{monthly_rent:,.2f}" if pd.notna(monthly_rent) and isinstance(monthly_rent, (int, float)) else "€nan"
    annual_rent_formatted = f"€{annual_rent:,.2f}" if pd.notna(annual_rent) and isinstance(annual_rent, (int, float)) else "€nan"
    recurring_expenses_formatted = f"€{total_recurring_expenses:,.2f}" if pd.notna(total_recurring_expenses) and isinstance(total_recurring_expenses, (int, float)) else "€nan"
    noi_formatted = f"€{noi:,.2f}" if pd.notna(noi) and isinstance(noi, (int, float)) else "€nan"
    cap_rate_formatted = f"{cap_rate:.2f}%" if pd.notna(cap_rate) and isinstance(cap_rate, (int, float)) else "nan%"
    gross_yield_formatted = f"{gross_yield:.2f}%" if pd.notna(gross_yield) and isinstance(gross_yield, (int, float)) else "nan%"
    price_per_sqm_formatted = f"€{price_per_sqm:,.2f}" if pd.notna(price_per_sqm) and isinstance(price_per_sqm, (int, float)) else "€nan"
    
    # Determine property classification based on cap rate
    classification = ""
    css_class = ""
    if pd.notna(cap_rate) and isinstance(cap_rate, (int, float)):
        if cap_rate >= 6.0:
            classification = "High Potential"
            css_class = "high"
        elif cap_rate >= 4.5:
            classification = "Medium Potential"
            css_class = "medium"
        else:
            classification = "Low Potential"
            css_class = "low"
    
    # Create the table row
    new_row = f'''<tr class="{css_class}" data-url="{property_url}" data-neighborhood="{neighborhood_formatted}">
<td><a href="{property_url}" target="_blank">View Property</a></td>
<td>{price_formatted}</td>
<td>{neighborhood_formatted}</td>
<td>{size_formatted}</td>
<td>{monthly_rent_formatted}</td>
<td>{annual_rent_formatted}</td>
<td>{recurring_expenses_formatted}</td>
<td>{noi_formatted}</td>
<td>{cap_rate_formatted}</td>
<td>{gross_yield_formatted}</td>
<td>{price_per_sqm_formatted}</td>
<td>{classification}</td>
</tr>'''
    new_table_body += new_row

new_table_body += '</tbody>'
html_content = re.sub(table_body_pattern, new_table_body, html_content, flags=re.DOTALL)
print("Rebuilt complete table body with all properties")

# Add a link to the neighborhood report
report_link_pattern = r'<div id="neighborhoodReport">.*?</div>'
new_report_link = '''<div id="neighborhoodReport">
<a href="neighborhood_report_updated.html" class="btn">View Detailed Neighborhood Analysis</a>
</div>'''
if re.search(report_link_pattern, html_content):
    html_content = re.sub(report_link_pattern, new_report_link, html_content, flags=re.DOTALL)
else:
    # If the div doesn't exist, add it before the table
    html_content = re.sub(r'<table id="propertyTable">', f'{new_report_link}\n<table id="propertyTable">', html_content)
print("Added link to neighborhood report")

# Add a reference to investment_filter.js if it doesn't exist
if '<script src="investment_filter.js"></script>' not in html_content:
    html_content = html_content.replace('</body>', '<script src="investment_filter.js"></script>\n</body>')
    print("Added reference to investment_filter.js")

# Write the updated HTML
with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html_content)
print(f"Successfully updated {html_file} with all investment metrics and improved styling") 