#!/usr/bin/env python3
"""
Update Investment Dashboard Script

This script adds a new column to the investment dashboard HTML file:
"Avg Price/m² (Neighborhood)" - pulled from the Neighborhood Price Comparison dashboard.

The script extracts neighborhood average price data from the neighborhood report,
then adds the corresponding values to each property row in the investment dashboard.
"""

import os
import re
import sys
from bs4 import BeautifulSoup
from datetime import datetime

# Define file paths
DASHBOARD_PATH = 'propbot/ui/investment_dashboard_latest.html'
NEIGHBORHOOD_REPORT_PATH = 'propbot/ui/neighborhood_report_updated.html'
OUTPUT_PATH = 'propbot/ui/investment_dashboard_updated.html'

def extract_neighborhood_data(html_path):
    """Extract neighborhood average price data from the neighborhood report."""
    neighborhood_prices = {}
    
    try:
        with open(html_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
            
        # Find the neighborhood table
        table = soup.find('table', {'id': 'neighborhoodTable'})
        if not table:
            print("Error: Could not find neighborhood table in the report.")
            return neighborhood_prices
            
        # Extract data from table rows
        rows = table.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                neighborhood = cells[0].text.strip()
                avg_price = cells[2].text.strip()
                neighborhood_prices[neighborhood] = avg_price
                
        print(f"Extracted price data for {len(neighborhood_prices)} neighborhoods.")
        return neighborhood_prices
        
    except Exception as e:
        print(f"Error extracting neighborhood data: {e}")
        return neighborhood_prices

def update_dashboard(dashboard_path, output_path, neighborhood_prices):
    """Update the investment dashboard with neighborhood average price data."""
    try:
        with open(dashboard_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Update the table headers - add new column after Price/m²
        headers = soup.find('thead').find('tr').find_all('th')
        if len(headers) < 7:
            print("Error: Could not find enough columns in the dashboard table.")
            return False
            
        # Insert new header after Price/m² (which is index 6)
        price_per_sqm_header = headers[6]
        new_header = soup.new_tag('th')
        new_header['onclick'] = "sortTable(7)"
        new_header.string = "Avg Price/m² (Neighborhood)"
        price_per_sqm_header.insert_after(new_header)
        
        # Update the onclick attributes for subsequent columns
        for i in range(7, len(headers)):
            headers[i]['onclick'] = f"sortTable({i+1})"
        
        # Update each row with neighborhood average price
        rows = soup.find('tbody').find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 7:
                neighborhood_cell = cells[4]
                neighborhood = neighborhood_cell.text.strip()
                
                # Create new cell for neighborhood average price
                new_cell = soup.new_tag('td')
                
                # Look up the neighborhood price
                if neighborhood in neighborhood_prices:
                    new_cell.string = neighborhood_prices[neighborhood]
                else:
                    new_cell.string = "N/A"
                
                # Insert after Price/m² cell
                cells[6].insert_after(new_cell)
        
        # Write updated HTML to output file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(str(soup))
            
        print(f"Successfully updated dashboard with neighborhood average prices.")
        print(f"Output saved to: {output_path}")
        return True
        
    except Exception as e:
        print(f"Error updating dashboard: {e}")
        return False

def main():
    print("Starting Investment Dashboard Update...")
    
    # Extract neighborhood average price data
    neighborhood_prices = extract_neighborhood_data(NEIGHBORHOOD_REPORT_PATH)
    if not neighborhood_prices:
        print("Error: Failed to extract neighborhood price data.")
        return False
        
    # Update dashboard with the new column
    success = update_dashboard(DASHBOARD_PATH, OUTPUT_PATH, neighborhood_prices)
    
    if success:
        print("Dashboard update completed successfully!")
    else:
        print("Dashboard update failed.")
        
    return success

if __name__ == "__main__":
    main() 