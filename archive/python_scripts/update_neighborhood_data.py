#!/usr/bin/env python3
import json
import pandas as pd
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def extract_neighborhood(location_str):
    """Extract the neighborhood name from a location string."""
    if not location_str:
        return "Unknown"
    
    # Common neighborhoods in Lisbon - expanded list of 38 neighborhoods
    neighborhoods = [
        "Alfama", "Baixa", "Chiado", "Bairro Alto", "Príncipe Real", "Mouraria", 
        "Graça", "Belém", "Alcântara", "Lapa", "Estrela", "Parque das Nações",
        "Campo de Ourique", "Avenidas Novas", "Alvalade", "Areeiro", "Benfica",
        "Santo António", "Misericórdia", "Santa Maria Maior", "São Vicente", 
        "Lumiar", "Carnide", "Campolide", "Ajuda", "Penha de França",
        # Additional neighborhoods
        "Cais do Sodré", "Avenida da Liberdade", "Marquês de Pombal", "Saldanha",
        "Anjos", "Intendente", "Arroios", "Alameda", "Roma", "Martim Moniz",
        "Rossio", "Santa Clara", "Marvila", "Olivais", "São Domingos de Benfica", "Beato"
    ]
    
    # Check for direct neighborhood matches
    for neighborhood in neighborhoods:
        if neighborhood.lower() in location_str.lower():
            return neighborhood
    
    # Try to extract from common location format patterns
    parts = location_str.split(',')
    if len(parts) >= 2:
        # Often the neighborhood is the last part
        potential_neighborhood = parts[-1].strip()
        if potential_neighborhood in neighborhoods:
            return potential_neighborhood
        
        # Or second to last part for more detailed addresses
        if len(parts) >= 3:
            potential_neighborhood = parts[-2].strip()
            if potential_neighborhood in neighborhoods:
                return potential_neighborhood
    
    return "Unknown"

def load_json_data(json_file):
    """Load property data from the original JSON file."""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logging.info(f"Loaded {len(data)} properties from {json_file}")
        return data
    except Exception as e:
        logging.error(f"Error loading JSON data: {str(e)}")
        return None

def extract_and_update_neighborhoods(data, csv_file):
    """Extract neighborhoods from JSON data and update CSV file."""
    try:
        # Read CSV
        df = pd.read_csv(csv_file)
        logging.info(f"Loaded {len(df)} properties from {csv_file}")
        
        # Create URL to neighborhood mapping
        url_to_neighborhood = {}
        url_to_location = {}
        
        for item in data:
            url = item.get('url', '')
            location = item.get('location', '')
            if url and location:
                url_to_location[url] = location
                url_to_neighborhood[url] = extract_neighborhood(location)
        
        logging.info(f"Extracted neighborhoods for {len(url_to_neighborhood)} properties")
        
        # Update CSV with new neighborhoods
        updated_count = 0
        for idx, row in df.iterrows():
            property_url = row['Property URL']
            if property_url in url_to_neighborhood:
                df.at[idx, 'Neighborhood'] = url_to_neighborhood[property_url]
                updated_count += 1
        
        logging.info(f"Updated neighborhoods for {updated_count} properties")
        
        # Save updated CSV
        updated_csv = "investment_summary_with_neighborhoods_updated.csv"
        df.to_csv(updated_csv, index=False)
        logging.info(f"Saved updated CSV to {updated_csv}")
        
        # Get unique neighborhoods count
        unique_neighborhoods = sorted(df['Neighborhood'].unique())
        logging.info(f"Found {len(unique_neighborhoods)} unique neighborhoods")
        logging.info(f"Neighborhoods: {unique_neighborhoods}")
        
        return updated_csv, df
    except Exception as e:
        logging.error(f"Error updating neighborhoods: {str(e)}")
        return None, None

def calculate_neighborhood_stats(df):
    """Calculate statistics for each neighborhood."""
    # Make sure we have the required columns
    required_columns = ['Neighborhood', 'Price (€)', 'Size (sqm)', 'Price per sqm (€)']
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
            'avg_price_per_sqm': group['Price per sqm (€)'].mean(),
            'median_price_per_sqm': group['Price per sqm (€)'].median(),
            'min_price_per_sqm': group['Price per sqm (€)'].min(),
            'max_price_per_sqm': group['Price per sqm (€)'].max(),
            'price_range': group['Price per sqm (€)'].max() - group['Price per sqm (€)'].min(),
            'std_deviation': group['Price per sqm (€)'].std(),
            'total_size_sqm': group['Size (sqm)'].sum()
        }
        neighborhood_stats[neighborhood] = stats
    
    logging.info(f"Calculated statistics for {len(neighborhood_stats)} neighborhoods")
    return neighborhood_stats

def format_currency(value):
    """Format a number as Euro currency."""
    return f"€{value:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")

def format_number(value, decimals=2):
    """Format a number with specified decimals."""
    format_str = f"{value:,.{decimals}f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return format_str

def generate_html_report(stats, output_file="neighborhood_report_updated.html"):
    """Generate HTML report from neighborhood statistics."""
    if not stats:
        logging.error("No statistics provided")
        return False
    
    # Sort neighborhoods by average price per sqm (descending)
    sorted_neighborhoods = sorted(stats.items(), key=lambda x: x[1]['avg_price_per_sqm'], reverse=True)
    
    # Calculate city-wide average
    all_properties_count = sum(n['property_count'] for _, n in sorted_neighborhoods)
    city_avg_price_per_sqm = sum(n['avg_price_per_sqm'] * n['property_count'] for _, n in sorted_neighborhoods) / all_properties_count
    
    # Highest and lowest average neighborhoods
    highest_avg = sorted_neighborhoods[0]
    lowest_avg = sorted_neighborhoods[-1]
    
    html_content = f'''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Updated Neighborhood Analysis Report</title>
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                }}
                h1, h2 {{
                    color: #2c3e50;
                }}
                .summary {{
                    background-color: #f9f9f9;
                    border-radius: 5px;
                    padding: 20px;
                    margin-bottom: 30px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                }}
                .stats-container {{
                    display: flex;
                    flex-wrap: wrap;
                    gap: 20px;
                    margin-bottom: 30px;
                }}
                .stat-box {{
                    flex: 1;
                    min-width: 200px;
                    background-color: #f4f7f9;
                    border-radius: 5px;
                    padding: 15px;
                    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
                    text-align: center;
                }}
                .stat-value {{
                    font-size: 24px;
                    font-weight: bold;
                    color: #3498db;
                    margin: 10px 0;
                }}
                .stat-label {{
                    font-size: 14px;
                    color: #7f8c8d;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin-bottom: 30px;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                }}
                th {{
                    background-color: #3498db;
                    color: white;
                    font-weight: normal;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                tr:hover {{
                    background-color: #e3f2fd;
                }}
                .price-bar {{
                    height: 20px;
                    background-color: #ecf0f1;
                    border-radius: 10px;
                    position: relative;
                    overflow: hidden;
                    margin-top: 5px;
                }}
                .price-indicator {{
                    height: 100%;
                    background-color: #3498db;
                    border-radius: 10px;
                }}
                .chart-container {{
                    margin-top: 40px;
                }}
                .chart {{
                    width: 100%;
                    height: 400px;
                    margin-bottom: 40px;
                }}
                .footer {{
                    margin-top: 50px;
                    text-align: center;
                    font-size: 12px;
                    color: #95a5a6;
                }}
            </style>
        </head>
        <body>
            <h1>Updated Neighborhood Analysis Report</h1>
            
            <div class="summary">
                <h2>Summary</h2>
                <div class="stats-container">
        
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
                        <div class="stat-value">{format_currency(highest_avg[1]['avg_price_per_sqm'])}</div>
                        <div class="stat-label">{highest_avg[0]}</div>
                    </div>
                    <div class="stat-box">
                        <div class="stat-label">Lowest Average (Neighborhood)</div>
                        <div class="stat-value">{format_currency(lowest_avg[1]['avg_price_per_sqm'])}</div>
                        <div class="stat-label">{lowest_avg[0]}</div>
                    </div>
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
                    <th>Price Range</th>
                    <th>Total Size (sqm)</th>
                </tr>
        '''
    
    # Add rows for each neighborhood
    for neighborhood, n_stats in sorted_neighborhoods:
        html_content += f'''
                <tr>
                    <td>{neighborhood}</td>
                    <td>{n_stats['property_count']}</td>
                    <td>{format_currency(n_stats['avg_price_per_sqm'])}</td>
                    <td>{format_currency(n_stats['median_price_per_sqm'])}</td>
                    <td>{format_currency(n_stats['min_price_per_sqm'])}</td>
                    <td>{format_currency(n_stats['max_price_per_sqm'])}</td>
                    <td>{format_currency(n_stats['price_range'])}</td>
                    <td>{format_number(n_stats['total_size_sqm'], 2)} sqm</td>
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
    max_price = max(n['max_price_per_sqm'] for _, n in sorted_neighborhoods)
    
    # Add price range visualization
    for neighborhood, n_stats in sorted_neighborhoods:
        min_pct = (n_stats['min_price_per_sqm'] / max_price) * 100
        max_pct = (n_stats['max_price_per_sqm'] / max_price) * 100
        range_width = max_pct - min_pct
        
        html_content += f'''
                <tr>
                    <td>{neighborhood}</td>
                    <td>{format_currency(n_stats['min_price_per_sqm'])} - {format_currency(n_stats['max_price_per_sqm'])}</td>
                    <td>
                        <div class="price-bar">
                            <div class="price-indicator" style="width: {range_width}%; margin-left: {min_pct}%"></div>
                        </div>
                    </td>
                </tr>
        '''
    
    html_content += '''
            </table>
            
            <div class="footer">
                Report generated on ''' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '''
            </div>
        </body>
        </html>
    '''
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"Generated HTML report saved to {output_file}")
        return True
    except Exception as e:
        logging.error(f"Error generating HTML report: {str(e)}")
        return False

def main():
    """Main function to update neighborhoods and generate report."""
    logging.info("Starting neighborhood update and report generation")
    
    # Load JSON data
    json_file = "idealista_listings.json"
    data = load_json_data(json_file)
    if not data:
        logging.error("Failed to load property data")
        return
    
    # Update neighborhoods in CSV
    csv_file = "investment_summary_with_neighborhoods.csv.bak_20250318_225001.csv"
    updated_csv, updated_df = extract_and_update_neighborhoods(data, csv_file)
    if not updated_csv or updated_df is None:
        logging.error("Failed to update neighborhoods")
        return
    
    # Calculate neighborhood statistics
    stats = calculate_neighborhood_stats(updated_df)
    if not stats:
        logging.error("Failed to calculate neighborhood statistics")
        return
    
    # Save stats to JSON file
    with open("neighborhood_stats_updated.json", 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2)
    logging.info("Saved updated neighborhood statistics to neighborhood_stats_updated.json")
    
    # Generate HTML report
    if generate_html_report(stats):
        logging.info("Updated neighborhood report generated successfully")
        # Add link to original html file
        try:
            with open("neighborhood_report.html", 'r') as original_file:
                original_content = original_file.read()
            
            # Add link to the updated report
            if '<div class="footer">' in original_content:
                modified_content = original_content.replace(
                    '<div class="footer">',
                    '<div style="text-align: center; margin: 20px 0;"><a href="neighborhood_report_updated.html" style="display: inline-block; padding: 10px 20px; background-color: #3498db; color: white; text-decoration: none; border-radius: 5px;">View Updated Report with All Neighborhoods</a></div>\n<div class="footer">'
                )
                
                with open("neighborhood_report.html", 'w') as original_file:
                    original_file.write(modified_content)
                
                logging.info("Added link to updated report in the original report")
            
        except Exception as e:
            logging.error(f"Error updating original report: {str(e)}")
    else:
        logging.error("Failed to generate updated neighborhood report")

if __name__ == "__main__":
    main() 