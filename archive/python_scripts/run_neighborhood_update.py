#!/usr/bin/env python3
import json
import pandas as pd
import re
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

def load_property_data(json_file):
    """Load property data from JSON file."""
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Format into a list of property dictionaries with neighborhoods
        property_data = []
        for item in data:
            url = item.get('url', '')
            location = item.get('location', '')
            
            # Extract neighborhood from location
            neighborhood = extract_neighborhood(location)
            
            property_data.append({
                'url': url,
                'location': location,
                'neighborhood': neighborhood
            })
        
        logging.info(f"Loaded {len(property_data)} properties from {json_file}")
        return property_data
    except Exception as e:
        logging.error(f"Error loading property data: {str(e)}")
        return []

def update_neighborhoods(csv_file, property_data):
    """Update neighborhoods in the CSV file."""
    try:
        # Load CSV
        df = pd.read_csv(csv_file)
        logging.info(f"Loaded CSV with {len(df)} rows")
        
        # Create a dictionary mapping URLs to neighborhoods
        neighborhood_dict = {p['url']: p['neighborhood'] for p in property_data if p['url']}
        
        # Count of updated properties
        updated_count = 0
        
        # Update neighborhoods
        for index, row in df.iterrows():
            property_url = row['Property URL']
            if property_url in neighborhood_dict:
                # Only update if the current value is "Unknown" or empty or we have a better match
                current_neighborhood = row.get('Neighborhood', '')
                if pd.isna(current_neighborhood) or current_neighborhood == 'Unknown' or current_neighborhood == '':
                    df.at[index, 'Neighborhood'] = neighborhood_dict[property_url]
                    updated_count += 1
        
        logging.info(f"Updated neighborhoods for {updated_count} properties")
        return df
    except Exception as e:
        logging.error(f"Error updating neighborhoods: {str(e)}")
        return None

def main():
    """Main function."""
    logging.info("Starting neighborhood update process")
    
    # File paths
    json_file = "idealista_listings.json"
    csv_file = "investment_summary_with_neighborhoods.csv"
    
    # Create a backup of the CSV file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{csv_file}.bak_{timestamp}.csv"
    os.system(f"cp {csv_file} {backup_file}")
    logging.info(f"Created backup of CSV file as {backup_file}")
    
    # Load property data
    property_data = load_property_data(json_file)
    
    if property_data:
        # Update neighborhoods
        updated_df = update_neighborhoods(csv_file, property_data)
        
        if updated_df is not None:
            # Save updated CSV
            updated_df.to_csv(csv_file, index=False)
            logging.info(f"Saved updated CSV to {csv_file}")
            
            # Update HTML file with updated neighborhoods
            logging.info("Updating HTML file with update_rows_fixed.py")
            os.system("python3 update_rows_fixed.py")
            logging.info("Successfully updated HTML file")
            
            # Count and print unique neighborhoods
            unique_neighborhoods = updated_df['Neighborhood'].unique()
            logging.info(f"Number of unique neighborhoods: {len(unique_neighborhoods)}")
            logging.info(f"Neighborhoods: {sorted(unique_neighborhoods)}")
            
            logging.info("Neighborhood update completed successfully")
        else:
            logging.error("Failed to update neighborhoods")
    else:
        logging.error("Failed to load property data")

if __name__ == "__main__":
    main() 