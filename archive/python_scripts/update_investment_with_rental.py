#!/usr/bin/env python3
import pandas as pd
import json
import os
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

def load_rental_report(report_file):
    """Load the rental income report."""
    try:
        if report_file.endswith('.json'):
            with open(report_file, 'r') as f:
                data = json.load(f)
                # Convert to a list of dictionaries with url as key
                properties = []
                for url, details in data.items():
                    details['url'] = url
                    properties.append(details)
                return properties
        elif report_file.endswith('.csv'):
            df = pd.read_csv(report_file)
            return df.to_dict('records')
        else:
            logging.error(f"Unsupported file format: {report_file}")
            return []
    except Exception as e:
        logging.error(f"Error loading rental report: {e}")
        return []

def load_investment_summary(file_path):
    """Load the investment summary CSV."""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        logging.error(f"Error loading investment summary: {e}")
        return None

def update_investment_summary(investment_df, rental_data):
    """Update investment summary with rental data."""
    # Create a dictionary of rental data by URL
    rental_dict = {item['url']: item for item in rental_data}
    
    # Count how many properties were updated
    updated_count = 0
    
    # Update each row in the investment summary
    for index, row in investment_df.iterrows():
        property_url = row['Property URL']
        
        if property_url in rental_dict:
            rental_info = rental_dict[property_url]
            
            # Update monthly rent
            if 'estimated_monthly_rent' in rental_info and rental_info['estimated_monthly_rent'] > 0:
                investment_df.at[index, 'Monthly Rent (€)'] = rental_info['estimated_monthly_rent']
                
                # Update annual rent
                investment_df.at[index, 'Annual Rent (€)'] = rental_info['estimated_annual_rent']
                
                # If gross_rental_yield is provided, update that too
                if 'gross_rental_yield' in rental_info:
                    investment_df.at[index, 'Gross Yield (%)'] = rental_info['gross_rental_yield']
                
                updated_count += 1
    
    logging.info(f"Updated {updated_count} properties in the investment summary")
    return investment_df

def save_investment_summary(df, output_file):
    """Save the updated investment summary."""
    try:
        df.to_csv(output_file, index=False)
        logging.info(f"Successfully saved updated investment summary")
    except Exception as e:
        logging.error(f"Error saving investment summary: {e}")

def update_html():
    """Update the HTML file with the updated investment summary."""
    try:
        logging.info("Updating HTML file with update_rows_fixed.py")
        # Call the other script to update the HTML file
        os.system("python3 update_rows_fixed.py")
        logging.info("Successfully updated HTML file")
    except Exception as e:
        logging.error(f"Error updating HTML file: {e}")

def main():
    """Main function to run the update process."""
    logging.info("Starting investment summary update process")
    
    # File paths
    rental_report = "rental_income_report_improved_40.csv"
    investment_summary = "investment_summary_with_neighborhoods.csv"
    
    # Create a backup of the investment summary
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"{investment_summary}.bak_{timestamp}.csv"
    os.system(f"cp {investment_summary} {backup_file}")
    logging.info(f"Created backup of investment summary as {backup_file}")
    
    # Load data
    rental_data = load_rental_report(rental_report)
    logging.info(f"Loaded rental income report with {len(rental_data)} properties")
    
    investment_df = load_investment_summary(investment_summary)
    logging.info(f"Loaded investment summary with {len(investment_df)} properties")
    
    if rental_data and investment_df is not None:
        # Update the investment summary
        updated_df = update_investment_summary(investment_df, rental_data)
        
        # Save the updated investment summary
        save_investment_summary(updated_df, investment_summary)
        
        # Update the HTML file
        update_html()
        
        logging.info("Investment summary update completed successfully")
    else:
        logging.error("Failed to update investment summary due to data loading errors")

if __name__ == "__main__":
    main() 