#!/usr/bin/env python3
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json

def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def load_investment_summary(file_path="investment_summary_with_neighborhoods_updated.csv"):
    """Load the investment summary CSV data."""
    try:
        log_message(f"Loading investment summary from {file_path}")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            log_message(f"Successfully loaded {len(df)} properties")
            return df
        else:
            log_message(f"File {file_path} does not exist")
            return None
    except Exception as e:
        log_message(f"Error loading investment summary: {e}")
        return None

def validate_metrics(df):
    """Recalculate and validate key investment metrics."""
    log_message("Validating investment metrics...")
    
    # Create dataframe to store validation results
    validation_results = pd.DataFrame()
    validation_results['Property URL'] = df['Property URL']
    
    # Count properties with missing values for key metrics
    missing_price = df['Price (€)'].isna().sum()
    missing_size = df['Size (sqm)'].isna().sum()
    missing_rent = df['Monthly Rent (€)'].isna().sum()
    
    log_message(f"Properties with missing price: {missing_price}")
    log_message(f"Properties with missing size: {missing_size}")
    log_message(f"Properties with missing monthly rent: {missing_rent}")
    
    # Recalculate Annual Rent
    validation_results['Annual Rent (Original)'] = df['Annual Rent (€)']
    validation_results['Annual Rent (Calculated)'] = df['Monthly Rent (€)'] * 12
    validation_results['Annual Rent Difference'] = validation_results['Annual Rent (Original)'] - validation_results['Annual Rent (Calculated)']
    annual_rent_discrepancies = (abs(validation_results['Annual Rent Difference']) > 1).sum()
    log_message(f"Annual Rent discrepancies found: {annual_rent_discrepancies}")
    
    # Recalculate Gross Yield
    validation_results['Gross Yield (Original)'] = df['Gross Yield (%)']
    # Calculate gross yield: (Annual Rent / Price) * 100
    validation_results['Gross Yield (Calculated)'] = (df['Annual Rent (€)'] / df['Price (€)']) * 100
    validation_results['Gross Yield Difference'] = validation_results['Gross Yield (Original)'] - validation_results['Gross Yield (Calculated)']
    yield_discrepancies = (abs(validation_results['Gross Yield Difference']) > 0.1).sum()
    log_message(f"Gross Yield discrepancies found: {yield_discrepancies}")
    
    # Recalculate Price per sqm
    validation_results['Price per sqm (Original)'] = df['Price per sqm (€)']
    validation_results['Price per sqm (Calculated)'] = df['Price (€)'] / df['Size (sqm)']
    validation_results['Price per sqm Difference'] = validation_results['Price per sqm (Original)'] - validation_results['Price per sqm (Calculated)']
    price_per_sqm_discrepancies = (abs(validation_results['Price per sqm Difference']) > 1).sum()
    log_message(f"Price per sqm discrepancies found: {price_per_sqm_discrepancies}")
    
    # Recalculate NOI (Net Operating Income)
    validation_results['NOI (Original)'] = df['NOI (€)']
    validation_results['NOI (Calculated)'] = df['Annual Rent (€)'] - df['Total Recurring Expenses (€)']
    validation_results['NOI Difference'] = validation_results['NOI (Original)'] - validation_results['NOI (Calculated)']
    noi_discrepancies = (abs(validation_results['NOI Difference']) > 1).sum()
    log_message(f"NOI discrepancies found: {noi_discrepancies}")
    
    # Recalculate Cap Rate
    validation_results['Cap Rate (Original)'] = df['Cap Rate (%)']
    validation_results['Cap Rate (Calculated)'] = (df['NOI (€)'] / df['Price (€)']) * 100
    validation_results['Cap Rate Difference'] = validation_results['Cap Rate (Original)'] - validation_results['Cap Rate (Calculated)']
    cap_rate_discrepancies = (abs(validation_results['Cap Rate Difference']) > 0.1).sum()
    log_message(f"Cap Rate discrepancies found: {cap_rate_discrepancies}")
    
    # Calculate total discrepancies
    total_discrepancies = annual_rent_discrepancies + yield_discrepancies + price_per_sqm_discrepancies + noi_discrepancies + cap_rate_discrepancies
    log_message(f"Total discrepancies found: {total_discrepancies}")
    
    # Display top discrepancies
    if total_discrepancies > 0:
        log_message("\nTop discrepancies:")
        # Combine all differences and get properties with largest discrepancies
        validation_results['Total Absolute Difference'] = (
            abs(validation_results['Annual Rent Difference']) +
            abs(validation_results['Gross Yield Difference']) +
            abs(validation_results['Price per sqm Difference'] / 100) + # Scale to be comparable
            abs(validation_results['NOI Difference'] / 100) + # Scale to be comparable
            abs(validation_results['Cap Rate Difference'])
        )
        
        top_discrepancies = validation_results.nlargest(5, 'Total Absolute Difference')
        for idx, row in top_discrepancies.iterrows():
            log_message(f"\nProperty: {row['Property URL']}")
            log_message(f"  Annual Rent: {row['Annual Rent (Original)']} vs {row['Annual Rent (Calculated)']} (diff: {row['Annual Rent Difference']})")
            log_message(f"  Gross Yield: {row['Gross Yield (Original)']}% vs {row['Gross Yield (Calculated)']}% (diff: {row['Gross Yield Difference']}%)")
            log_message(f"  Price per sqm: {row['Price per sqm (Original)']} vs {row['Price per sqm (Calculated)']} (diff: {row['Price per sqm Difference']})")
            log_message(f"  NOI: {row['NOI (Original)']} vs {row['NOI (Calculated)']} (diff: {row['NOI Difference']})")
            log_message(f"  Cap Rate: {row['Cap Rate (Original)']}% vs {row['Cap Rate (Calculated)']}% (diff: {row['Cap Rate Difference']}%)")
    
    # Save validation results
    validation_file = "metric_validation_results.csv"
    validation_results.to_csv(validation_file, index=False)
    log_message(f"Validation results saved to {validation_file}")
    
    return validation_results

def fix_metrics(df):
    """Fix any incorrect metrics in the dataset."""
    log_message("Fixing incorrect metrics...")
    
    # Fix Annual Rent
    df['Annual Rent (€)'] = df['Monthly Rent (€)'] * 12
    
    # Fix Gross Yield
    df['Gross Yield (%)'] = (df['Annual Rent (€)'] / df['Price (€)']) * 100
    
    # Fix Price per sqm
    df['Price per sqm (€)'] = df['Price (€)'] / df['Size (sqm)']
    
    # Fix NOI (Net Operating Income)
    df['NOI (€)'] = df['Annual Rent (€)'] - df['Total Recurring Expenses (€)']
    
    # Fix Cap Rate
    df['Cap Rate (%)'] = (df['NOI (€)'] / df['Price (€)']) * 100
    
    # Clean up NaN values
    df = df.replace([np.inf, -np.inf], np.nan)
    
    log_message(f"Fixed metrics for {len(df)} properties")
    return df

def save_fixed_data(df, output_file="investment_summary_with_metrics_fixed.csv"):
    """Save the fixed dataset to a CSV file."""
    try:
        # Create a backup of the original file
        if os.path.exists(output_file):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"{output_file}.bak_{timestamp}"
            os.rename(output_file, backup_file)
            log_message(f"Created backup of existing file as {backup_file}")
        
        # Save the fixed dataset
        df.to_csv(output_file, index=False)
        log_message(f"Saved fixed metrics to {output_file}")
        return True
    except Exception as e:
        log_message(f"Error saving fixed data: {e}")
        return False

def main():
    """Main function to validate and fix investment metrics."""
    log_message("Starting investment metrics validation...")
    
    # Load the investment summary data
    df = load_investment_summary()
    if df is None:
        log_message("Exiting due to error loading data")
        return False
    
    # Validate metrics
    validation_results = validate_metrics(df)
    
    # Ask user if they want to fix discrepancies
    user_input = input("\nWould you like to fix discrepancies and generate a corrected file? (y/n): ")
    if user_input.lower() == 'y':
        fixed_df = fix_metrics(df)
        if save_fixed_data(fixed_df):
            log_message("Process completed successfully with fixes")
            return True
    else:
        log_message("Process completed without making fixes")
        return True

if __name__ == "__main__":
    main() 