#!/usr/bin/env python3
import pandas as pd
from datetime import datetime

def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def verify_fixed_metrics(fixed_file="investment_summary_with_metrics_fixed.csv"):
    """Verify that metrics are calculated correctly in the fixed CSV file."""
    try:
        log_message(f"Loading fixed data from {fixed_file}")
        df = pd.read_csv(fixed_file)
        log_message(f"Loaded {len(df)} properties")
        
        # Calculate metrics from scratch
        log_message("Verifying metric calculations...")
        
        # Select sample properties
        sample_size = min(5, len(df))
        sample_indices = range(sample_size)
        
        all_correct = True
        for i in sample_indices:
            sample = df.iloc[i]
            log_message(f"\n=== PROPERTY {i+1}: {sample['Property URL']} ===")
            
            # Verify Annual Rent
            expected_annual = sample['Monthly Rent (€)'] * 12
            actual_annual = sample['Annual Rent (€)']
            annual_match = abs(expected_annual - actual_annual) < 1
            log_message(f"Annual Rent: {expected_annual:.2f}€ vs {actual_annual:.2f}€ - {'✓' if annual_match else '✗'}")
            if not annual_match:
                all_correct = False
            
            # Verify NOI
            expected_noi = sample['Annual Rent (€)'] - sample['Total Recurring Expenses (€)']
            actual_noi = sample['NOI (€)']
            noi_match = abs(expected_noi - actual_noi) < 1
            log_message(f"NOI: {expected_noi:.2f}€ vs {actual_noi:.2f}€ - {'✓' if noi_match else '✗'}")
            if not noi_match:
                all_correct = False
            
            # Verify Cap Rate
            expected_cap_rate = (sample['NOI (€)'] / sample['Price (€)']) * 100
            actual_cap_rate = sample['Cap Rate (%)']
            cap_rate_match = abs(expected_cap_rate - actual_cap_rate) < 0.1
            log_message(f"Cap Rate: {expected_cap_rate:.2f}% vs {actual_cap_rate:.2f}% - {'✓' if cap_rate_match else '✗'}")
            if not cap_rate_match:
                all_correct = False
            
            # Verify Gross Yield
            expected_gross_yield = (sample['Annual Rent (€)'] / sample['Price (€)']) * 100
            actual_gross_yield = sample['Gross Yield (%)']
            gross_yield_match = abs(expected_gross_yield - actual_gross_yield) < 0.1
            log_message(f"Gross Yield: {expected_gross_yield:.2f}% vs {actual_gross_yield:.2f}% - {'✓' if gross_yield_match else '✗'}")
            if not gross_yield_match:
                all_correct = False
                
            # Verify Price per sqm
            expected_price_per_sqm = sample['Price (€)'] / sample['Size (sqm)']
            actual_price_per_sqm = sample['Price per sqm (€)']
            price_per_sqm_match = abs(expected_price_per_sqm - actual_price_per_sqm) < 1
            log_message(f"Price per sqm: {expected_price_per_sqm:.2f}€ vs {actual_price_per_sqm:.2f}€ - {'✓' if price_per_sqm_match else '✗'}")
            if not price_per_sqm_match:
                all_correct = False
        
        # Run a complete check on all properties
        df['Calculated NOI'] = df['Annual Rent (€)'] - df['Total Recurring Expenses (€)']
        df['NOI Difference'] = df['NOI (€)'] - df['Calculated NOI']
        
        df['Calculated Cap Rate'] = (df['NOI (€)'] / df['Price (€)']) * 100
        df['Cap Rate Difference'] = df['Cap Rate (%)'] - df['Calculated Cap Rate']
        
        noi_discrepancies = (abs(df['NOI Difference']) > 1).sum()
        cap_rate_discrepancies = (abs(df['Cap Rate Difference']) > 0.1).sum()
        
        log_message(f"\n=== FULL VALIDATION RESULTS ===")
        log_message(f"NOI Discrepancies: {noi_discrepancies} out of {len(df)} properties")
        log_message(f"Cap Rate Discrepancies: {cap_rate_discrepancies} out of {len(df)} properties")
        
        if all_correct and noi_discrepancies == 0 and cap_rate_discrepancies == 0:
            log_message("\nAll metrics are correctly calculated! ✓")
        else:
            log_message("\nSome metrics still have discrepancies! ✗")
            
    except Exception as e:
        log_message(f"Error verifying metrics: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_fixed_metrics() 