import os
import sys
from datetime import datetime
from rental_analysis import (
    load_sales_data, 
    load_rental_data, 
    generate_income_report, 
    save_report_to_json, 
    save_report_to_csv,
    log_message
)

def run_complete_analysis():
    """Run rental analysis with the complete rental dataset."""
    log_message("Starting rental income analysis with COMPLETE rental dataset (360 properties)")
    
    # Load the sales data
    properties_for_sale = load_sales_data()
    log_message(f"Loaded {len(properties_for_sale)} properties for sale")
    
    # Determine the current month for loading the rental data
    current_month = datetime.now().strftime("%Y-%m")
    
    # Use the complete rental data file
    rental_filename = f"rental_data_{current_month}_complete.csv"
    
    if not os.path.exists(rental_filename):
        log_message(f"Error: Complete rental data file {rental_filename} not found")
        return False
    
    # Load the rental data
    rental_properties = load_rental_data(rental_filename)
    log_message(f"Loaded {len(rental_properties)} rental properties from complete dataset")
    
    if not properties_for_sale or not rental_properties:
        log_message("Error: Could not load required data")
        return False
    
    # Generate the report with a unique filename
    income_estimates = generate_income_report(properties_for_sale, rental_properties)
    log_message(f"Generated income estimates for {len(income_estimates)} properties")
    
    # Calculate statistics
    properties_with_comparables = [p for p in income_estimates if p.get('comparable_count', 0) > 0]
    percent_with_comparables = (len(properties_with_comparables) / len(properties_for_sale)) * 100
    
    # Display statistics
    log_message(f"Properties with estimates: {len(properties_with_comparables)} out of {len(properties_for_sale)} ({percent_with_comparables:.1f}%)")
    
    # Count properties by number of comparables
    comparable_counts = {}
    for prop in income_estimates:
        count = prop.get('comparable_count', 0)
        comparable_counts[count] = comparable_counts.get(count, 0) + 1
    
    log_message("Distribution of comparable counts:")
    for count in sorted(comparable_counts.keys()):
        log_message(f"  {count} comparables: {comparable_counts[count]} properties")
    
    # Save results with unique filenames to avoid overwriting
    json_filename = "rental_income_report_complete.json"
    csv_filename = "rental_income_report_complete.csv"
    
    json_saved = save_report_to_json(income_estimates, json_filename)
    csv_saved = save_report_to_csv(income_estimates, csv_filename)
    
    log_message(f"Report saved to {json_filename} and {csv_filename}")
    log_message("Complete rental income analysis completed")
    return json_saved and csv_saved

if __name__ == "__main__":
    run_complete_analysis() 