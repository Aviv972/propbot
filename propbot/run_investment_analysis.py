#!/usr/bin/env python3
"""
PropBot Investment Analysis and Summary Generator

This script analyzes property data and generates a comprehensive investment summary.
"""

import logging
import os
import json
from pathlib import Path
from typing import Dict, Any, List

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

import pandas as pd

# Import analysis modules
from propbot.analysis.metrics.investment_metrics import (
    calculate_all_investment_metrics,
    generate_best_properties_report
)
from propbot.analysis.metrics.rental_metrics import run_improved_analysis
from propbot.analysis.metrics.db_functions import (
    get_rental_estimates,
    get_sales_listings_from_database,
    save_multiple_analyzed_properties
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent
DATA_DIR = BASE_DIR / "propbot" / "data"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = DATA_DIR / "reports"

# Ensure directories exist
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

def analyze_rental_data() -> dict:
    """Run the rental data analysis to estimate rental income and return the report dictionary."""
    logger.info("Running rental income analysis...")
    
    try:
        # First check if rental estimates exist in the database
        rental_estimates = get_rental_estimates()
        
        if rental_estimates:
            # Convert list of dictionaries to dictionary with URL as key
            rental_estimates_dict = {estimate['url']: estimate for estimate in rental_estimates}
            logger.info(f"Loaded {len(rental_estimates_dict)} rental estimates from database")
            return rental_estimates_dict
            
        # If no rental estimates exist in database, run the rental analysis
        run_improved_analysis(
            similarity_threshold=40,  # Location similarity threshold
            min_comparable_properties=2  # Minimum number of comparable properties
        )
        
        # Try again to get rental estimates from database
        rental_estimates = get_rental_estimates()
        rental_estimates_dict = {estimate['url']: estimate for estimate in rental_estimates}
        logger.info(f"Generated and loaded {len(rental_estimates_dict)} rental estimates from database")
        return rental_estimates_dict
        
    except Exception as e:
        logger.error(f"Error in rental analysis: {str(e)}")
        logger.error("Returning empty dictionary for rental estimates")
        return {}

def load_sales_data() -> List[Dict[str, Any]]:
    """Load sales data directly from CSV."""
    logger.info("Loading sales data...")
    
    # First try to get data from the database
    try:
        from propbot.analysis.metrics.db_functions import get_sales_listings_from_database
        db_sales = get_sales_listings_from_database()
        if db_sales and len(db_sales) > 0:
            logger.info(f"Loaded {len(db_sales)} sales listings from database")
            return db_sales
        else:
            logger.info("No sales data found in database, falling back to file-based loading")
    except Exception as e:
        logger.info(f"Error loading sales data from database: {str(e)}")
        logger.info("Falling back to file-based loading")
    
    # Define file paths to use real data files
    sales_files = [
        PROCESSED_DIR / "sales_current.csv",
        PROCESSED_DIR / "sales.csv"
    ]
    
    sales_file = None
    for file_path in sales_files:
        if file_path.exists():
            sales_file = file_path
            break
    
    if not sales_file:
        logger.error(f"Sales data file not found in: {[str(f) for f in sales_files]}")
        raise FileNotFoundError(f"Sales data file not found")
    
    # Load data
    try:
        sales_df = pd.read_csv(sales_file)
        logger.info(f"Loaded {len(sales_df)} sales listings from {sales_file}")
        
        # Convert DataFrame to list of dictionaries
        sales_data = []
        for _, row in sales_df.iterrows():
            # Ensure size is properly parsed
            try:
                size = float(row.get('size', 0))
            except (ValueError, TypeError):
                size_str = str(row.get('size', ''))
                # Try to extract numeric part if it's a string
                if size_str:
                    import re
                    match = re.search(r'(\d+(?:\.\d+)?)', size_str)
                    size = float(match.group(1)) if match else 0
                else:
                    size = 0
            
            # Create property dict
            property_dict = {
                'url': row.get('url', ''),
                'price': float(row.get('price', 0)),
                'size': size,
                'room_type': row.get('room_type', ''),
                'location': row.get('location', '')
            }
            sales_data.append(property_dict)
        
        return sales_data
    
    except Exception as e:
        logger.error(f"Error loading sales data: {str(e)}")
        raise

def run_investment_metrics(properties_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Calculate investment metrics for all properties."""
    logger.info("Calculating investment metrics...")
    
    if not properties_data:
        logger.warning("No properties data provided for investment metrics calculation")
        return []
    
    # Default parameters
    investment_params = {
        "down_payment_rate": 0.20,  # 20% down payment
        "interest_rate": 0.035,  # 3.5% interest rate
        "loan_term_years": 30,  # 30-year mortgage
        "appreciation_rate": 0.03,  # 3% annual appreciation
        "income_tax_rate": 0.28,  # 28% income tax
    }
    
    expense_params = {
        "property_tax_rate": 0.005,  # 0.5% property tax
        "insurance_rate": 0.004,  # 0.4% insurance
        "maintenance_rate": 0.01,  # 1% maintenance
        "management_rate": 0.08,  # 8% property management
        "vacancy_rate": 0.08,  # 8% vacancy
        "closing_cost_rate": 0.03,  # 3% closing costs
        "renovation_cost_rate": 0.05,  # 5% renovation costs
    }
    
    # Process each property
    processed_properties = []
    
    for property_data in properties_data:
        try:
            # Calculate all investment metrics
            property_with_metrics = calculate_all_investment_metrics(
                property_data,
                investment_params=investment_params,
                expense_params=expense_params
            )
            
            processed_properties.append(property_with_metrics)
            
        except Exception as e:
            try:
                url = property_data.get('url', 'unknown')
            except AttributeError:
                url = str(property_data) if property_data else 'unknown'
            logger.warning(f"Failed to calculate metrics for property {url}: {str(e)}")
    
    logger.info(f"Investment metrics calculated for {len(processed_properties)} properties")
    return processed_properties

def generate_investment_summary(properties_with_metrics: List[Dict[str, Any]]) -> None:
    """Generate the investment summary report."""
    logger.info("Generating investment summary...")
    
    if not properties_with_metrics:
        logger.warning("No properties with metrics to include in the investment summary")
        return
    
    # Define output files
    json_output = REPORTS_DIR / f"investment_summary_{pd.Timestamp.now().strftime('%Y%m%d')}.json"
    csv_output = REPORTS_DIR / f"investment_summary_{pd.Timestamp.now().strftime('%Y%m%d')}.csv"
    best_properties_output = REPORTS_DIR / f"best_properties_{pd.Timestamp.now().strftime('%Y%m%d')}.json"
    
    # Save the complete report
    with open(json_output, 'w') as f:
        json.dump(properties_with_metrics, f, indent=2)
    
    # Convert to DataFrame for CSV export
    try:
        df = pd.DataFrame(properties_with_metrics)
        df.to_csv(csv_output, index=False)
    except Exception as e:
        logger.error(f"Error saving CSV report: {str(e)}")
    
    # Generate the best properties report
    try:
        generate_best_properties_report(properties_with_metrics, str(best_properties_output))
        logger.info(f"Best properties report saved to {best_properties_output}")
    except Exception as e:
        logger.error(f"Error generating best properties report: {str(e)}")
    
    logger.info(f"Investment summary saved to:")
    logger.info(f"  - JSON: {json_output}")
    logger.info(f"  - CSV: {csv_output}")

def main():
    """Main function to run the investment analysis pipeline."""
    logger.info("Starting PropBot investment analysis...")
    
    try:
        # Step 1: Run the rental analysis and load the results
        logger.info("Step 1: Running rental analysis and loading results...")
        rental_analysis_dict = analyze_rental_data()
        
        if not rental_analysis_dict:
            logger.warning("No rental analysis results available. The investment analysis may be incomplete.")
        
        # Load sales data
        sales_data = load_sales_data()
        
        # Merge sales data with rental estimates
        properties_with_rental_estimates = []
        for property_item in sales_data:
            url = property_item.get('url', '')
            rental_estimate = rental_analysis_dict.get(url, {})
            
            # Merge the data
            property_with_estimate = property_item.copy()
            property_with_estimate['monthly_rent'] = rental_estimate.get('estimated_monthly_rent', 0)
            property_with_estimate['annual_rent'] = rental_estimate.get('estimated_monthly_rent', 0) * 12
            property_with_estimate['comparable_count'] = rental_estimate.get('comparable_count', 0)
            property_with_estimate['rental_price_per_sqm'] = rental_estimate.get('price_per_sqm', 0)
            property_with_estimate['confidence'] = rental_estimate.get('confidence', 'low')
            
            properties_with_rental_estimates.append(property_with_estimate)
        
        logger.info(f"Merged {len(properties_with_rental_estimates)} properties with rental estimates")
        
        # Step 2: Calculate investment metrics
        logger.info("Step 2: Calculating investment metrics...")
        properties_with_metrics = run_investment_metrics(properties_with_rental_estimates)
        
        # Step 3: Generate investment summary
        logger.info("Step 3: Generating investment summary...")
        summary_result = generate_investment_summary(properties_with_metrics)
        
        # Step 4: Save analyzed properties to database
        logger.info("Step 4: Saving analyzed properties to database...")
        save_multiple_analyzed_properties(properties_with_metrics)
        
        logger.info("Investment analysis completed successfully")
        return summary_result
        
    except Exception as e:
        logger.error(f"Error in investment analysis: {str(e)}")
        return None

if __name__ == "__main__":
    main() 