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
import shutil

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

import pandas as pd

# Import configuration
from propbot.config import (
    DATA_DIR, RAW_DATA_DIR, PROCESSED_DATA_DIR, REPORTS_DIR, OUTPUT_DIR,
    SALES_RAW_FILE, SALES_PROCESSED_FILE
)

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

# Import database functions
try:
    from propbot.database_utils import (
        get_connection, 
        get_sales_listings_from_database,
        get_analyzed_properties_from_database,
        save_analyzed_property_to_database,
        save_analysis_results
    )
    HAS_DB_FUNCTIONS = True
except ImportError:
    HAS_DB_FUNCTIONS = False

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

def generate_reports(investment_data, base_dir=None, output_dir=None):
    """Generate investment analysis reports in JSON and CSV format."""
    logger.info("Generating investment analysis reports")
    
    # Ensure the reports directory exists
    if base_dir is None:
        # Use default paths
        output_dir = REPORTS_DIR
    else:
        output_dir = Path(base_dir) / "data" / "reports"
        
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate a timestamp for the filenames
    timestamp = pd.Timestamp.now().strftime('%Y%m%d')
    
    # Define output file paths
    json_output = output_dir / f"investment_summary_{timestamp}.json"
    csv_output = output_dir / f"investment_summary_{timestamp}.csv"
    best_properties_output = output_dir / f"best_properties_{timestamp}.json"
    
    # Copy to standard locations (for compatibility with older code)
    json_standard = output_dir / "investment_summary_current.json"
    csv_standard = output_dir / "investment_summary_current.csv"
    best_properties_standard = output_dir / "best_properties_current.json"
    
    # Filter to only properties with all metrics calculated
    valid_properties = []
    incomplete_properties = []
    for prop in investment_data:
        if all(key in prop and prop[key] is not None for key in INVESTMENT_METRICS):
            valid_properties.append(prop)
        else:
            incomplete_properties.append(prop)
    
    logger.info(f"Properties with complete metrics: {len(valid_properties)}")
    logger.info(f"Properties with incomplete metrics: {len(incomplete_properties)}")
    
    # Save the JSON file with all properties
    with open(json_output, 'w', encoding='utf-8') as f:
        json.dump(investment_data, f, indent=2, ensure_ascii=False)
    logger.info(f"Saved full investment data to {json_output}")
    
    # Also save to the standard location
    with open(json_standard, 'w', encoding='utf-8') as f:
        json.dump(investment_data, f, indent=2, ensure_ascii=False)
    
    # Save to the database if available
    if HAS_DB_FUNCTIONS:
        try:
            result_data = {
                "total_properties": len(investment_data),
                "valid_properties": len(valid_properties),
                "incomplete_properties": len(incomplete_properties),
                "metrics": INVESTMENT_METRICS
            }
            save_analysis_results("investment", result_data, len(investment_data))
            logger.info("Saved investment analysis results to database history")
        except Exception as e:
            logger.error(f"Error saving analysis results to database: {str(e)}")
    
    # Convert to DataFrame for CSV output
    df = pd.DataFrame(investment_data)
    
    # Save as CSV
    df.to_csv(csv_output, index=False, encoding='utf-8')
    logger.info(f"Saved investment data CSV to {csv_output}")
    
    # Also save to the standard location
    df.to_csv(csv_standard, index=False, encoding='utf-8')
    
    # Generate the best properties report
    if valid_properties:
        # Sort by cash flow, yield, etc.
        cash_flow_sorted = sorted(valid_properties, key=lambda x: x.get('monthly_cash_flow', 0), reverse=True)
        yield_sorted = sorted(valid_properties, key=lambda x: x.get('gross_yield', 0), reverse=True)
        cap_rate_sorted = sorted(valid_properties, key=lambda x: x.get('cap_rate', 0), reverse=True)
        
        best_properties = {
            'top_cash_flow': cash_flow_sorted[:20],
            'top_yield': yield_sorted[:20],
            'top_cap_rate': cap_rate_sorted[:20],
            'generated_at': pd.Timestamp.now().isoformat()
        }
        
        with open(best_properties_output, 'w', encoding='utf-8') as f:
            json.dump(best_properties, f, indent=2, ensure_ascii=False)
        logger.info(f"Generated best properties report at {best_properties_output}")
        
        # Also save to the standard location
        with open(best_properties_standard, 'w', encoding='utf-8') as f:
            json.dump(best_properties, f, indent=2, ensure_ascii=False)
        
        # Save best properties to database if available
        if HAS_DB_FUNCTIONS:
            try:
                result_data = {
                    "top_properties": {
                        "cash_flow": [prop["url"] for prop in cash_flow_sorted[:20]],
                        "yield": [prop["url"] for prop in yield_sorted[:20]],
                        "cap_rate": [prop["url"] for prop in cap_rate_sorted[:20]]
                    }
                }
                save_analysis_results("best_properties", result_data, len(valid_properties))
                logger.info("Saved best properties to database history")
            except Exception as e:
                logger.error(f"Error saving best properties to database: {str(e)}")
    else:
        logger.warning("No valid properties found with complete metrics - best properties report not generated")
    
    return json_output, csv_output, best_properties_output

def save_analyzed_property(property_data):
    """Save analyzed property to database."""
    if not HAS_DB_FUNCTIONS:
        logger.warning("Database functions not available - cannot save analyzed property")
        return False
    
    try:
        # Check for required fields
        required_fields = ['url', 'price', 'size', 'monthly_rent', 'gross_yield', 'cap_rate', 'monthly_cash_flow']
        if not all(field in property_data for field in required_fields):
            logger.warning(f"Missing required fields in property data - cannot save to database")
            return False
        
        # Save to database
        return save_analyzed_property_to_database(property_data)
    except Exception as e:
        logger.error(f"Error saving analyzed property to database: {str(e)}")
        return False

def main():
    """Main entry point for running the investment analysis."""
    
    # Ensure directory structure exists
    for directory in [PROCESSED_DIR, OUTPUT_DIR, REPORTS_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    # Copy current sales data to the processed directory
    # This is a legacy step for compatibility but will be removed in the future
    if os.path.exists(SALES_RAW_FILE) and not os.path.exists(SALES_PROCESSED_FILE):
        logger.info(f"Copying sales data from {SALES_RAW_FILE} to {SALES_PROCESSED_FILE}")
        shutil.copy2(SALES_RAW_FILE, SALES_PROCESSED_FILE)
    
    # Run the rental analysis to calculate estimated rents
    logger.info("Running rental analysis")
    rental_data, rental_metadata = analyze_rental_data()
    
    # Load sales data 
    logger.info("Loading sales data")
    if HAS_DB_FUNCTIONS:
        logger.info("Using database for sales data")
        investment_data = get_sales_listings_from_database()
        if not investment_data:
            logger.warning("No sales data found in database - falling back to CSV")
            investment_data = load_sales_data()
    else:
        investment_data = load_sales_data()
    
    logger.info(f"Loaded {len(investment_data)} properties for investment analysis")
    
    # Calculate investment metrics for all properties
    logger.info("Calculating investment metrics")
    enriched_data = calculate_investment_metrics(investment_data, rental_data)
    
    # Generate reports
    logger.info("Generating investment summary reports")
    json_report, csv_report, best_properties = generate_reports(enriched_data)
    
    # Save all analyzed properties to the database if available
    if HAS_DB_FUNCTIONS:
        logger.info("Saving analyzed properties to database")
        saved_count = 0
        for property_data in enriched_data:
            if save_analyzed_property(property_data):
                saved_count += 1
        logger.info(f"Saved {saved_count} analyzed properties to database")
    
    logger.info("Investment analysis complete")
    logger.info(f"Reports saved to {REPORTS_DIR}")
    
    return enriched_data

if __name__ == "__main__":
    main() 