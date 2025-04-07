#!/usr/bin/env python3
import logging
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

from propbot.analysis.metrics.rental_analysis import analyze_rental_yields

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Testing analyze_rental_yields with no data provided...")
    
    # Call analyze_rental_yields without providing any data
    results = analyze_rental_yields()
    
    # Log the results
    logger.info("Analysis results:")
    logger.info(f"Total rentals: {results.get('total_rentals', 0)}")
    logger.info(f"Total sales: {results.get('total_sales', 0)}")
    logger.info(f"Average rental price: {results.get('avg_rental_price')}")
    logger.info(f"Average sales price: {results.get('avg_sales_price')}")
    logger.info(f"Annual yield: {results.get('annual_yield')}")
    
    # Check if we got any data
    if results.get('total_rentals', 0) > 0 or results.get('total_sales', 0) > 0:
        logger.info("✅ Success: Data was loaded from the database")
    else:
        logger.warning("❌ Warning: No data was loaded from the database")

if __name__ == "__main__":
    main() 