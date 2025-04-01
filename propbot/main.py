#!/usr/bin/env python3
"""
PropBot Main Entry Point

This module provides the main command-line interface for running
the PropBot data processing pipeline.
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

# Import pipeline functions
from propbot.data_processing.pipeline import (
    run_sales_pipeline,
    run_rentals_pipeline,
    run_full_pipeline
)

# Import database functions
try:
    from propbot.db_data_import import main as import_db_data
    from propbot.data_processing.update_db import update_database_after_scrape
    HAS_DB_FUNCTIONS = True
except ImportError:
    HAS_DB_FUNCTIONS = False

# Import utilities
from propbot.data_processing.utils import save_json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(description="PropBot Data Processing CLI")
    
    # Pipeline type
    parser.add_argument('--type', choices=['sales', 'rentals', 'both'], 
                        default='both', help='Pipeline type to run')
    
    # Input files
    parser.add_argument('--input', nargs='+', help='Input files to process')
    
    # Skip flags
    parser.add_argument('--skip-validation', action='store_true', help='Skip validation step')
    parser.add_argument('--skip-consolidation', action='store_true', help='Skip consolidation step')
    parser.add_argument('--skip-conversion', action='store_true', help='Skip conversion step')
    
    # Test data generation
    parser.add_argument('--create-test-data', action='store_true', help='Create test data')
    
    # Data directory
    parser.add_argument('--data-dir', help='Data directory path')

    # Database operations
    if HAS_DB_FUNCTIONS:
        parser.add_argument('--db-import', action='store_true', help='Import data to database')
        parser.add_argument('--update-db', action='store_true', help='Update database after processing')
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    # Determine data directory
    if args.data_dir:
        data_dir = Path(args.data_dir)
    elif 'PROPBOT_DATA_DIR' in os.environ:
        data_dir = Path(os.environ['PROPBOT_DATA_DIR'])
    elif 'DYNO' in os.environ:  # Running on Heroku
        data_dir = Path('/app/propbot/data')
    else:
        # Default to a data directory inside the package
        data_dir = Path(os.path.dirname(os.path.abspath(__file__))) / 'data'
    
    # Ensure data directories exist
    raw_dir = data_dir / 'raw'
    processed_dir = data_dir / 'processed'
    logs_dir = data_dir / 'logs'
    
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    
    # Create test data if requested
    if args.create_test_data:
        create_test_data(raw_dir)
        return 0
    
    # Handle database import if requested
    if HAS_DB_FUNCTIONS and args.db_import:
        logger.info("Importing data to database...")
        success = import_db_data()
        if success:
            logger.info("Database import completed successfully")
            return 0
        else:
            logger.error("Database import failed")
            return 1
    
    # Set up input files
    input_files = args.input if args.input else None
    sales_input_files = None
    rentals_input_files = None
    
    if input_files:
        if args.type == 'sales':
            sales_input_files = input_files
        elif args.type == 'rentals':
            rentals_input_files = input_files
        else:  # both
            # Try to determine which files are for which pipeline
            sales_input_files = [f for f in input_files if 'sale' in f.lower()]
            rentals_input_files = [f for f in input_files if 'rent' in f.lower()]
            
            # If we couldn't determine, use all files for both
            if not sales_input_files and not rentals_input_files:
                sales_input_files = input_files
                rentals_input_files = input_files
    
    # Set up pipeline configuration
    pipeline_config = {
        "data_dir": str(data_dir),
        "raw_sales_dir": str(raw_dir / 'sales'),
        "raw_rentals_dir": str(raw_dir / 'rentals'),
        "processed_dir": str(processed_dir),
        "logs_dir": str(logs_dir)
    }
    
    # Track start time
    start_time = datetime.now()
    
    try:
        # Run appropriate pipeline
        if args.type == "sales":
            logger.info("Starting sales pipeline")
            results = run_sales_pipeline(
                config=pipeline_config,
                input_files=input_files,
                skip_validation=args.skip_validation,
                skip_consolidation=args.skip_consolidation,
                skip_conversion=args.skip_conversion
            )
        elif args.type == "rentals":
            logger.info("Starting rentals pipeline")
            results = run_rentals_pipeline(
                config=pipeline_config,
                input_files=input_files,
                skip_validation=args.skip_validation,
                skip_consolidation=args.skip_consolidation,
                skip_conversion=args.skip_conversion
            )
        else:  # both
            logger.info("Starting full pipeline (sales and rentals)")
            results = run_full_pipeline(
                config=pipeline_config,
                sales_input_files=sales_input_files,
                rentals_input_files=rentals_input_files,
                skip_validation=args.skip_validation,
                skip_consolidation=args.skip_consolidation,
                skip_conversion=args.skip_conversion
            )
        
        # Add summary information
        elapsed_time = (datetime.now() - start_time).total_seconds()
        results["runtime_seconds"] = elapsed_time
        
        # Save the combined results to a timestamped file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = logs_dir / f"pipeline_results_{timestamp}.json"
        
        # Use our custom JSON encoder
        save_json(results, results_file)
        
        # Update database if requested and not already updated by the pipeline
        if HAS_DB_FUNCTIONS and args.update_db and "database_update" not in results:
            try:
                logger.info("Updating database with new data...")
                if update_database_after_scrape(args.type if args.type != 'both' else None):
                    logger.info("Database updated successfully")
                    results["database_update"] = {"success": True}
                else:
                    logger.warning("Database update failed")
                    results["database_update"] = {"success": False}
            except Exception as e:
                logger.error(f"Error updating database: {str(e)}")
                results["database_update"] = {"success": False, "error": str(e)}
            
            # Update the results file with database info
            save_json(results, results_file)
        
        # Report final status
        if results["success"]:
            logger.info(f"PropBot pipeline completed successfully in {elapsed_time:.2f} seconds")
            return 0
        else:
            logger.error(f"PropBot pipeline failed after {elapsed_time:.2f} seconds")
            return 1
            
    except Exception as e:
        logger.exception(f"Unhandled error in pipeline: {e}")
        return 1

def create_test_data(raw_dir: Path):
    """Create sample test data for pipeline testing."""
    # Create directories
    sales_dir = raw_dir / "sales"
    rentals_dir = raw_dir / "rentals"
    os.makedirs(sales_dir, exist_ok=True)
    os.makedirs(rentals_dir, exist_ok=True)
    
    # Create sample sales data
    sales_data = [
        {
            "url": "https://example.com/property/123",
            "title": "Test Property 1",
            "price": "250000",
            "size": "85 m²",
            "location": "Lisbon",
            "details": "2 bedrooms, 1 bathroom"
        },
        {
            "url": "https://example.com/property/456",
            "title": "Test Property 2",
            "price": "350000",
            "size": "120 m²",
            "location": "Porto",
            "details": "3 bedrooms, 2 bathrooms"
        }
    ]
    
    # Create sample rentals data
    rentals_data = [
        {
            "url": "https://example.com/rental/123",
            "title": "Test Rental 1",
            "price": "900",
            "size": "60 m²",
            "location": "Lisbon",
            "details": "T1",
            "is_rental": True
        },
        {
            "url": "https://example.com/rental/456",
            "title": "Test Rental 2",
            "price": "1200",
            "size": "85 m²",
            "location": "Porto",
            "details": "T2",
            "is_rental": True
        }
    ]
    
    # Write files using our save_json utility
    save_json(sales_data, raw_dir / "sales_listings.json")
    save_json(rentals_data, raw_dir / "rental_listings.json")
        
    logger.info("Created sample test data files")

if __name__ == "__main__":
    sys.exit(main())

