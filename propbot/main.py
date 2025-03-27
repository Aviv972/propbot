#!/usr/bin/env python3
"""
PropBot - Property Data Analysis Tool

Main entry point for running the complete property data processing pipeline.
"""

import os
import argparse
import logging
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

# Import PropBot modules
from propbot.data_processing.pipeline import (
    run_sales_pipeline,
    run_rentals_pipeline,
    run_full_pipeline
)
from propbot.data_processing.utils import PathJSONEncoder, save_json
from propbot.config import load_config, CONFIG_PATH

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_input_files(raw_dir: Path, property_type: str) -> List[Path]:
    """
    Get a list of input files for a specific property type.
    
    Args:
        raw_dir: Raw data directory
        property_type: Type of properties (sales or rentals)
        
    Returns:
        List of input file paths
    """
    input_files = []
    
    # Add the main property listing file if it exists
    main_file = raw_dir / f"{property_type}_listings.json"
    if os.path.exists(main_file):
        input_files.append(main_file)
    
    # Add files from the property type subdirectory
    property_dir = raw_dir / property_type
    if os.path.exists(property_dir) and os.path.isdir(property_dir):
        for file in os.listdir(property_dir):
            file_path = property_dir / file
            if os.path.isfile(file_path) and file_path.suffix.lower() in ['.json', '.csv']:
                input_files.append(file_path)
    
    # Add legacy CSV if it exists
    legacy_csv = raw_dir / f"legacy_{property_type}.csv"
    if os.path.exists(legacy_csv):
        input_files.append(legacy_csv)
    
    return input_files

def setup_environment(args):
    """Set up the environment and configuration for the pipeline run."""
    # Load configuration
    try:
        config = load_config(args.config)
        logger.info(f"Loaded configuration from {args.config}")
    except Exception as e:
        logger.warning(f"Error loading configuration: {e}")
        config = {}
    
    # Override data directory if specified
    if args.data_dir:
        config["data_dir"] = args.data_dir
    
    # Set up data directories
    data_dir = Path(config.get("data_dir", "data"))
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"
    logs_dir = data_dir / "logs"
    
    # Ensure directories exist
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)
    
    # Update configuration with command line options
    pipeline_config = {
        **config,
        "force_continue": args.force_continue,
        "run_sales": args.type in ["sales", "both"],
        "run_rentals": args.type in ["rentals", "both"]
    }
    
    return pipeline_config, data_dir, raw_dir, processed_dir, logs_dir

def main():
    """Main entry point for PropBot."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PropBot - Property Data Analysis Tool")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to configuration file")
    parser.add_argument("--data-dir", help="Data directory (overrides config)")
    parser.add_argument("--type", choices=["sales", "rentals", "both"], default="both", 
                        help="Type of properties to process")
    parser.add_argument("--skip-validation", action="store_true", help="Skip validation step")
    parser.add_argument("--skip-consolidation", action="store_true", help="Skip consolidation step")
    parser.add_argument("--skip-conversion", action="store_true", help="Skip conversion step")
    parser.add_argument("--force-continue", action="store_true", help="Continue pipeline even if a step fails")
    parser.add_argument("--input-files", nargs="*", help="Specify input files for validation")
    parser.add_argument("--test", action="store_true", help="Run in test mode with sample data")
    
    args = parser.parse_args()
    
    # Set up environment
    pipeline_config, data_dir, raw_dir, processed_dir, logs_dir = setup_environment(args)
    
    # Create sample test data if in test mode
    if args.test:
        create_test_data(raw_dir)
    
    # Get input files for validation
    input_files = args.input_files if args.input_files else None
    
    # Get specific input files for each property type if processing both
    sales_input_files = None
    rentals_input_files = None
    
    if args.type == "both" and not args.input_files:
        sales_input_files = get_input_files(raw_dir, "sales")
        rentals_input_files = get_input_files(raw_dir, "rentals")
    elif args.type == "sales" and not args.input_files:
        input_files = get_input_files(raw_dir, "sales")
    elif args.type == "rentals" and not args.input_files:
        input_files = get_input_files(raw_dir, "rentals")
    
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

