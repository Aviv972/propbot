#!/usr/bin/env python3
"""
Data Processing Pipeline Workflow

This module provides functions to orchestrate the complete data processing
workflow from raw data to processed CSV files.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Optional, Union, Any

# Import modules from relative paths
from ..consolidation.sales import consolidate_sales
from ..consolidation.rentals import consolidate_rentals
from ..conversion.sales import convert_sales
from ..conversion.rentals import convert_rentals
from ..validation.precheck import validate_data

# Set up logging
logger = logging.getLogger(__name__)

def get_default_paths() -> Dict[str, Path]:
    """
    Get default file paths for the data processing pipeline.
    
    Returns:
        Dictionary of default file paths
    """
    base_dir = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    data_dir = base_dir / "data"
    
    return {
        "raw_sales": data_dir / "raw" / "sales" / "idealista_listings.json",
        "raw_rentals": data_dir / "raw" / "rentals" / "rental_listings.json",
        "consolidated_sales": data_dir / "processed" / "sales_listings_consolidated.json",
        "consolidated_rentals": data_dir / "processed" / "rental_listings_consolidated.json",
        "sales_csv": data_dir / "processed" / "sales.csv",
        "rentals_csv": data_dir / "processed" / "rentals.csv"
    }

def run_workflow(custom_paths: Optional[Dict[str, Union[str, Path]]] = None) -> bool:
    """
    Run the complete data processing workflow.
    
    Args:
        custom_paths: Optional dictionary with custom file paths
        
    Returns:
        True if the workflow completed successfully, False otherwise
    """
    # Get paths (use defaults or override with custom paths)
    paths = get_default_paths()
    if custom_paths:
        for key, value in custom_paths.items():
            if key in paths:
                paths[key] = Path(value)
    
    try:
        # Step 1: Validate raw data
        logger.info("Validating raw data...")
        raw_files = [paths["raw_sales"], paths["raw_rentals"]]
        if not validate_data(raw_files):
            logger.error("Data validation failed")
            return False
            
        # Step 2: Consolidate data
        logger.info("Consolidating sales data...")
        raw_sales_dir = os.path.dirname(paths["raw_sales"])
        if not consolidate_sales(
            str(paths["raw_sales"]), 
            str(paths["consolidated_sales"]),
            raw_sales_dir
        ):
            logger.error("Sales consolidation failed")
            return False
            
        logger.info("Consolidating rental data...")
        raw_rentals_dir = os.path.dirname(paths["raw_rentals"])
        if not consolidate_rentals(
            str(paths["raw_rentals"]), 
            str(paths["consolidated_rentals"]),
            raw_rentals_dir
        ):
            logger.error("Rental consolidation failed")
            return False
            
        # Step 3: Convert to standardized CSV
        logger.info("Converting sales data to CSV...")
        if not convert_sales(paths["consolidated_sales"], paths["sales_csv"]):
            logger.error("Sales conversion failed")
            return False
            
        logger.info("Converting rental data to CSV...")
        if not convert_rentals(paths["consolidated_rentals"], paths["rentals_csv"]):
            logger.error("Rental conversion failed")
            return False
            
        logger.info("Data processing workflow completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in data processing workflow: {e}")
        return False

# CLI entry point
if __name__ == "__main__":
    import argparse
    
    # Set up logging for command line use
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the data processing workflow")
    parser.add_argument("--raw-sales", help="Path to raw sales data")
    parser.add_argument("--raw-rentals", help="Path to raw rentals data")
    parser.add_argument("--consolidated-sales", help="Path to save consolidated sales data")
    parser.add_argument("--consolidated-rentals", help="Path to save consolidated rentals data")
    parser.add_argument("--sales-csv", help="Path to save sales CSV")
    parser.add_argument("--rentals-csv", help="Path to save rentals CSV")
    
    args = parser.parse_args()
    
    # Build custom paths dictionary from command line arguments
    custom_paths = {}
    if args.raw_sales:
        custom_paths["raw_sales"] = args.raw_sales
    if args.raw_rentals:
        custom_paths["raw_rentals"] = args.raw_rentals
    if args.consolidated_sales:
        custom_paths["consolidated_sales"] = args.consolidated_sales
    if args.consolidated_rentals:
        custom_paths["consolidated_rentals"] = args.consolidated_rentals
    if args.sales_csv:
        custom_paths["sales_csv"] = args.sales_csv
    if args.rentals_csv:
        custom_paths["rentals_csv"] = args.rentals_csv
    
    # Run the workflow
    success = run_workflow(custom_paths if custom_paths else None)
    exit(0 if success else 1) 