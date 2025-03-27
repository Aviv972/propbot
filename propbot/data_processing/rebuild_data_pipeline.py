#!/usr/bin/env python3
"""
Rebuild Data Processing Pipeline

This script refreshes the data processing pipeline to ensure all data
is processed with the latest extraction utilities. It:

1. Recreates the processed data from raw input files
2. Re-runs the investment analysis
3. Regenerates the investment dashboard

This ensures all property sizes are properly parsed and analyzed.
"""

import os
import sys
import logging
from pathlib import Path
import pandas as pd
from datetime import datetime
import subprocess
import importlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_backup(file_path):
    """Create a backup of the specified file."""
    if not Path(file_path).exists():
        logger.warning(f"File not found, cannot create backup: {file_path}")
        return False
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_path = f"{file_path}.backup.{timestamp}"
    try:
        import shutil
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup at {backup_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create backup: {e}")
        return False

def run_command(command):
    """Run a shell command and return the result."""
    logger.info(f"Running command: {command}")
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info("Command completed successfully")
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False, e.stderr

def import_and_run_module(module_path):
    """Import a module and run its main function."""
    try:
        logger.info(f"Importing module: {module_path}")
        module = importlib.import_module(module_path)
        
        if hasattr(module, 'main'):
            logger.info(f"Running {module_path}.main()")
            result = module.main()
            return True, result
        else:
            logger.warning(f"Module {module_path} has no main function")
            return False, None
    except Exception as e:
        logger.error(f"Failed to import or run module {module_path}: {e}")
        return False, None

def main():
    """Main execution function to rebuild the data pipeline."""
    start_time = datetime.now()
    logger.info(f"Starting rebuild of data processing pipeline at {start_time}")
    
    # Step 1: Back up existing processed data
    logger.info("Creating backups of existing processed data...")
    data_dir = Path(__file__).parent.parent / "data" / "processed"
    
    for csv_file in ["sales.csv", "sales_current.csv", "rentals.csv", "rentals_current.csv"]:
        file_path = data_dir / csv_file
        create_backup(file_path)
    
    # Step 2: Process raw data to recreate sales and rentals datasets
    logger.info("Regenerating processed data from raw data...")
    success, _ = import_and_run_module("propbot.data_processing.process_raw_data")
    if not success:
        logger.error("Failed to process raw data. Aborting pipeline rebuild.")
        return False
    
    # Step 3: Run the data conversion
    logger.info("Running data conversions...")
    success, _ = import_and_run_module("propbot.data_processing.conversion.sales")
    if not success:
        logger.error("Failed to convert sales data. Continuing with rental conversion...")
    
    success, _ = import_and_run_module("propbot.data_processing.conversion.rentals")
    if not success:
        logger.error("Failed to convert rental data. Continuing with analysis...")
    
    # Step 4: Run investment analysis with rebuilt data
    logger.info("Running investment analysis...")
    success, _ = run_command("python3 -m propbot.run_investment_analysis")
    if not success:
        logger.error("Failed to run investment analysis. Continuing with dashboard generation...")
    
    # Step 5: Generate dashboard
    logger.info("Regenerating investment dashboard...")
    success, _ = run_command("python3 -m propbot.generate_dashboard")
    if not success:
        logger.error("Failed to generate dashboard.")
    
    # Step 6: Restart dashboard server
    logger.info("Restarting dashboard server...")
    run_command("pkill -f \"python3 -m propbot.run_dashboard_server\" || true")
    success, _ = run_command("python3 -m propbot.run_dashboard_server &")
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Data pipeline rebuild completed at {end_time} (Duration: {duration})")
    logger.info("You can view the updated dashboard at http://localhost:8005")
    
    return True

if __name__ == "__main__":
    main() 