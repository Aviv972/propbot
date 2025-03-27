#!/usr/bin/env python3
"""
Standard Data Processing Pipeline

This module provides a standard pipeline for processing property data
from raw input to validated, consolidated, and converted output.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Union, Optional, List
import shutil

# Import from our own modules
from propbot.data_processing.validation import validate_data, validate_listings_file
from propbot.data_processing.consolidation import consolidate_sales, consolidate_rentals
from propbot.data_processing.conversion import convert_sales, convert_rentals
from propbot.data_processing.utils import save_json

# Set up logging
logger = logging.getLogger(__name__)

class PropertyDataPipeline:
    """
    A pipeline for processing property data through all stages.
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline with configuration.
        
        Args:
            config: Configuration dictionary with paths and options
        """
        self.config = config or {}
        self.data_dir = Path(self.config.get("data_dir", "data"))
        self.results = {
            "pipeline_start": datetime.now().isoformat(),
            "stages": [],
            "success": False
        }
        
        # Ensure directories exist
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        self.logs_dir = self.data_dir / "logs"
        
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.logs_dir, exist_ok=True)
    
    def _add_stage_result(self, stage: str, success: bool, details: Dict[str, Any]) -> None:
        """Add results from a pipeline stage."""
        self.results["stages"].append({
            "stage": stage,
            "timestamp": datetime.now().isoformat(),
            "success": success,
            "details": details
        })
    
    def validate(self, file_paths: List[Union[str, Path]]) -> bool:
        """
        Validate input data files.
        
        Args:
            file_paths: List of file paths to validate
            
        Returns:
            True if validation succeeded, False otherwise
        """
        logger.info(f"Validating {len(file_paths)} input files")
        start_time = datetime.now()
        
        # Basic validation
        basic_validation = validate_data(file_paths)
        
        # Schema validation for JSON files
        schema_results = []
        all_schema_valid = True
        
        for file_path in file_paths:
            file_path = Path(file_path)
            if file_path.suffix.lower() == '.json':
                # Try to determine if it's rental or sales data from filename
                is_rental = 'rental' in file_path.name.lower()
                
                is_valid, results = validate_listings_file(file_path, is_rental)
                schema_results.append(results)
                
                if not is_valid:
                    all_schema_valid = False
        
        # Record results
        success = basic_validation and all_schema_valid
        self._add_stage_result("validation", success, {
            "basic_validation": basic_validation,
            "schema_validation": {
                "success": all_schema_valid,
                "results": schema_results
            },
            "duration_seconds": (datetime.now() - start_time).total_seconds()
        })
        
        if success:
            logger.info("Validation completed successfully")
        else:
            logger.error("Validation failed")
        
        return success
    
    def consolidate(self, property_type: str) -> bool:
        """
        Consolidate property data.
        
        Args:
            property_type: 'sales' or 'rentals'
            
        Returns:
            True if consolidation succeeded, False otherwise
        """
        logger.info(f"Consolidating {property_type} data")
        start_time = datetime.now()
        
        # Setup paths
        if property_type.lower() == 'sales':
            raw_file = self.raw_dir / "sales_listings.json"
            consolidated_file = self.processed_dir / "sales_listings_consolidated.json"
            raw_dir = self.raw_dir / "sales"
            legacy_csv = self.raw_dir / "legacy_sales.csv" if os.path.exists(self.raw_dir / "legacy_sales.csv") else None
            
            consolidate_fn = consolidate_sales
        elif property_type.lower() == 'rentals':
            raw_file = self.raw_dir / "rental_listings.json"
            consolidated_file = self.processed_dir / "rental_listings_consolidated.json"
            raw_dir = self.raw_dir / "rentals"
            legacy_csv = self.raw_dir / "legacy_rentals.csv" if os.path.exists(self.raw_dir / "legacy_rentals.csv") else None
            
            consolidate_fn = consolidate_rentals
        else:
            logger.error(f"Unknown property type: {property_type}")
            self._add_stage_result("consolidation", False, {
                "property_type": property_type,
                "error": f"Unknown property type: {property_type}"
            })
            return False
        
        try:
            # Ensure raw directory exists
            os.makedirs(raw_dir, exist_ok=True)
            
            # Run consolidation
            metadata = consolidate_fn(
                raw_file, 
                consolidated_file,
                raw_dir=raw_dir,
                legacy_csv=legacy_csv
            )
            
            success = metadata is not None
            self._add_stage_result("consolidation", success, {
                "property_type": property_type,
                "metadata": metadata,
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            })
            
            if success:
                logger.info(f"Consolidation of {property_type} completed successfully")
            else:
                logger.error(f"Consolidation of {property_type} failed")
            
            return success
            
        except Exception as e:
            logger.exception(f"Error during consolidation of {property_type}: {e}")
            self._add_stage_result("consolidation", False, {
                "property_type": property_type,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds()
            })
            return False
    
    def convert(self, property_type: str) -> bool:
        """
        Convert property data from consolidated JSON to standardized CSV.
        
        Args:
            property_type: 'sales' or 'rentals'
            
        Returns:
            True if conversion succeeded, False otherwise
        """
        logger.info(f"Converting {property_type} data")
        start_time = datetime.now()
        
        # Setup paths
        if property_type.lower() == 'sales':
            input_json = self.processed_dir / "sales_listings_consolidated.json"
            output_csv = self.processed_dir / "sales.csv"
            convert_fn = convert_sales
        elif property_type.lower() == 'rentals':
            input_json = self.processed_dir / "rental_listings_consolidated.json"
            output_csv = self.processed_dir / "rentals.csv"
            convert_fn = convert_rentals
        else:
            logger.error(f"Unknown property type: {property_type}")
            self._add_stage_result("conversion", False, {
                "property_type": property_type,
                "error": f"Unknown property type: {property_type}"
            })
            return False
        
        try:
            # Check for an existing file and make backup
            if os.path.exists(output_csv):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_file = str(output_csv) + f".backup.{timestamp}"
                shutil.copy2(output_csv, backup_file)
                logger.info(f"Created backup of existing file: {backup_file}")
            
            # Convert data
            metadata = convert_fn(input_json, output_csv)
            
            # For rentals specifically, make sure we preserve existing data
            if property_type.lower() == 'rentals':
                try:
                    import pandas as pd
                    # Check for archived rental data
                    archive_path = self.processed_dir / "archive"
                    newest_archive = None
                    newest_timestamp = 0
                    
                    # Find the most recent archive
                    if os.path.exists(archive_path):
                        for f in os.listdir(archive_path):
                            if f.startswith("rentals_") and f.endswith(".csv"):
                                file_path = os.path.join(archive_path, f)
                                file_time = os.path.getmtime(file_path)
                                if file_time > newest_timestamp:
                                    newest_timestamp = file_time
                                    newest_archive = file_path
                    
                    # If we found an archive, merge with current data
                    if newest_archive:
                        logger.info(f"Found rental archive: {newest_archive}")
                        # Load both datasets
                        current = pd.read_csv(output_csv)
                        logger.info(f"Current rentals CSV has {len(current)} entries")
                        archived = pd.read_csv(newest_archive)
                        logger.info(f"Archived rentals CSV has {len(archived)} entries")
                        
                        # Merge and deduplicate
                        merged = pd.concat([current, archived]).drop_duplicates(subset=['url'])
                        logger.info(f"Merged dataset has {len(merged)} entries")
                        
                        # Save merged data
                        merged.to_csv(output_csv, index=False)
                        
                        # Update metadata
                        if isinstance(metadata, dict):
                            metadata['record_count'] = len(merged)
                            metadata['processed_at'] = datetime.now().isoformat()
                            # Save updated metadata
                            metadata_file = str(output_csv).replace('.csv', '.metadata.json')
                            with open(metadata_file, 'w', encoding='utf-8') as f:
                                json.dump(metadata, f, indent=2)
                            logger.info(f"Updated metadata to reflect merged dataset with {len(merged)} records")
                except Exception as e:
                    logger.warning(f"Could not merge with archived rental data: {e}")
            
            # Create a current version (used by analysis scripts)
            current_file = str(output_csv).replace('.csv', '_current.csv')
            shutil.copy2(output_csv, current_file)
            logger.info(f"Created current version: {current_file}")
            
            end_time = datetime.now()
            self._add_stage_result("conversion", True, {
                "property_type": property_type,
                "records": metadata.get("record_count") if isinstance(metadata, dict) else None,
                "duration_seconds": (end_time - start_time).total_seconds()
            })
            logger.info(f"Conversion of {property_type} completed successfully")
            return True
        except Exception as e:
            logger.error(f"Error during conversion of {property_type}: {e}")
            logger.exception(e)
            self._add_stage_result("conversion", False, {
                "property_type": property_type,
                "error": str(e)
            })
            return False
    
    def run_pipeline(self, property_type: str, 
                    input_files: Optional[List[Union[str, Path]]] = None,
                    skip_validation: bool = False,
                    skip_consolidation: bool = False,
                    skip_conversion: bool = False) -> Dict[str, Any]:
        """
        Run the full pipeline for a property type.
        
        Args:
            property_type: 'sales' or 'rentals'
            input_files: List of input files for validation
            skip_validation: Skip the validation stage
            skip_consolidation: Skip the consolidation stage
            skip_conversion: Skip the conversion stage
            
        Returns:
            Dictionary with pipeline results
        """
        logger.info(f"Starting {property_type} pipeline")
        
        if not skip_validation and input_files:
            validation_success = self.validate(input_files)
            if not validation_success and not self.config.get("force_continue", False):
                logger.error("Validation failed. Pipeline aborted.")
                self.results["success"] = False
                self.results["pipeline_end"] = datetime.now().isoformat()
                return self.results
        
        if not skip_consolidation:
            consolidation_success = self.consolidate(property_type)
            if not consolidation_success and not self.config.get("force_continue", False):
                logger.error("Consolidation failed. Pipeline aborted.")
                self.results["success"] = False
                self.results["pipeline_end"] = datetime.now().isoformat()
                return self.results
        
        if not skip_conversion:
            conversion_success = self.convert(property_type)
            if not conversion_success:
                logger.error("Conversion failed.")
                self.results["success"] = False
                self.results["pipeline_end"] = datetime.now().isoformat()
                return self.results
        
        # If we got here, the pipeline was successful
        self.results["success"] = True
        self.results["pipeline_end"] = datetime.now().isoformat()
        logger.info(f"{property_type} pipeline completed successfully")
        
        # Save pipeline results
        try:
            results_file = self.logs_dir / f"{property_type}_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            save_json(self.results, results_file)
            logger.info(f"Pipeline results saved to {results_file}")
        except Exception as e:
            logger.error(f"Error saving pipeline results: {e}")
        
        return self.results

def run_sales_pipeline(config: Optional[Dict[str, Any]] = None, 
                       input_files: Optional[List[Union[str, Path]]] = None,
                       **kwargs) -> Dict[str, Any]:
    """
    Run the sales pipeline with the given configuration.
    
    Args:
        config: Pipeline configuration
        input_files: List of input files to validate
        **kwargs: Additional arguments for run_pipeline method
        
    Returns:
        Pipeline results
    """
    pipeline = PropertyDataPipeline(config)
    return pipeline.run_pipeline('sales', input_files, **kwargs)

def run_rentals_pipeline(config: Optional[Dict[str, Any]] = None, 
                         input_files: Optional[List[Union[str, Path]]] = None,
                         **kwargs) -> Dict[str, Any]:
    """
    Run the rentals pipeline with the given configuration.
    
    Args:
        config: Pipeline configuration
        input_files: List of input files to validate
        **kwargs: Additional arguments for run_pipeline method
        
    Returns:
        Pipeline results
    """
    pipeline = PropertyDataPipeline(config)
    return pipeline.run_pipeline('rentals', input_files, **kwargs)

def run_full_pipeline(config: Optional[Dict[str, Any]] = None,
                      sales_input_files: Optional[List[Union[str, Path]]] = None,
                      rentals_input_files: Optional[List[Union[str, Path]]] = None,
                      **kwargs) -> Dict[str, Any]:
    """
    Run both sales and rentals pipelines.
    
    Args:
        config: Pipeline configuration
        sales_input_files: Sales input files to validate
        rentals_input_files: Rental input files to validate
        **kwargs: Additional arguments for run_pipeline method
        
    Returns:
        Dictionary with combined pipeline results
    """
    results = {
        "pipeline_start": datetime.now().isoformat(),
        "sales_pipeline": None,
        "rentals_pipeline": None,
        "success": False
    }
    
    # Run sales pipeline
    if sales_input_files is not None or config.get("run_sales", True):
        logger.info("Starting sales pipeline")
        sales_pipeline = PropertyDataPipeline(config)
        sales_results = sales_pipeline.run_pipeline('sales', sales_input_files, **kwargs)
        results["sales_pipeline"] = sales_results
    
    # Run rentals pipeline
    if rentals_input_files is not None or config.get("run_rentals", True):
        logger.info("Starting rentals pipeline")
        rentals_pipeline = PropertyDataPipeline(config)
        rentals_results = rentals_pipeline.run_pipeline('rentals', rentals_input_files, **kwargs)
        results["rentals_pipeline"] = rentals_results
    
    # Determine overall success
    sales_success = results.get("sales_pipeline", {}).get("success", False) if config.get("run_sales", True) else True
    rentals_success = results.get("rentals_pipeline", {}).get("success", False) if config.get("run_rentals", True) else True
    results["success"] = sales_success and rentals_success
    results["pipeline_end"] = datetime.now().isoformat()
    
    # Save combined results
    try:
        logs_dir = Path(config.get("data_dir", "data")) / "logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        results_file = logs_dir / f"full_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        save_json(results, results_file)
        logger.info(f"Combined pipeline results saved to {results_file}")
    except Exception as e:
        logger.error(f"Error saving combined pipeline results: {e}")
    
    return results

# CLI entry point
if __name__ == "__main__":
    import argparse
    import sys
    
    # Set up basic logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logger.info("Starting sales pipeline")
    
    # Set up default directories
    script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    project_root = os.path.dirname(script_dir)
    
    # Define paths relative to project root and script directories
    data_dir = os.path.join(script_dir, "data")
    raw_sales_dir = os.path.join(data_dir, "raw", "sales")
    raw_rentals_dir = os.path.join(data_dir, "raw", "rentals")
    processed_dir = os.path.join(data_dir, "processed")
    
    # Make sure directories exist
    os.makedirs(raw_sales_dir, exist_ok=True)
    os.makedirs(raw_rentals_dir, exist_ok=True)
    os.makedirs(processed_dir, exist_ok=True)
    
    config = {
        "data_dir": data_dir,
        "raw_sales_dir": raw_sales_dir,
        "raw_rentals_dir": raw_rentals_dir,
        "processed_dir": processed_dir,
        "logs_dir": os.path.join(script_dir, "logs"),
        # Also look for files in the project root directory
        "project_root": project_root
    }
    
    # Run the pipeline
    result = run_full_pipeline(config)
    logger.info(f"Pipeline completed with success={result['success']}")
    
    if not result['success']:
        sys.exit(1) 