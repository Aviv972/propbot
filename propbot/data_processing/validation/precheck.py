#!/usr/bin/env python3
"""
Data Validation Precheck

This module provides functions to validate property data before processing.
"""

import os
import json
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Union, Optional, Tuple

# Set up logging
logger = logging.getLogger(__name__)

def validate_json_file(file_path: Union[str, Path]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Validate a JSON file for proper format and basic content.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Tuple of (is_valid, message, validation_results)
    """
    file_path = Path(file_path)
    
    # Check if file exists
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}", None
    
    try:
        # Try to load the JSON file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Basic validation
        validation_results = {
            'file_path': str(file_path),
            'file_size': os.path.getsize(file_path),
            'is_array': isinstance(data, list),
            'is_object': isinstance(data, dict),
            'record_count': len(data) if isinstance(data, list) else 1,
            'has_listings': 'listings' in data if isinstance(data, dict) else False,
            'listings_count': len(data['listings']) if isinstance(data, dict) and 'listings' in data else 0,
        }
        
        # More specific validations depending on data structure
        if validation_results['is_array']:
            # For array data, check if it contains objects with URL
            has_urls = sum(1 for item in data if isinstance(item, dict) and 'url' in item)
            validation_results['records_with_url'] = has_urls
            validation_results['url_percentage'] = round(has_urls / max(1, len(data)) * 100, 2)
            
            is_valid = has_urls > 0
            message = f"JSON file valid: {has_urls}/{len(data)} records have URLs"
            
        elif validation_results['has_listings']:
            # For objects with 'listings' field, check the listings
            listings = data['listings']
            has_urls = sum(1 for item in listings if isinstance(item, dict) and 'url' in item)
            validation_results['records_with_url'] = has_urls
            validation_results['url_percentage'] = round(has_urls / max(1, len(listings)) * 100, 2)
            
            is_valid = has_urls > 0
            message = f"JSON file valid: {has_urls}/{len(listings)} listings have URLs"
            
        else:
            # For single objects or other structures
            has_url = isinstance(data, dict) and 'url' in data
            validation_results['has_url'] = has_url
            
            is_valid = has_url
            message = "JSON file valid: single record has URL" if has_url else "JSON file invalid: missing URL field"
        
        logger.info(f"Validated {file_path}: {message}")
        return is_valid, message, validation_results
        
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON format: {e}", None
    except Exception as e:
        return False, f"Error validating file: {e}", None

def validate_csv_file(file_path: Union[str, Path]) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """
    Validate a CSV file for proper format and basic content.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (is_valid, message, validation_results)
    """
    file_path = Path(file_path)
    
    # Check if file exists
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}", None
    
    try:
        # Try to read the CSV file
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            if not header:
                return False, "CSV file is empty or has no header", None
            
            # Count the rows
            row_count = sum(1 for _ in reader)
        
        # Now check for required fields
        required_fields = ['url']
        missing_fields = [field for field in required_fields if field not in header]
        
        validation_results = {
            'file_path': str(file_path),
            'file_size': os.path.getsize(file_path),
            'header': header,
            'row_count': row_count,
            'has_required_fields': not missing_fields,
            'missing_fields': missing_fields,
        }
        
        is_valid = row_count > 0 and not missing_fields
        message = f"CSV file valid: {row_count} rows with required fields" if is_valid else \
                  f"CSV file invalid: Missing required fields {missing_fields}"
        
        logger.info(f"Validated {file_path}: {message}")
        return is_valid, message, validation_results
        
    except csv.Error as e:
        return False, f"Invalid CSV format: {e}", None
    except Exception as e:
        return False, f"Error validating file: {e}", None

def validate_data(file_paths: List[Union[str, Path]]) -> bool:
    """
    Validate data files before processing.
    
    Args:
        file_paths: List of paths to data files to validate
        
    Returns:
        True if all files pass validation, False otherwise
    """
    all_valid = True
    validation_results = []
    
    for file_path in file_paths:
        file_path = Path(file_path)
        
        # Skip if file doesn't exist
        if not os.path.exists(file_path):
            logger.warning(f"File not found: {file_path}")
            validation_results.append({
                'file_path': str(file_path),
                'is_valid': False,
                'message': "File not found"
            })
            all_valid = False
            continue
        
        # Determine file type and validate
        if file_path.suffix.lower() == '.json':
            is_valid, message, result = validate_json_file(file_path)
        elif file_path.suffix.lower() == '.csv':
            is_valid, message, result = validate_csv_file(file_path)
        else:
            logger.warning(f"Unsupported file type: {file_path}")
            validation_results.append({
                'file_path': str(file_path),
                'is_valid': False,
                'message': "Unsupported file type"
            })
            all_valid = False
            continue
        
        # Store validation result
        validation_results.append({
            'file_path': str(file_path),
            'is_valid': is_valid,
            'message': message,
            'details': result
        })
        
        if not is_valid:
            all_valid = False
    
    # Save validation results to a log file
    log_file = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))) / "logs" / "validation_log.json"
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': str(Path(__file__).stem),
                'files_checked': len(file_paths),
                'all_valid': all_valid,
                'results': validation_results
            }, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving validation log: {e}")
    
    if all_valid:
        logger.info(f"All {len(file_paths)} files passed validation")
    else:
        logger.warning(f"Validation failed for some files. See {log_file} for details.")
    
    return all_valid

# CLI entry point
if __name__ == "__main__":
    import argparse
    
    # Set up logging for command line use
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Validate property data files")
    parser.add_argument("files", nargs="+", help="Files to validate")
    
    args = parser.parse_args()
    
    # Run the validation
    success = validate_data(args.files)
    exit(0 if success else 1) 