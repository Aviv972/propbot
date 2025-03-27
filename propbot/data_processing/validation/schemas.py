#!/usr/bin/env python3
"""
Schema Validation Module

This module provides schema definitions and validation for property data.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Union, Optional, Tuple

# Set up logging
logger = logging.getLogger(__name__)

# Schema definitions
PROPERTY_BASE_SCHEMA = {
    "required": ["url"],
    "properties": {
        "url": {"type": "string"},
        "title": {"type": "string"},
        "price": {"type": ["string", "number", "null"]},
        "location": {"type": ["string", "null"]},
        "details": {"type": ["string", "null"]},
        "snapshot_date": {"type": ["string", "null"]}
    }
}

SALES_LISTING_SCHEMA = {
    **PROPERTY_BASE_SCHEMA,
    "required": ["url", "price"],
    "properties": {
        **PROPERTY_BASE_SCHEMA["properties"],
        "is_rental": {"type": "boolean", "enum": [False]},
        "size": {"type": ["string", "number", "null"]},
        "num_rooms": {"type": ["string", "number", "null"]},
        "num_bathrooms": {"type": ["string", "number", "null"]}
    }
}

RENTAL_LISTING_SCHEMA = {
    **PROPERTY_BASE_SCHEMA,
    "required": ["url", "price"],
    "properties": {
        **PROPERTY_BASE_SCHEMA["properties"],
        "is_rental": {"type": "boolean", "enum": [True]},
        "size": {"type": ["string", "number", "null"]},
        "num_rooms": {"type": ["string", "number", "null"]},
        "room_type": {"type": ["string", "null"]}
    }
}

def validate_against_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate data against a schema.
    
    Args:
        data: Data to validate
        schema: Schema to validate against
        
    Returns:
        Tuple of (is_valid, list of validation errors)
    """
    # Try to import jsonschema, with a helpful error if it's not installed
    try:
        from jsonschema import validate, ValidationError
    except ImportError:
        logger.error("jsonschema package is not installed. Please install it with: pip install jsonschema")
        # Basic validation - just check if required fields are present
        errors = []
        if "required" in schema:
            for field in schema["required"]:
                if field not in data or data[field] is None:
                    errors.append(f"Missing required field: {field}")
        
        # Consider it valid if no required field errors (minimal validation)
        return len(errors) == 0, errors
    
    errors = []
    
    try:
        validate(instance=data, schema=schema)
        return True, []
    except ValidationError as e:
        errors.append(f"{e.message} at {e.json_path}")
        return False, errors
    except Exception as e:
        errors.append(f"Unexpected error: {str(e)}")
        return False, errors

def validate_property_listing(listing: Dict[str, Any], is_rental: bool = None) -> Tuple[bool, List[str]]:
    """
    Validate a property listing against the appropriate schema.
    
    Args:
        listing: Property listing data
        is_rental: Override to specify if it's a rental listing
        
    Returns:
        Tuple of (is_valid, list of validation errors)
    """
    # Determine if it's a rental or sales listing
    if is_rental is None:
        is_rental = listing.get('is_rental', False)
    
    # Choose the appropriate schema
    schema = RENTAL_LISTING_SCHEMA if is_rental else SALES_LISTING_SCHEMA
    
    return validate_against_schema(listing, schema)

def validate_listings_file(file_path: Union[str, Path], 
                           is_rental: bool = None) -> Tuple[bool, Dict[str, Any]]:
    """
    Validate all listings in a JSON file.
    
    Args:
        file_path: Path to the JSON file
        is_rental: Override to specify if they are rental listings
        
    Returns:
        Tuple of (is_valid, validation results)
    """
    file_path = Path(file_path)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        return False, {
            "file": str(file_path),
            "error": f"Failed to load file: {str(e)}",
            "valid": False
        }
    
    # Determine the structure of the file
    listings = []
    
    if isinstance(data, list):
        listings = data
    elif isinstance(data, dict) and 'listings' in data:
        listings = data['listings']
    elif isinstance(data, dict):
        # Single listing
        listings = [data]
    
    # Validate each listing
    all_valid = True
    invalid_listings = []
    valid_count = 0
    
    for i, listing in enumerate(listings):
        is_valid, errors = validate_property_listing(listing, is_rental)
        
        if not is_valid:
            all_valid = False
            invalid_listings.append({
                "index": i,
                "url": listing.get('url', 'Unknown'),
                "errors": errors
            })
        else:
            valid_count += 1
    
    # Compile results
    results = {
        "file": str(file_path),
        "total_listings": len(listings),
        "valid_listings": valid_count,
        "invalid_listings": len(invalid_listings),
        "invalid_details": invalid_listings if invalid_listings else None,
        "all_valid": all_valid
    }
    
    if all_valid:
        logger.info(f"All {len(listings)} listings in {file_path} passed schema validation")
    else:
        logger.warning(f"{len(invalid_listings)} of {len(listings)} listings in {file_path} failed schema validation")
    
    return all_valid, results

# CLI entry point
if __name__ == "__main__":
    import argparse
    import sys
    
    # Set up logging for command line use
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Validate property data against schemas")
    parser.add_argument("file", help="JSON file to validate")
    parser.add_argument("--rental", action="store_true", help="Validate as rental listings")
    parser.add_argument("--sales", action="store_true", help="Validate as sales listings")
    parser.add_argument("--output", help="Output file for validation results")
    
    args = parser.parse_args()
    
    # Determine property type
    is_rental = None
    if args.rental:
        is_rental = True
    elif args.sales:
        is_rental = False
    
    # Run validation
    is_valid, results = validate_listings_file(args.file, is_rental)
    
    # Output results
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        print(f"Validation results saved to {args.output}")
    else:
        print(json.dumps(results, indent=2))
    
    sys.exit(0 if is_valid else 1) 