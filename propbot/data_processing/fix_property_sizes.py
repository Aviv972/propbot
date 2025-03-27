#!/usr/bin/env python3
"""
Fix Property Sizes

This script corrects property sizes in existing datasets by applying improved
size extraction logic to fix cases where room types were incorrectly included
in the size values (e.g., "T2 70 m²" → "270 m²").
"""

import pandas as pd
import re
import os
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Define paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
DATA_DIR = BASE_DIR / "propbot" / "data"
PROCESSED_DIR = DATA_DIR / "processed"
SALES_CSV_FILE = PROCESSED_DIR / "sales.csv"
SALES_CURRENT_CSV_FILE = PROCESSED_DIR / "sales_current.csv"
RENTAL_CSV_FILE = PROCESSED_DIR / "rentals.csv"
RENTAL_CURRENT_CSV_FILE = PROCESSED_DIR / "rentals_current.csv"

# Typical size ranges for apartments in Lisbon by room type
TYPICAL_SIZES = {
    'T0': (15, 50),    # Studio apartments typically 15-50 m²
    'Studio': (15, 50), # Same as T0
    'T1': (30, 80),    # 1-bedroom apartments typically 30-80 m²
    'T2': (50, 120),   # 2-bedroom apartments typically 50-120 m²
    'T3': (70, 150),   # 3-bedroom apartments typically 70-150 m²
    'T4': (90, 200),   # 4-bedroom apartments typically 90-200 m²
    'T5': (110, 250),  # 5-bedroom apartments typically 110-250 m²
    'T6': (130, 300),  # 6-bedroom apartments typically 130-300 m²
}

# Max size thresholds (anything above these is automatically corrected)
MAX_SIZE_THRESHOLDS = {
    'T0': 60,
    'Studio': 60,
    'T1': 100,
    'T2': 140, 
    'T3': 180,
    'T4': 220,
    'T5': 280,
    'T6': 350,
    'default': 400,  # Default max size for any apartment
}

def correct_property_size(row):
    """
    Corrects property size based on the details and room type.
    """
    original_size = row['size']
    details = row.get('details', '')
    room_type = row.get('room_type', '')
    neighborhood = row.get('neighborhood', '')
    corrected_size = original_size

    # Skip if size is already reasonable or not a number
    if pd.isna(original_size) or not isinstance(original_size, (int, float)):
        return corrected_size
    
    # Get typical size range for this property type
    if isinstance(room_type, str) and room_type in TYPICAL_SIZES:
        min_size, max_size = TYPICAL_SIZES[room_type]
        max_threshold = MAX_SIZE_THRESHOLDS.get(room_type, MAX_SIZE_THRESHOLDS['default'])
    else:
        min_size, max_size = 30, 150  # Default range if room type is unknown
        max_threshold = MAX_SIZE_THRESHOLDS['default']
    
    # If size is below typical minimum, it's likely in square feet or there's a data error
    # For this script we focus on large sizes
    
    # Check if the size is too large due to room type + size concatenation
    if room_type and isinstance(room_type, str) and room_type.startswith('T') and len(room_type) > 1:
        try:
            room_number = int(room_type[1:])
            # If room_type is T1, T2, etc. and size is suspiciously large
            # Extract a more reasonable size from original value
            if original_size > max_threshold or (original_size > max_size and original_size < 1000):
                # For example, if room_type is T2 and size is 275, the actual size is 75
                room_digits = len(str(room_number))
                size_str = str(int(original_size))
                
                # Check if first digit matches room number
                if len(size_str) >= 2 and size_str[0] == str(room_number):
                    corrected_size = float(size_str[1:])
                    if min_size <= corrected_size <= max_size * 1.2:  # Allow some flexibility
                        return corrected_size
        except (ValueError, TypeError):
            pass

    # Look for size in the details if available
    if isinstance(details, str) and 'm²' in details:
        size_matches = re.findall(r'(\d+(?:\.\d+)?)\s*m²', details)
        if size_matches:
            # Get the first occurrence of m²
            try:
                size_value = float(size_matches[0])
                if min_size <= size_value <= max_size * 1.5 and size_value < original_size:
                    corrected_size = size_value
                    return corrected_size
            except ValueError:
                pass
    
    # Handle specific T1, T2, T3 and T4 patterns
    if room_type and isinstance(room_type, str) and room_type.startswith('T') and len(room_type) > 1:
        try:
            room_digit = int(room_type[1:])
            size_str = str(int(original_size))
            
            # Check specific patterns
            if room_digit == 1:
                if original_size > 100:
                    # T1 with size > 100 is suspicious
                    if len(size_str) == 3 and size_str.startswith('1'):
                        return float(size_str[1:])
                    elif original_size > max_threshold:
                        return original_size / 2
            
            elif room_digit == 2:
                if original_size > 120:
                    # T2 with size > 120 is suspicious
                    if len(size_str) == 3 and size_str.startswith('2'):
                        return float(size_str[1:])
                    elif original_size > max_threshold:
                        return original_size / 2
            
            elif room_digit == 3:
                if original_size > 150:
                    # T3 with size > 150 is suspicious
                    if len(size_str) == 3 and size_str.startswith('3'):
                        return float(size_str[1:])
                    elif original_size > max_threshold:
                        return original_size / 2
            
            elif room_digit == 4:
                if original_size > 200:
                    # T4 with size > 200 is suspicious
                    if len(size_str) == 3 and size_str.startswith('4'):
                        return float(size_str[1:])
                    elif original_size > max_threshold:
                        return original_size / 2
            
            # Handle T5 and T6 similarly
            elif room_digit in [5, 6]:
                if original_size > max_threshold:
                    if len(size_str) == 3 and size_str.startswith(str(room_digit)):
                        return float(size_str[1:])
                    return original_size / 2
        
        except (ValueError, TypeError):
            pass
    
    # Apply size corrections based on room type if the size is outside typical range
    if original_size > max_size and isinstance(room_type, str) and room_type in TYPICAL_SIZES:
        min_size, max_size = TYPICAL_SIZES[room_type]
        
        # Size is too large for the room type
        if original_size > max_threshold:
            # Check for T1 with 150+, T2 with 260+ pattern
            room_num_match = re.search(r'T(\d+)', room_type) if isinstance(room_type, str) else None
            if room_num_match:
                room_num = int(room_num_match.group(1))
                size_str = str(int(original_size))
                
                # First try to fix common error pattern
                if len(size_str) == 3 and size_str.startswith(str(room_num)):
                    corrected_size = float(size_str[1:])
                # Otherwise scale down to a reasonable size
                else:
                    factor = original_size / max_size
                    if factor > 3:
                        corrected_size = original_size / 10
                    elif factor > 2:
                        corrected_size = original_size / 3
                    else:
                        corrected_size = original_size / 2
            else:
                # Generic scaling for unknown room types
                corrected_size = original_size / 2
    
    # Final sanity check: extremely large apartments are rare in Lisbon
    if original_size > 400:
        # This is likely a parsing error - reduce to a more realistic size
        if original_size > 1000:
            corrected_size = original_size / 10
        elif original_size > 600:
            corrected_size = original_size / 5
        else:
            corrected_size = original_size / 2

    # If size is outside the typical range for the room type, adjust it
    if isinstance(room_type, str) and room_type in TYPICAL_SIZES:
        min_size, max_size = TYPICAL_SIZES[room_type]
        if corrected_size > max_threshold:
            # Adjust to be within a reasonable range
            corrected_size = (min_size + max_size) / 2  # Set to average size for this type
    
    # Return the corrected size, or original if no correction was applied
    return corrected_size

def backup_file(file_path):
    """Creates a backup of the file before making changes."""
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    backup_path = f"{file_path}.backup.{timestamp}"
    if os.path.exists(file_path):
        os.system(f"cp {file_path} {backup_path}")
        logger.info(f"Created backup at {backup_path}")
    return backup_path

def fix_property_sizes(file_path):
    """
    Reads a CSV file, corrects property sizes, and saves the updated data.
    """
    if not os.path.exists(file_path):
        logger.warning(f"File not found: {file_path}")
        return 0
    
    backup_file(file_path)
    
    try:
        df = pd.read_csv(file_path)
        if 'size' not in df.columns:
            logger.warning(f"Size column not found in {file_path}")
            return 0
        
        corrections_count = 0
        for idx, row in df.iterrows():
            original_size = row['size']
            if pd.isna(original_size):
                continue
                
            corrected_size = correct_property_size(row)
            if corrected_size != original_size:
                room_info = f" for {row.get('room_type', 'unknown room type')}" if 'room_type' in row else ""
                neighborhood_info = f" in {row.get('neighborhood', 'unknown area')}" if 'neighborhood' in row else ""
                logger.info(f"Corrected {original_size} to {corrected_size}{room_info}{neighborhood_info}")
                df.at[idx, 'size'] = corrected_size
                corrections_count += 1
        
        df.to_csv(file_path, index=False)
        logger.info(f"Saved corrected data to {file_path} with {corrections_count} corrections")
        return corrections_count
    
    except Exception as e:
        logger.error(f"Error processing {file_path}: {str(e)}")
        return 0

def fix_all_property_sizes():
    """
    Fix property sizes in all property data files.
    
    Returns:
        Total number of corrections made
    """
    logger.info("Starting property size correction process")
    
    total_corrections = 0
    
    # Fix sales data
    if os.path.exists(SALES_CSV_FILE):
        total_corrections += fix_property_sizes(SALES_CSV_FILE)
    
    if os.path.exists(SALES_CURRENT_CSV_FILE):
        total_corrections += fix_property_sizes(SALES_CURRENT_CSV_FILE)
    
    # Fix rental data
    if os.path.exists(RENTAL_CSV_FILE):
        total_corrections += fix_property_sizes(RENTAL_CSV_FILE)
    
    if os.path.exists(RENTAL_CURRENT_CSV_FILE):
        total_corrections += fix_property_sizes(RENTAL_CURRENT_CSV_FILE)
    
    logger.info(f"Property size correction process completed with {total_corrections} total corrections")
    return total_corrections

if __name__ == "__main__":
    fix_all_property_sizes() 