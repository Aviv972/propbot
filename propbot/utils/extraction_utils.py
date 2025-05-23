#!/usr/bin/env python3
"""
Property Data Extraction Utilities

This module provides robust, centralized extraction functions for property data
to ensure consistent parsing and validation across the PropBot system.
"""

import re
import logging
from typing import Optional, Dict, Tuple, Union, Any

logger = logging.getLogger(__name__)

# Typical size ranges in square meters for different property types in Lisbon
TYPICAL_SIZE_RANGES = {
    'T0': (15, 50),    # Studio apartments
    'Studio': (15, 50),
    'T1': (30, 80),    # 1-bedroom apartments
    'T2': (50, 120),   # 2-bedroom apartments
    'T3': (70, 150),   # 3-bedroom apartments
    'T4': (90, 200),   # 4-bedroom apartments
    'T5': (110, 250),  # 5-bedroom apartments
    'T6': (130, 300),  # 6-bedroom apartments
}

# Maximum reasonable size thresholds (anything above is suspicious)
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

def extract_size(text: Union[str, None], room_type: str = None) -> Tuple[Optional[float], bool]:
    """
    Extract property size from text with robust pattern matching and validation.
    
    This is the canonical size extraction function that should be used throughout
    the codebase to ensure consistent handling of property size formats.
    
    Args:
        text: Text containing size information (can be details, title, or dedicated size field)
        room_type: Optional room type (T0, T1, T2, etc.) to help validate the extracted size
        
    Returns:
        Tuple of:
        - Extracted size as float or None if extraction failed
        - Boolean indicating if the extraction is high confidence (True) or potentially problematic (False)
    """
    if not text:
        return None, False
    
    # Convert to string and normalize whitespace
    text = str(text).strip()
    high_confidence = True
    extracted_size = None
    
    # Priority 1: Room type and size concatenated without space (e.g., "T275 m²") - most error-prone pattern
    concatenated_pattern = re.search(r'T([0-6])(\d{2,})\s*m²', text)
    if concatenated_pattern:
        try:
            room_digit = concatenated_pattern.group(1)
            size_digits = concatenated_pattern.group(2)
            extracted_size = float(size_digits)
            
            # This is the problematic pattern we're targeting
            logger.info(f"Found concatenated room type and size: T{room_digit}{size_digits} m², extracting size as {extracted_size}")
            detected_room_type = f"T{room_digit}"
            
            # If provided room_type matches what we found, this increases confidence
            if room_type and room_type == detected_room_type:
                high_confidence = True
            
            return extracted_size, high_confidence
        except (ValueError, TypeError):
            pass
    
    # Priority 2: Room type and size separated by space or hyphen (e.g., "T2 70 m²" or "T2-70 m²")
    separated_pattern = re.search(r'T([0-6])[\s-]+(\d+(?:\.\d+)?)\s*m²', text)
    if separated_pattern:
        try:
            extracted_size = float(separated_pattern.group(2))
            logger.debug(f"Found separated room type and size: {text}, extracting size as {extracted_size}")
            return extracted_size, True
        except (ValueError, TypeError):
            pass
    
    # Priority 3: Standard size pattern (e.g., "70 m²")
    standard_pattern = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
    if standard_pattern:
        try:
            size_str = standard_pattern.group(1)
            extracted_size = float(size_str)
            logger.debug(f"Found standard size pattern: {size_str} m², extracting size as {extracted_size}")
            
            # Validate: If size is suspiciously large and starts with a digit 1-6,
            # it might be a T-format with attached size (e.g., "T270 m²" represented as "270 m²")
            if extracted_size > 100 and len(size_str) >= 3:
                first_digit = size_str[0]
                
                # If first digit matches a room type and pattern looks suspicious
                if first_digit in '123456':
                    # Check if we have a known room type to compare against
                    if room_type and f'T{first_digit}' == room_type:
                        new_size = float(size_str[1:])
                        logger.warning(f"Corrected size from {extracted_size} to {new_size} based on room type {room_type}")
                        return new_size, False  # Lower confidence since we're making an assumption
                    
                    # If room type is present in text and matches first digit
                    elif room_type and re.search(rf'T{first_digit}\b', text):
                        new_size = float(size_str[1:])
                        logger.warning(f"Corrected size from {extracted_size} to {new_size} based on room type in text")
                        return new_size, False
                    elif room_type:
                        # If room type doesn't match the first digit, don't correct
                        logger.debug(f"Room type {room_type} doesn't match first digit {first_digit}, keeping original size")
                        return extracted_size, high_confidence
                    
                    # If no room type is provided, be more conservative about correcting
                    elif not room_type:
                        logger.debug(f"No room type provided, keeping original size {extracted_size}")
                        return extracted_size, high_confidence
            
            return extracted_size, high_confidence
        except (ValueError, TypeError):
            pass
    
    # Check for size patterns with T that might be missing the space (e.g., "T270" without "m²")
    implied_size_pattern = re.search(r'T([0-6])(\d{2,})', text)
    if implied_size_pattern:
        try:
            room_digit = implied_size_pattern.group(1) 
            size_digits = implied_size_pattern.group(2)
            extracted_size = float(size_digits)
            logger.debug(f"Extracted size {extracted_size} from pattern without m² unit: T{room_digit}{size_digits}")
            return extracted_size, False  # Lower confidence without explicit unit
        except (ValueError, TypeError):
            pass
    
    # Check for plain number after room type
    plain_number_pattern = re.search(r'T\d.*?(\d+(?:\.\d+)?)', text)
    if plain_number_pattern:
        try:
            extracted_size = float(plain_number_pattern.group(1))
            logger.debug(f"Found plain number after room type: {extracted_size}")
            return extracted_size, False  # Lower confidence
        except (ValueError, TypeError):
            pass
    
    # Nothing matched, try simpler fallback - any number between 20-400
    # This is desperation mode with very low confidence
    fallback_pattern = re.search(r'(\d+)', text)
    if fallback_pattern:
        try:
            num = float(fallback_pattern.group(1))
            # Don't limit to 400 as some properties can be larger
            if num >= 20:  # Just ensure it's a reasonable size
                logger.debug(f"Using fallback extraction, found number: {num}")
                return num, False
        except (ValueError, TypeError):
            pass
    
    # No size found or all attempts failed
    return None, False

def extract_room_type(text: Union[str, None]) -> Optional[str]:
    """
    Extract room type (T0, T1, T2, etc.) from text.
    
    Args:
        text: Text that may contain room type information
        
    Returns:
        Extracted room type or None if not found
    """
    if not text:
        return None
    
    text = str(text).strip()
    
    # Standard pattern: T followed by a digit, as a standalone pattern
    standard_match = re.search(r'\b(T[0-6])\b', text)
    if standard_match:
        return standard_match.group(1)
    
    # Look for "studio" or "T0" as equivalent
    studio_match = re.search(r'\b(studio|studios)\b', text, re.IGNORECASE)
    if studio_match:
        return "T0"
    
    # Fall back to any T-digit pattern, even if not bounded
    fallback_match = re.search(r'T([0-6])', text)
    if fallback_match:
        return f"T{fallback_match.group(1)}"
    
    # Check for room counts that could imply room types
    rooms_match = re.search(r'(\d+)[- ]*(quartos|bedrooms|rooms)', text, re.IGNORECASE)
    if rooms_match:
        try:
            room_count = int(rooms_match.group(1))
            if 0 <= room_count <= 6:
                return f"T{room_count}"
        except (ValueError, TypeError):
            pass
    
    return None

def validate_property_size(size: Optional[float], room_type: Optional[str]) -> Tuple[Optional[float], bool]:
    """
    Validate and possibly correct a property size based on typical ranges.
    
    Args:
        size: The size value to validate
        room_type: The room type (T0, T1, etc.) for context
        
    Returns:
        Tuple of:
        - Validated/corrected size or original if no issues found
        - Boolean indicating whether the size is valid (True) or suspicious (False)
    """
    if size is None:
        return None, False
    
    # If no room type, just do basic sanity check
    if not room_type or room_type not in TYPICAL_SIZE_RANGES:
        # Basic sanity checks for any property
        if size <= 0:
            return None, False
        if size > MAX_SIZE_THRESHOLDS['default']:
            # Extremely large size, likely an error
            return size / 10 if size > 1000 else size / 2, False
        return size, True
    
    # Get typical range for this room type
    min_size, max_size = TYPICAL_SIZE_RANGES[room_type]
    max_threshold = MAX_SIZE_THRESHOLDS.get(room_type, MAX_SIZE_THRESHOLDS['default'])
    
    # If size is within normal range, it's valid
    if min_size <= size <= max_size:
        return size, True
    
    # Slightly outside range but under threshold - probably fine
    if size < min_size * 0.7 or (size > max_size and size <= max_threshold):
        return size, False  # Return as is but flag as suspicious
    
    # Way outside reasonable range - attempt correction
    if size > max_threshold:
        # Room type might be embedded in the size
        room_digit_match = re.match(r'T(\d)', room_type)
        if room_digit_match:
            room_digit = room_digit_match.group(1)
            size_str = str(int(size))
            
            # If size starts with the room digit, remove it
            if size_str.startswith(room_digit) and len(size_str) >= 3:
                corrected_size = float(size_str[1:])
                # If the corrected size is reasonable, use it
                if min_size * 0.7 <= corrected_size <= max_size * 1.3:
                    return corrected_size, False
        
        # Extremely large - might be a decimal error or other issue
        if size > max_threshold * 3:
            return size / 10, False
        elif size > max_threshold * 1.5:
            return size / 2, False
    
    # If too small, it might be wrong units or a partial size
    if size < min_size * 0.7 and size > 0:
        # Extremely small size for any apartment type
        if size < 10:
            # Might be missing a digit - but this is very low confidence
            return size * 10, False
    
    # If we got here, the size is outside normal range but we couldn't correct it
    return size, False 