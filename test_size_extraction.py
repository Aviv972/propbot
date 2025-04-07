#!/usr/bin/env python3
"""
Test script for size extraction logic

This script tests the improved size extraction functionality with various formats
to verify it correctly handles cases where room type (T2) is concatenated with size.
"""

import sys
import logging
import re
from typing import Union, Optional, Tuple

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# We'll implement a simplified version for testing purposes
# In production, this would use the actual function from extraction_utils.py
def extract_size_improved(text: Union[str, None], room_type: str = None) -> Tuple[Optional[float], bool]:
    """
    Extract property size from text with robust pattern matching and validation.
    
    This is a test version of the extraction_utils.extract_size function.
    
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
            print(f"Found concatenated room type and size: T{room_digit}{size_digits} m², extracting size as {extracted_size}")
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
            print(f"Found separated room type and size: {text}, extracting size as {extracted_size}")
            return extracted_size, True
        except (ValueError, TypeError):
            pass
    
    # Priority 3: Standard size pattern (e.g., "70 m²")
    standard_pattern = re.search(r'(\d+(?:\.\d+)?)\s*m²', text)
    if standard_pattern:
        try:
            size_str = standard_pattern.group(1)
            extracted_size = float(size_str)
            print(f"Found standard size pattern: {size_str} m², extracting size as {extracted_size}")
            
            # Validate: If size is suspiciously large and starts with a digit 1-6,
            # it might be a T-format with attached size (e.g., "T270 m²" represented as "270 m²")
            if extracted_size > 100 and len(size_str) >= 3:
                first_digit = size_str[0]
                
                # If first digit matches a room type and pattern looks suspicious
                if first_digit in '123456':
                    # Check if we have a known room type to compare against
                    if room_type and f'T{first_digit}' == room_type:
                        new_size = float(size_str[1:])
                        print(f"Corrected size from {extracted_size} to {new_size} based on room type {room_type}")
                        return new_size, False  # Lower confidence since we're making an assumption
                    
                    # If no room type is provided but there's a room type in the text
                    elif room_type and re.search(rf'T{first_digit}\b', text):
                        new_size = float(size_str[1:])
                        print(f"Corrected size from {extracted_size} to {new_size} based on room type in text")
                        return new_size, False
                    elif room_type:
                        # If room type doesn't match the first digit, don't correct
                        print(f"Room type {room_type} doesn't match first digit {first_digit}, keeping original size")
                        return extracted_size, high_confidence
                    
                    # If no room type is provided, be more conservative about correcting
                    elif not room_type:
                        print(f"No room type provided, keeping original size {extracted_size}")
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
            print(f"Extracted size {extracted_size} from pattern without m² unit: T{room_digit}{size_digits}")
            return extracted_size, False  # Lower confidence without explicit unit
        except (ValueError, TypeError):
            pass
        
    # Check for plain number after room type or size specification
    plain_number_pattern = re.search(r'T\d.*?(\d+(?:\.\d+)?)', text)
    if plain_number_pattern:
        try:
            extracted_size = float(plain_number_pattern.group(1))
            print(f"Found plain number after room type: {extracted_size}")
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
                print(f"Using fallback extraction, found number: {num}")
                return num, False
        except (ValueError, TypeError):
            pass
    
    print(f"Failed to extract size from: {text}")
    # No size found or all attempts failed
    return None, False

# Sample test cases
test_cases = [
    # Format: (test_string, expected_size, known_room_type)
    ("T275 m²Ground floor without lift", 75, "T2"),
    ("T170 m²1st floor with lift", 70, "T1"),
    ("T2 75 m² Ground floor", 75, "T2"),
    ("T2-75 m² Ground floor", 75, "T2"),
    ("75 m² T2 Ground floor", 75, "T2"),
    ("T2 apartment, 75 m²", 75, "T2"),
    ("T3, 110m²", 110, "T3"),
    ("T3110m²", 110, "T3"),
    # Keep high values without room type as is
    ("270 m²", 270, None),  # No room type, so don't correct
    ("T270 m²", 70, "T2"),  # The problematic case
    ("T2, Ground floor, 75m²", 75, "T2"),
    ("Size: 75m², T2", 75, "T2"),
    # For numeric size, only correct if room type is known and matches the first digit
    ("270m² T2", 70, "T2"),  # Contains "T2", so should correct 270
    ("Ground floor T2, 75", 75, "T2"),  # No units
]

# Run tests
print("Testing size extraction functionality:")
print("-" * 50)
passed = 0
failed = 0

for test_string, expected_size, room_type in test_cases:
    print(f"\nTest case: {test_string!r} (Expected: {expected_size})")
    size, confidence = extract_size_improved(test_string, room_type)
    print(f"Extracted: {size}, Confidence: {'High' if confidence else 'Low'}")
    
    if size == expected_size:
        print("✓ PASSED")
        passed += 1
    else:
        print(f"✗ FAILED - Expected {expected_size}, got {size}")
        failed += 1
    print("-" * 30)

print(f"\nTest results: {passed} passed, {failed} failed")

if __name__ == "__main__":
    # You can also test a specific string from command line
    if len(sys.argv) > 1:
        test_string = sys.argv[1]
        room_type = sys.argv[2] if len(sys.argv) > 2 else None
        print(f"\nTesting command line input: '{test_string}' (Room type: {room_type})")
        size, confidence = extract_size_improved(test_string, room_type)
        print(f"Extracted size: {size} (Confidence: {'High' if confidence else 'Low'})") 