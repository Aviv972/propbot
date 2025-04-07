#!/usr/bin/env python3
"""
Test script for price extraction logic in update_db.py

This script tests the price extraction functionality with various price formats
to ensure it correctly parses Euro prices in different formats.
"""

import re
import sys
import json

# Function to test - similar to what's in update_db.py
def extract_price_improved(price_str):
    """Extract price from string with improved parsing"""
    price_value = None
    
    # Try to directly extract price 
    if isinstance(price_str, (int, float)):
        price_value = float(price_str)
        print(f"Direct numeric price: {price_value}")
        return price_value
        
    # Handle non-string or empty inputs
    if not isinstance(price_str, str) or not price_str.strip():
        print(f"Invalid input: {price_str!r}")
        return 0
        
    # Improved price extraction for string values with Euro symbol
    # First, clean up the string to make extraction easier
    cleaned_price = price_str.replace('€', '').strip()
    
    # Try to extract the numeric part
    price_match = re.search(r'[\d.,\s]+', cleaned_price)
    if price_match:
        price_numeric = price_match.group(0).strip()
        
        # European format check (e.g., "350.000,00")
        if '.' in price_numeric and ',' in price_numeric and price_numeric.rindex('.') < price_numeric.rindex(','):
            # European format: "350.000,00" -> replace dots, then replace comma with dot
            price_numeric = price_numeric.replace('.', '').replace(',', '.')
            print(f"Detected European format: {price_str} -> {price_numeric}")
        # American format check (e.g., "350,000.00")
        elif ',' in price_numeric and '.' in price_numeric and price_numeric.rindex(',') < price_numeric.rindex('.'):
            # American format: "350,000.00" -> just remove commas
            price_numeric = price_numeric.replace(',', '')
            print(f"Detected American format: {price_str} -> {price_numeric}")
        # Only commas present - determine if thousand separator or decimal
        elif ',' in price_numeric and '.' not in price_numeric:
            # Check position of comma - if near end, likely decimal
            if len(price_numeric.split(',')[1]) <= 2:
                # Likely decimal comma: "350,00" -> replace with dot
                price_numeric = price_numeric.replace(',', '.')
                print(f"Detected decimal comma: {price_str} -> {price_numeric}")
            else:
                # Likely thousand separator: "350,000" -> remove comma
                price_numeric = price_numeric.replace(',', '')
                print(f"Detected thousand separator comma: {price_str} -> {price_numeric}")
        # Only dots present - determine if thousand separator or decimal
        elif '.' in price_numeric and ',' not in price_numeric:
            # Check position of dot - if near end, likely decimal
            if len(price_numeric.split('.')[1]) <= 2:
                # Already in correct format: "350.00"
                print(f"Detected decimal dot: {price_str} -> {price_numeric}")
            else:
                # Likely thousand separator: "350.000" -> treat as European
                price_numeric = price_numeric.replace('.', '')
                print(f"Detected thousand separator dot: {price_str} -> {price_numeric}")
        
        # Remove any spaces
        price_numeric = price_numeric.replace(' ', '')
        
        try:
            price_value = float(price_numeric)
            print(f"Successfully parsed price: {price_value}")
            return price_value
        except ValueError as e:
            print(f"Could not convert to float: {price_numeric}, Error: {e}")
    else:
        print(f"No price pattern found in '{price_str}'")
    
    # Fallback if all else fails - just extract digits and try again
    digits_only = ''.join(c for c in price_str if c.isdigit())
    if digits_only:
        try:
            price_value = float(digits_only)
            print(f"Last resort digit extraction: {price_value}")
            return price_value
        except ValueError:
            pass
    
    # If we couldn't extract anything valid, return 0
    return 0

# Sample prices to test
test_prices = [
    "350,000 €",
    "350.000 €",
    "350.000€",
    "€350,000",
    "350000€",
    "350,000.00 €",
    "350.000,00 €",
    "350 000 €",
    350000,
    "Invalid price",
    None,
    ""
]

# Run tests
print("Testing price extraction functionality:")
print("-" * 50)
for price in test_prices:
    print(f"\nInput: {price!r}")
    result = extract_price_improved(price)
    print(f"Result: {result}")
    print("-" * 30)

print("\nDone testing.")

if __name__ == "__main__":
    # You can also test a specific price from command line
    if len(sys.argv) > 1:
        test_price = sys.argv[1]
        print(f"\nTesting command line input: '{test_price}'")
        result = extract_price_improved(test_price)
        print(f"Extracted price: {result}") 