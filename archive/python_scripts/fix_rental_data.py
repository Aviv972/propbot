#!/usr/bin/env python3
import csv
import re
import os
from datetime import datetime

def fix_size_format(size_str):
    """Extract correct size from format like 'T2109 m²' -> 109 or '288 m²' -> 88 when it's a T2"""
    if not size_str:
        return None, None
    
    # Extract room type (T0, T1, T2, etc.)
    room_type_match = re.search(r'T(\d+)', size_str)
    room_type = None
    if room_type_match:
        room_type = f"T{room_type_match.group(1)}"
    
    # Extract size (just the number)
    size_match = re.search(r'(\d+)\s*m²', size_str)
    size = None
    if size_match:
        size_str = size_match.group(1)
        
        # Check for patterns where room type digit is prepended to size
        # e.g., 288 m² for a T2 property should be 88 m²
        if room_type and room_type.startswith('T') and len(room_type) == 2:
            room_digit = room_type[1]  # Get the digit from T2, T3, etc.
            
            if size_str.startswith(room_digit) and len(size_str) > 1:
                # If room digit matches first digit of size and size has more than 1 digit
                # Remove the first digit to get the actual size
                size = int(size_str[1:])
                return size, room_type
        
        # Default case - use the full size
        size = int(size_str)
    
    # Check for special patterns: T + digit + actual size
    if not size and room_type_match:
        # This handles cases like "T2109" where size might be after room type
        # Get everything after the room type pattern
        remainder = size_str[room_type_match.end():]
        # Look for a number
        remainder_match = re.search(r'(\d+)', remainder)
        if remainder_match:
            size = int(remainder_match.group(1))
        else:
            # If still no size, the number after T might be combined
            # Example: T2109 could mean T2 with 109 m²
            full_number = room_type_match.group(1)
            if len(full_number) > 1:
                room_type = f"T{full_number[0]}"
                size = int(full_number[1:])
    
    # Sanity check for unrealistic sizes
    if size and size > 1000:
        # Portuguese apartments are rarely over 300m²
        # If the size is over 1000m², it's likely a parsing error
        # Especially for "T2109" which could be "T2" and "109m²"
        
        # If the size matches exactly 2100 or 2109, it's likely a T2 apartment with size ~100-109m²
        if size in [2100, 2109]:
            size = size - 2000  # Convert 2109 to 109
        elif str(size).startswith('2'):
            # If the size starts with 2, it's likely a T2 apartment
            # Extract the size part (remove the leading 2)
            size = int(str(size)[1:])
        elif size > 1000 and size < 2000:
            # For cases like 1110 for a T1, it should be 110
            size_str = str(size)
            if len(size_str) > 2 and size_str[0] == '1':
                size = int(size_str[1:])
    
    # Additional check for room type prepended to size
    # For sizes like "288" when room_type is "T2", it's likely just "88"
    if size and room_type:
        size_str = str(size)
        room_digit = room_type[1]  # Get the digit from T2, T3, etc.
        
        # Check if the size starts with the room digit and is more than 2 digits
        if size_str.startswith(room_digit) and len(size_str) > 2:
            real_size = int(size_str[1:])
            # Only apply if the resulting size is reasonably sized (20-200 sqm)
            if 20 <= real_size <= 200:
                size = real_size
    
    return size, room_type

def fix_price_format(price_str):
    """Extract correct price from format like '3,250€/month' -> 3250"""
    if not price_str:
        return None
    
    # Extract just the numeric part before the € symbol
    price_match = re.search(r'([\d,.]+)', price_str)
    if not price_match:
        return None
    
    # Remove all non-numeric characters except digits and decimal point
    price_str = price_match.group(1)
    
    # Handle different number formats
    # If using comma as decimal separator (not likely for Portuguese format)
    if '.' in price_str and ',' in price_str:
        # Format like 1.234,56 -> 1234.56
        price_str = price_str.replace('.', '').replace(',', '.')
    else:
        # Format like 1,234 -> 1234 (comma as thousands separator)
        price_str = price_str.replace(',', '')
    
    try:
        return float(price_str)
    except ValueError:
        return None

def main():
    input_file = "rental_data_2025-03_complete.csv"
    output_file = "rental_data_2025-03_fixed.csv"
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found!")
        return
    
    # Create a backup of the original file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = f"rental_data_2025-03_complete.bak_{timestamp}.csv"
    with open(input_file, 'r', encoding='utf-8') as src, open(backup_file, 'w', encoding='utf-8') as dst:
        dst.write(src.read())
    print(f"Created backup: {backup_file}")
    
    rows_processed = 0
    size_fixed = 0
    price_fixed = 0
    invalid_rows = 0
    unrealistic_sizes_fixed = 0
    
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)
            
            # Write header row
            header = next(reader)
            writer.writerow(header)
            
            for row in reader:
                rows_processed += 1
                
                if len(row) < 5:
                    invalid_rows += 1
                    continue
                
                # Fix size and extract room type
                original_size = row[0]
                original_room_type = row[1]
                
                fixed_size, extracted_room_type = fix_size_format(original_size)
                
                # If room type was extracted from size field and original room type matches, use it
                if extracted_room_type:
                    if original_room_type and original_room_type != extracted_room_type:
                        # If room types don't match, check which is more reliable
                        if re.match(r'T\d+', original_room_type):
                            # Original room type follows expected pattern, keep it
                            pass
                        else:
                            # Use extracted room type
                            row[1] = extracted_room_type
                    else:
                        # Use extracted room type if original is empty
                        if not original_room_type:
                            row[1] = extracted_room_type
                
                # Replace size with fixed size
                if fixed_size:
                    # Check for unrealistic sizes
                    original_size_num = None
                    size_match = re.search(r'(\d+)', original_size)
                    if size_match:
                        original_size_num = int(size_match.group(1))
                        
                    if original_size_num and original_size_num != fixed_size:
                        print(f"Fixed size: {original_size} → {fixed_size} m² for {row[4]}")
                        unrealistic_sizes_fixed += 1
                    
                    row[0] = f"{fixed_size} m²"
                    size_fixed += 1
                
                # Fix price
                original_price = row[2]
                fixed_price = fix_price_format(original_price)
                
                if fixed_price:
                    row[2] = f"{fixed_price:.0f}€/month"
                    price_fixed += 1
                
                writer.writerow(row)
        
        print(f"Processed {rows_processed} rows")
        print(f"Fixed {size_fixed} sizes")
        print(f"Corrected {unrealistic_sizes_fixed} unrealistic sizes")
        print(f"Fixed {price_fixed} prices")
        print(f"Skipped {invalid_rows} invalid rows")
        print(f"Fixed data saved to {output_file}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 