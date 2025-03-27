#!/usr/bin/env python3
import json
import os

json_file = 'propbot/data/raw/rentals/rental_listings.json'

if os.path.exists(json_file):
    print(f"Reading file: {json_file}")
    with open(json_file, 'r') as f:
        data = json.load(f)
        
    print(f"File type: {type(data)}")
    
    if isinstance(data, list):
        print(f"Number of top-level entries: {len(data)}")
        
        if len(data) > 0:
            first_entry = data[0]
            print(f"First entry keys: {list(first_entry.keys())}")
            
            if 'listings' in first_entry:
                listings_count = len(first_entry['listings'])
                print(f"Number of listings in first entry: {listings_count}")
                
                total_listings = sum(len(item.get('listings', [])) for item in data)
                print(f"Total number of rental properties: {total_listings}")
            else:
                # Assume each entry is a property
                print(f"Total number of rental properties: {len(data)}")
else:
    print(f"File not found: {json_file}") 