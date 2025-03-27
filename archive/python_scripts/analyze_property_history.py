import json
import csv
from collections import defaultdict
import os
from datetime import datetime

# Check if we have any property snapshots or backups
print("Checking for property data history...")
files = [f for f in os.listdir('.') if f.startswith('idealista_listings') and f.endswith('.json')]
files.sort()  # Sort by name which should order by date if backups are named with dates

# If we only have the current file, we'll just analyze that in detail
if len(files) <= 1:
    print(f"Only found current idealista_listings.json file. Analyzing in detail...")
    
    # Load the sales data
    with open('idealista_listings.json', 'r', encoding='utf-8') as f:
        sales_data = json.load(f)
    
    print(f"Total properties: {len(sales_data)}")
    
    # Check for properties with the same ID but different URLs
    property_ids = {}
    duplicate_ids = []
    
    for prop in sales_data:
        url = prop.get('url', '')
        prop_id = None
        
        # Extract property ID from URL if possible
        if 'imovel/' in url:
            parts = url.split('imovel/')
            if len(parts) > 1:
                id_part = parts[1].split('/')[0]
                if id_part.isdigit():
                    prop_id = id_part
        
        if prop_id:
            if prop_id in property_ids:
                duplicate_ids.append(prop_id)
            else:
                property_ids[prop_id] = url
    
    print(f"Properties with IDs extracted: {len(property_ids)}")
    print(f"Duplicate property IDs: {len(set(duplicate_ids))}")
    
    # Analyze when properties were added based on credits usage history
    if os.path.exists('credits_usage.json'):
        with open('credits_usage.json', 'r') as f:
            credits_data = json.load(f)
        
        usage_history = credits_data.get('usage_history', [])
        dates = [entry['timestamp'].split()[0] for entry in usage_history]
        unique_dates = sorted(set(dates))
        
        print("\nScraping activity by date:")
        for date in unique_dates:
            date_count = dates.count(date)
            credits = sum([entry['credits_used'] for entry in usage_history if entry['timestamp'].startswith(date)])
            print(f"  {date}: {date_count} API calls, {credits} credits used")
        
        print(f"\nTotal credits used: {credits_data.get('total_credits_used', 0)}")
        
    # Group properties by location to find possible duplicates in same building
    locations = defaultdict(list)
    for prop in sales_data:
        location = prop.get('location', '').strip()
        if location:
            locations[location].append(prop)
    
    multi_prop_locations = {loc: props for loc, props in locations.items() if len(props) > 1}
    print(f"\nLocations with multiple properties: {len(multi_prop_locations)}")
    
    print("\nTop 5 locations by property count:")
    sorted_locations = sorted(locations.items(), key=lambda x: len(x[1]), reverse=True)
    for i, (location, props) in enumerate(sorted_locations[:5]):
        print(f"  {location}: {len(props)} properties")
        for j, prop in enumerate(props[:3]):
            price = prop.get('price', 'unknown price')
            details = prop.get('details', 'no details')
            print(f"    - {prop.get('url', 'no url')} ({price}, {details})")
        if len(props) > 3:
            print(f"    - ... and {len(props)-3} more")
    
    # Count properties that were likely collected in the initial run (180 properties)
    # Find properties with different URLs but identical attributes
    by_attributes = defaultdict(list)
    for prop in sales_data:
        # Create a key using important attributes (excluding URL)
        title = prop.get('title', '').strip()
        location = prop.get('location', '').strip()
        price = prop.get('price', '').strip()
        details = prop.get('details', '').strip()
        
        key = f"{title}|{location}|{price}|{details}"
        by_attributes[key].append(prop)
    
    duplicate_attrs = {attrs: props for attrs, props in by_attributes.items() if len(props) > 1}
    print(f"\nProperty groups with identical attributes: {len(duplicate_attrs)}")
    print(f"Unique properties based on attributes: {len(by_attributes)}")
    
    # Let's try to estimate how many "real" unique properties we have by
    # counting unique addresses (ignoring floor levels)
    simplified_locations = defaultdict(list)
    for prop in sales_data:
        location = prop.get('location', '').strip()
        # Remove floor information and apartment numbers when present
        loc_parts = location.split(',')
        if len(loc_parts) > 1:
            # Keep just street name and number
            simplified_loc = loc_parts[0].strip()
            simplified_locations[simplified_loc].append(prop)
    
    print(f"\nUnique street addresses: {len(simplified_locations)}")
    
    # Count properties by room type
    room_types = defaultdict(int)
    for prop in sales_data:
        details = prop.get('details', '').upper()
        if 'T1' in details:
            room_types['T1'] += 1
        elif 'T2' in details:
            room_types['T2'] += 1
        elif 'T3' in details:
            room_types['T3'] += 1
        elif 'T4' in details:
            room_types['T4'] += 1
        else:
            room_types['Unknown'] += 1
    
    print("\nProperty count by room type:")
    for room_type, count in room_types.items():
        print(f"  {room_type}: {count} properties")
else:
    # Analyze multiple snapshots of property data if available
    print(f"Found {len(files)} property data files. Analyzing evolution over time...")
    for file in files:
        try:
            with open(file, 'r') as f:
                data = json.load(f)
                print(f"{file}: {len(data)} properties")
        except Exception as e:
            print(f"Error processing {file}: {str(e)}")
            
print("\nDone with property data analysis.") 