import json

# Load the JSON file
with open('propbot/data/raw/rentals/rental_listings.json', 'r') as f:
    data = json.load(f)

# Print basic info
print(f"Top level type: {type(data)}")
print(f"Length: {len(data)}")

# Check the structure
if isinstance(data, list):
    if len(data) > 0:
        print(f"First item type: {type(data[0])}")
        
        if isinstance(data[0], dict):
            print(f"Keys in first item: {list(data[0].keys())}")
            
            if 'listings' in data[0]:
                listings = data[0]['listings']
                print(f"Listings type: {type(listings)}")
                print(f"Listings length: {len(listings)}")
                
                # Count total listings across all items
                total_listings = 0
                for item in data:
                    if isinstance(item, dict) and 'listings' in item:
                        total_listings += len(item['listings'])
                
                print(f"Total listings across all items: {total_listings}")
                
                # Print some sample listings
                if len(listings) > 0:
                    print("\nSample listing:")
                    sample = listings[0]
                    for key, value in sample.items():
                        print(f"  {key}: {value}") 