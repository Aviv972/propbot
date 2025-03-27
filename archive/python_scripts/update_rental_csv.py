import json
import csv
from datetime import datetime

print("Updating rental data CSV from complete rental_listings.json...")

# Load the rental listings
with open('rental_listings.json', 'r', encoding='utf-8') as f:
    rental_data = json.load(f)

if len(rental_data) == 0:
    print("No rental data found in rental_listings.json")
    exit(1)

# Get the first month's data (assuming it's the current month we want)
month_data = rental_data[0]
month = month_data['month']
listings = month_data['listings']

print(f"Month: {month}")
print(f"Total properties in JSON: {len(listings)}")

# Generate the CSV filename
csv_filename = f"rental_data_{month}_complete.csv"

# Write the CSV file
with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['size', 'num_rooms', 'rent_price', 'location', 'url']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for prop in listings:
        # Only write the specific fields we're interested in
        writer.writerow({
            'size': prop.get('size', ''),
            'num_rooms': prop.get('num_rooms', ''),
            'rent_price': prop.get('rent_price', ''),
            'location': prop.get('location', ''),
            'url': prop.get('url', '')
        })

print(f"Updated CSV file generated: {csv_filename}")
print(f"Contains {len(listings)} rental properties")

# Now let's check the existing CSV to see what's different
print("\nChecking existing rental_data CSV...")
existing_csv = f"rental_data_{month}.csv"
existing_urls = set()

try:
    with open(existing_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        existing_urls = {row['url'] for row in rows if 'url' in row}
    
    print(f"Properties in existing CSV: {len(rows)}")
    print(f"Unique URLs in existing CSV: {len(existing_urls)}")
    
    # Find properties in JSON that aren't in the CSV
    json_urls = {prop['url'] for prop in listings if 'url' in prop}
    missing_urls = json_urls - existing_urls
    
    print(f"Properties missing from CSV: {len(missing_urls)}")
    
    if len(missing_urls) > 0:
        print("\nExample missing properties:")
        count = 0
        for prop in listings:
            if prop.get('url', '') in missing_urls:
                print(f"  {prop.get('url', '')}: {prop.get('size', '')}, {prop.get('num_rooms', '')}, {prop.get('rent_price', '')}")
                count += 1
                if count >= 5:
                    break
except Exception as e:
    print(f"Error checking existing CSV: {str(e)}")

print("\nDone!") 