import requests
import json
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define log message function
def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Rental listings URL
TARGET_URL = "https://www.idealista.pt/en/arrendar-casas/lisboa/com-tamanho-min_40,t1,t2,publicado_ultima-semana/"

log_message(f"Using URL: {TARGET_URL}")

# Configuration
API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
BASE_API_URL = "https://app.scrapingbee.com/api/v1/"
OUTPUT_FILE = "rental_listings.json"
CREDITS_USED_FILE = "rental_credits_usage.json"

def load_stored_listings():
    """Load previously stored rental listings from JSON file."""
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log_message(f"No existing file found at {OUTPUT_FILE}. Creating new dataset.")
        return []

def save_listings(listings):
    """Save rental listings to JSON file."""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    log_message(f"Saved {len(listings)} rental listings to {OUTPUT_FILE}")

def load_credits_usage():
    """Load previously stored credits usage from JSON file."""
    try:
        with open(CREDITS_USED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"total_credits_used": 0, "usage_history": []}

def update_credits_usage(credits_used):
    """Update credits usage file with new data."""
    credits_data = load_credits_usage()
    
    # Update total
    credits_data["total_credits_used"] += credits_used
    
    # Add to history
    usage_entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "credits_used": credits_used
    }
    credits_data["usage_history"].append(usage_entry)
    
    # Save updated data
    with open(CREDITS_USED_FILE, "w", encoding="utf-8") as f:
        json.dump(credits_data, f, ensure_ascii=False, indent=2)
    
    log_message(f"Updated credits usage: +{credits_used}, total: {credits_data['total_credits_used']}")

def fetch_page(url, page_num=1):
    """Fetch a page using ScrapingBee API with optimized parameters."""
    log_message(f"Fetching rental page {page_num}: {url}")
    
    if not API_KEY:
        raise ValueError("ScrapingBee API key not found. Please set SCRAPINGBEE_API_KEY environment variable.")
    
    params = {
        "api_key": API_KEY,
        "url": url,
        "render_js": "true",        # Enable JS rendering to bypass anti-bot measures
        "block_resources": "false",  # Don't block resources as they might be needed
        "block_ads": "true",        # Block ads
        "wait": "5000",             # Wait 5 seconds for page to load
        "transparent_status_code": "true"  # Get original status codes
    }
    
    # Add premium proxies if specified in environment
    if os.getenv("USE_PREMIUM_PROXY", "false").lower() == "true":
        params["premium_proxy"] = "true"
        log_message("Using premium proxies for this rental request")
    
    try:
        response = requests.get(BASE_API_URL, params=params)
        
        # Log the API cost for monitoring
        credits_used = 0
        if 'Spb-cost' in response.headers:
            credits_used = float(response.headers['Spb-cost'])
            log_message(f"Request cost: {credits_used} credits")
            update_credits_usage(credits_used)
        
        # Log the resolved URL if there was a redirect
        if 'Spb-resolved-url' in response.headers:
            log_message(f"Resolved URL: {response.headers['Spb-resolved-url']}")
        
        if response.status_code == 200:
            return response.text
        else:
            log_message(f"Error {response.status_code}: {response.text}")
            return None
    except Exception as e:
        log_message(f"Exception during fetch: {str(e)}")
        return None

def extract_rental_properties(html_content):
    """Extract rental property listings from HTML content, focusing on size, rooms, price and location."""
    properties = []
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Check if we have a "no results" page
    no_results_elem = soup.find("div", {"class": "empty-container"})
    if no_results_elem:
        log_message("No rental properties found (empty result set)")
        return properties, None
    
    listing_containers = soup.find_all("article", {"class": lambda x: x and "item" in x})
    
    log_message(f"Found {len(listing_containers)} rental listings on page")
    
    # If we have no listing containers but also no explicit "no results" message,
    # we should investigate what's on the page
    if len(listing_containers) == 0:
        log_message("Warning: No rental listing containers found in the page. This could be due to a change in page structure.")
        # Log the page title to help debugging
        page_title = soup.find("title")
        if page_title:
            log_message(f"Page title: {page_title.text.strip()}")
    
    for listing in listing_containers:
        # Basic listing info
        title_elem = listing.find("a", {"class": "item-link"})
        price_elem = listing.find("div", {"class": "price-row"})
        detail_elem = listing.find("div", {"class": "item-detail-char"})
        
        if not title_elem or not price_elem:
            continue  # Skip if something is missing
        
        title = title_elem.get_text(strip=True)
        url = title_elem.get('href', '')
        
        # Ensure URL is absolute
        if url.startswith("/"):
            url = "https://www.idealista.pt" + url
            
        # Extract rent price
        rent_price = price_elem.get_text(strip=True)
        
        # Extract details text that contains size and rooms
        details = detail_elem.get_text(strip=True) if detail_elem else ""
        
        # Improved extraction of room numbers and size
        num_rooms = ""
        size = ""
        
        # First check for room type (T1, T2, T3, etc.)
        if "T1" in details:
            num_rooms = "T1"
        elif "T2" in details:
            num_rooms = "T2"
        elif "T3" in details:
            num_rooms = "T3"
        elif "T4" in details:
            num_rooms = "T4"
        elif "T0" in details:
            num_rooms = "T0"
        
        # Now extract size separately
        if "m²" in details:
            # Look for numbers followed by m²
            import re
            size_match = re.search(r'(\d+)\s*m²', details)
            if size_match:
                size = f"{size_match.group(1)} m²"
        
        # Extract location from title
        location = ""
        if "in " in title:
            location = title.split("in ", 1)[1]
        
        # Create rental property record with the specific fields requested
        property_record = {
            "size": size,
            "num_rooms": num_rooms,
            "rent_price": rent_price,
            "location": location,
            "url": url,
            "details": details,  # Keep the full details for reference
            "title": title,      # Keep the title for reference
            "snapshot_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        properties.append(property_record)
    
    # Find next page link if available
    next_link = soup.find("a", {"class": "icon-arrow-right-after"})
    next_page_url = None
    
    if next_link and next_link.get('href'):
        next_href = next_link['href']
        if next_href.startswith("/"):
            next_page_url = "https://www.idealista.pt" + next_href
    
    return properties, next_page_url

def run_rental_scraper():
    """Run the scraper to collect rental property listings for monthly analysis."""
    log_message("Starting Idealista rental property scraper")
    
    # Load previously stored listings
    stored_listings = load_stored_listings()
    
    # For rental analysis, we want to create a new monthly snapshot rather than updating the old one
    # So we'll keep the old data for historical analysis but focus on the new data
    
    # Create a new list for this month's rental data
    current_month = datetime.now().strftime("%Y-%m")
    monthly_listings = []
    
    current_url = TARGET_URL
    page_count = 20
    max_pages = 32  # We'll scan more pages for rentals to get a good sample size
    
    while current_url and page_count < max_pages:
        page_count += 1
        html_content = fetch_page(current_url, page_count)
        
        if not html_content:
            log_message(f"Failed to fetch rental page {page_count}. Stopping.")
            break
        
        properties, next_page_url = extract_rental_properties(html_content)
        
        # Add all properties to this month's dataset
        monthly_listings.extend(properties)
        log_message(f"Added {len(properties)} rental properties to monthly dataset")
            
        current_url = next_page_url
        
        if current_url:
            log_message(f"Moving to next rental page: {current_url}")
            # Increase delay between pages to reduce chance of blocking
            delay = 5 + (page_count * 2)  # Gradually increase delay with each page
            log_message(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)  # More polite delay before fetching next page
    
    # Add this month's data to the stored listings
    monthly_data = {
        "month": current_month,
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "listings": monthly_listings
    }
    
    # Check if we already have data for this month
    month_exists = False
    for i, month_data in enumerate(stored_listings):
        if month_data.get("month") == current_month:
            # Update existing month data
            stored_listings[i] = monthly_data
            month_exists = True
            break
    
    if not month_exists:
        # Add new month data
        stored_listings.append(monthly_data)
    
    # Save updated data
    save_listings(stored_listings)
    
    log_message(f"Rental scraping completed: {len(monthly_listings)} properties collected for {current_month}")
    log_message(f"Total months in rental database: {len(stored_listings)}")
    
    # Generate a monthly CSV for easy data analysis
    generate_monthly_csv(monthly_listings, current_month)
    
    return len(monthly_listings)

def generate_monthly_csv(listings, month):
    """Generate a CSV file with the month's rental data for easy analysis."""
    import csv
    
    csv_filename = f"rental_data_{month}.csv"
    log_message(f"Generating CSV file: {csv_filename}")
    
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
    
    log_message(f"CSV file generated with {len(listings)} rental properties")
    return csv_filename

if __name__ == "__main__":
    run_rental_scraper() 