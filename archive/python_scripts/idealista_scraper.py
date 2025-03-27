import requests
import json
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define log message function first
def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Determine which URL to use based on scan mode
scan_mode = os.getenv("SCAN_MODE", "recent").lower()

# Use the URL directly from the environment without modifying
if scan_mode == "full":
    TARGET_URL = os.getenv("FULL_SCAN_URL", "https://www.idealista.pt/en/comprar-casas/lisboa/com-preco-max_300000,tamanho-min_40,t1,t2/")
elif scan_mode == "week":
    TARGET_URL = os.getenv("WEEK_SCAN_URL", "https://www.idealista.pt/en/comprar-casas/lisboa/com-preco-max_300000,tamanho-min_40,t1,t2,publicado_ultima-semana/")
else:  # Default to recent (last 48 hours)
    TARGET_URL = os.getenv("RECENT_SCAN_URL", "https://www.idealista.pt/en/comprar-casas/lisboa/com-preco-max_300000,tamanho-min_40,t1,t2,publicado_ultimas-48-horas/")

log_message(f"Using URL: {TARGET_URL}")

# Configuration
API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
BASE_API_URL = "https://app.scrapingbee.com/api/v1/"
OUTPUT_FILE = "idealista_listings.json"
CREDITS_USED_FILE = "credits_usage.json"

def load_stored_listings():
    """Load previously stored listings from JSON file."""
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        log_message(f"No existing file found at {OUTPUT_FILE}. Creating new dataset.")
        return []

def save_listings(listings):
    """Save listings to JSON file."""
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(listings, f, ensure_ascii=False, indent=2)
    log_message(f"Saved {len(listings)} listings to {OUTPUT_FILE}")

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
    log_message(f"Fetching page {page_num}: {url}")
    
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
        log_message("Using premium proxies for this request")
    
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

def extract_properties(html_content):
    """Extract property listings from HTML content."""
    properties = []
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Check if we have a "no results" page
    no_results_elem = soup.find("div", {"class": "empty-container"})
    if no_results_elem:
        log_message("No properties found (empty result set)")
        return properties, None
    
    listing_containers = soup.find_all("article", {"class": lambda x: x and "item" in x})
    
    log_message(f"Found {len(listing_containers)} listings on page")
    
    # If we have no listing containers but also no explicit "no results" message,
    # we should investigate what's on the page
    if len(listing_containers) == 0:
        log_message("Warning: No listing containers found in the page. This could be due to a change in page structure.")
        # Log the page title to help debugging
        page_title = soup.find("title")
        if page_title:
            log_message(f"Page title: {page_title.text.strip()}")
    
    for listing in listing_containers:
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
            
        price = price_elem.get_text(strip=True)
        details = detail_elem.get_text(strip=True) if detail_elem else ""
        
        # Try to extract location from title or details
        # This is a simplified approach - might need refinement based on actual data format
        location = ""
        if "in " in title:
            location = title.split("in ", 1)[1]
        
        property_record = {
            "title": title,
            "url": url,
            "price": price,
            "details": details,
            "location": location,
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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

def run_scraper():
    """Run the scraper to collect property listings."""
    log_message("Starting Idealista property scraper")
    
    # Load previously stored listings
    stored_listings = load_stored_listings()
    stored_map = {item['url']: item for item in stored_listings}
    initial_listing_count = len(stored_map)
    
    # Determine if this is a first run or subsequent run
    is_first_run = initial_listing_count == 0
    log_message(f"{'Initial run' if is_first_run else 'Subsequent run'} detected. Starting with {initial_listing_count} stored listings.")
    
    current_url = TARGET_URL
    new_found = 0
    updated_found = 0
    page_count = 0
    consecutive_existing_properties = 0
    max_consecutive_existing = 5  # If we see this many consecutive existing properties, we can stop
    max_pages = 10  # Safety limit to avoid excessive scraping
    
    while current_url and page_count < max_pages:
        page_count += 1
        html_content = fetch_page(current_url, page_count)
        
        if not html_content:
            log_message(f"Failed to fetch page {page_count}. Stopping.")
            break
        
        properties, next_page_url = extract_properties(html_content)
        
        # Track if we found any new properties on this page
        found_new_on_page = False
        
        for prop in properties:
            url = prop['url']
            
            if url in stored_map:
                # We've seen this property before
                consecutive_existing_properties += 1
                
                # Check if anything has changed
                old_prop = stored_map[url]
                if old_prop.get('price') != prop['price'] or old_prop.get('details') != prop['details']:
                    log_message(f"Updated property: {url} (price/details changed)")
                    stored_map[url] = prop
                    updated_found += 1
            else:
                # New property found
                log_message(f"New property found: {url}")
                stored_map[url] = prop
                new_found += 1
                found_new_on_page = True
                consecutive_existing_properties = 0  # Reset counter since we found a new one
        
        # For subsequent runs (not first run), if we've seen enough consecutive existing properties,
        # or found no new properties on this page, we can stop scraping
        if not is_first_run and (consecutive_existing_properties >= max_consecutive_existing or not found_new_on_page):
            log_message(f"Early exit: Found {consecutive_existing_properties} consecutive existing properties or no new properties on page.")
            break
            
        current_url = next_page_url
        
        if current_url:
            log_message(f"Moving to next page: {current_url}")
            # Increase delay between pages to reduce chance of blocking
            delay = 5 + (page_count * 2)  # Gradually increase delay with each page
            log_message(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)  # More polite delay before fetching next page
    
    # Save updated data
    updated_list = list(stored_map.values())
    save_listings(updated_list)
    
    log_message(f"Scraping completed: {new_found} new and {updated_found} updated properties found")
    log_message(f"Total properties in database: {len(updated_list)}")
    return new_found, updated_found

if __name__ == "__main__":
    run_scraper() 