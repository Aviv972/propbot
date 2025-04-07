import requests
import json
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from ...utils.extraction_utils import extract_price_improved

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

# API usage limits
MAX_API_CREDITS = int(os.getenv("MAX_API_CREDITS", "5000"))  # Default 5000 credits per run
API_CREDITS_SAFETY_MARGIN = 500  # Stop before hitting the absolute limit

# Define file paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "data")
RAW_SALES_DIR = os.path.join(DATA_DIR, "raw", "sales")
HISTORY_DIR = os.path.join(RAW_SALES_DIR, "history")

# Use /tmp directory for Heroku deployments
TMP_DIR = os.path.join("/tmp", "propbot_sales")
TMP_HISTORY_DIR = os.path.join(TMP_DIR, "history")

# Debug logs for paths
log_message(f"DEBUG: DATA_DIR: {DATA_DIR}")
log_message(f"DEBUG: RAW_SALES_DIR: {RAW_SALES_DIR}")
log_message(f"DEBUG: TMP_DIR: {TMP_DIR}")
log_message(f"DEBUG: Current working directory: {os.getcwd()}")

# Ensure directories exist
os.makedirs(RAW_SALES_DIR, exist_ok=True)
os.makedirs(HISTORY_DIR, exist_ok=True)
os.makedirs(TMP_DIR, exist_ok=True)
os.makedirs(TMP_HISTORY_DIR, exist_ok=True)

# Output file paths
OUTPUT_FILE = os.path.join(TMP_DIR, "idealista_listings.json")
CREDITS_USED_FILE = os.path.join(TMP_DIR, "credits_usage.json")

log_message(f"DEBUG: OUTPUT_FILE: {OUTPUT_FILE}")

# Import the database utilities
try:
    from propbot.database_utils import save_historical_sales_snapshot
    from propbot.data_processing.update_db import update_database_after_scrape
    HAS_DB_FUNCTIONS = True
except ImportError:
    HAS_DB_FUNCTIONS = False

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
    return credits_data["total_credits_used"]

def get_current_api_usage():
    """Get current API usage for the month."""
    try:
        response = requests.get(
            f"https://app.scrapingbee.com/api/v1/usage?api_key={API_KEY}"
        )
        if response.status_code == 200:
            data = response.json()
            credits_used = data.get("usage", {}).get("credit_used", 0)
            credit_limit = data.get("usage", {}).get("credit_limit", 0)
            log_message(f"Current API usage: {credits_used}/{credit_limit} credits")
            return credits_used, credit_limit
        else:
            log_message(f"Error getting API usage: {response.status_code}")
            return None, None
    except Exception as e:
        log_message(f"Exception getting API usage: {str(e)}")
        return None, None

def check_api_limit(session_usage=0):
    """Check if we're approaching API limits."""
    # Check current monthly usage
    monthly_usage, monthly_limit = get_current_api_usage()
    
    # If we couldn't get the usage, be cautious and check our session usage
    if monthly_usage is None:
        log_message(f"Couldn't get monthly API usage. Current session usage: {session_usage}")
        return session_usage < MAX_API_CREDITS
    
    # Check against monthly limits
    if monthly_limit and monthly_usage > (monthly_limit - API_CREDITS_SAFETY_MARGIN):
        log_message(f"WARNING: Monthly API credits limit approaching. Used {monthly_usage} of {monthly_limit}")
        return False
    
    # Also check session limit
    if session_usage > MAX_API_CREDITS:
        log_message(f"WARNING: Session API credits limit reached: {session_usage} > {MAX_API_CREDITS}")
        return False
    
    return True

def fetch_page(url, page_num=1, session_usage=0):
    """Fetch a page using ScrapingBee API with optimized parameters."""
    log_message(f"Fetching page {page_num}: {url}")
    
    # First check if we're approaching API limits
    if not check_api_limit(session_usage):
        log_message("API usage limit reached or approaching. Stopping requests.")
        return None
    
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
            total_used = update_credits_usage(credits_used)
            
            # Check if we've exceeded our session limit
            if total_used > MAX_API_CREDITS:
                log_message(f"WARNING: Session API credits limit exceeded: {total_used} > {MAX_API_CREDITS}")
                return None
        
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
            
        # Extract and process price
        raw_price = price_elem.get_text(strip=True)
        price = extract_price_improved(raw_price)
        log_message(f"Extracted price: {raw_price} -> {price}")
        
        details = detail_elem.get_text(strip=True) if detail_elem else ""
        
        # Try to extract location from title or details
        # This is a simplified approach - might need refinement based on actual data format
        location = ""
        if "in " in title:
            location = title.split("in ", 1)[1]
        
        # Get current timestamp for last_updated
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        property_record = {
            "title": title,
            "url": url,
            "price": price,  # Now storing the numeric price
            "details": details,
            "location": location,
            "last_updated": current_time
            # first_seen_date will be added when the property is first saved
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

def copy_files_to_persistent_storage():
    """Copy files from temporary storage to the actual data directories."""
    try:
        log_message(f"DEBUG: Copying files from temporary to persistent storage")
        
        # Ensure target directories exist
        os.makedirs(RAW_SALES_DIR, exist_ok=True)
        os.makedirs(HISTORY_DIR, exist_ok=True)
        
        # Copy main output file
        if os.path.exists(OUTPUT_FILE):
            persistent_file = os.path.join(RAW_SALES_DIR, "idealista_listings.json")
            log_message(f"DEBUG: Copying {OUTPUT_FILE} to {persistent_file}")
            
            # Read from temp and write to persistent
            with open(OUTPUT_FILE, "r", encoding="utf-8") as source:
                content = source.read()
                with open(persistent_file, "w", encoding="utf-8") as target:
                    target.write(content)
            
            if os.path.exists(persistent_file):
                log_message(f"DEBUG: Successfully copied to {persistent_file} ({os.path.getsize(persistent_file)} bytes)")
            else:
                log_message(f"DEBUG: Failed to copy to {persistent_file}")
        
        # Copy history files
        if os.path.exists(TMP_HISTORY_DIR):
            history_files = os.listdir(TMP_HISTORY_DIR)
            for filename in history_files:
                source_file = os.path.join(TMP_HISTORY_DIR, filename)
                target_file = os.path.join(HISTORY_DIR, filename)
                
                log_message(f"DEBUG: Copying history file {source_file} to {target_file}")
                
                # Read from temp and write to persistent
                with open(source_file, "r", encoding="utf-8") as source:
                    content = source.read()
                    with open(target_file, "w", encoding="utf-8") as target:
                        target.write(content)
                
                if os.path.exists(target_file):
                    log_message(f"DEBUG: Successfully copied history file ({os.path.getsize(target_file)} bytes)")
                else:
                    log_message(f"DEBUG: Failed to copy history file")
                
        log_message(f"DEBUG: Completed copying files to persistent storage")
        return True
    except Exception as e:
        log_message(f"DEBUG: Error copying files to persistent storage: {str(e)}")
        import traceback
        log_message(f"DEBUG: Stack trace: {traceback.format_exc()}")
        return False

def run_scraper():
    """Run the scraper to collect property listings."""
    log_message("Starting Idealista property scraper")
    
    # Check API usage before starting
    monthly_usage, monthly_limit = get_current_api_usage()
    if monthly_limit and monthly_usage > (monthly_limit - API_CREDITS_SAFETY_MARGIN):
        log_message(f"Monthly API calls limit reached: {monthly_usage}/{monthly_limit}")
        return 0
    
    # Load previously stored listings
    stored_listings = load_stored_listings()
    
    # Create a set of existing URLs for fast duplicate detection
    existing_urls = {item['url'] for item in stored_listings}
    initial_listing_count = len(existing_urls)
    
    # Determine if this is a first run or subsequent run
    is_first_run = initial_listing_count == 0
    log_message(f"{'Initial run' if is_first_run else 'Subsequent run'} detected. Starting with {initial_listing_count} stored listings.")
    
    current_url = TARGET_URL
    new_found = 0
    updated_found = 0
    page_count = 0
    consecutive_existing_properties = 0
    max_consecutive_existing = 5  # If we see this many consecutive existing properties, we can stop
    # Get max_pages from environment variable or use default
    max_pages = int(os.environ.get("MAX_SALES_PAGES", 10))
    log_message(f"Setting max_pages to {max_pages} from environment variable")
    
    # Track API usage in this session
    session_credits_used = 0
    
    # Setup page cache to avoid re-fetching the same pages
    page_cache = {}
    
    while current_url and page_count < max_pages:
        page_count += 1
        
        # Get the current session API usage 
        credits_data = load_credits_usage()
        session_credits_used = credits_data.get("total_credits_used", 0)
        
        # Check if we're approaching limits before making the request
        if not check_api_limit(session_credits_used):
            log_message("API usage limit reached. Stopping scraper.")
            break
        
        # Check if we already have this URL in cache
        if current_url in page_cache:
            log_message(f"Using cached content for page {page_count}: {current_url}")
            html_content = page_cache[current_url]
        else:
            # Add a longer delay between requests to be extra cautious with API limits
            if page_count > 1:
                delay = 5  # 5 seconds between pages
                log_message(f"Waiting {delay} seconds before next request...")
                time.sleep(delay)
                
            html_content = fetch_page(current_url, page_count, session_credits_used)
            
            # Cache the result if successful
            if html_content:
                page_cache[current_url] = html_content
        
        if not html_content:
            log_message(f"Failed to fetch page {page_count}. Stopping.")
            break
        
        properties, next_page_url = extract_properties(html_content)
        
        # Track if we found any new properties on this page
        found_new_on_page = False
        
        for prop in properties:
            url = prop['url']
            
            if url in existing_urls:
                # We've seen this property before
                consecutive_existing_properties += 1
                
                # Find the existing property and check if anything has changed
                for i, old_prop in enumerate(stored_listings):
                    if old_prop['url'] == url:
                        # Check if anything has changed
                        if old_prop.get('price') != prop['price'] or old_prop.get('details') != prop['details']:
                            log_message(f"Updated property: {url} (price/details changed)")
                            stored_listings[i] = prop  # Update with new information
                            updated_found += 1
                        break
            else:
                # New property found
                log_message(f"New property found: {url}")
                # Add first_seen_date when a property is discovered for the first time
                prop["first_seen_date"] = prop["last_updated"]
                stored_listings.append(prop)
                existing_urls.add(url)
                new_found += 1
                found_new_on_page = True
                consecutive_existing_properties = 0  # Reset counter since we found a new one
        
        if consecutive_existing_properties >= max_consecutive_existing and not is_first_run:
            log_message(f"Found {consecutive_existing_properties} consecutive existing properties. Stopping to avoid unnecessary requests.")
            break
        
        current_url = next_page_url
    
    log_message(f"Scraping completed: {new_found} new properties found, {updated_found} properties updated")
    log_message(f"Total properties: {len(stored_listings)}")
    
    # Save updated listings to file (keep as backup)
    save_listings(stored_listings)
    
    # Save a historical snapshot to the database
    if HAS_DB_FUNCTIONS:
        try:
            save_historical_sales_snapshot(stored_listings, new_found, updated_found)
            log_message("Saved historical snapshot to database")
            
            # Update database with new sales data
            update_database_after_scrape('sales')
            log_message("Updated database with latest sales data")
        except Exception as e:
            log_message(f"Error saving to database: {str(e)}")
            import traceback
            log_message(f"Stack trace: {traceback.format_exc()}")
    
    # Save a historical snapshot with timestamp (keep as file backup)
    if new_found > 0 or updated_found > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        history_file = os.path.join(TMP_HISTORY_DIR, f"idealista_listings_{timestamp}.json")
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(stored_listings, f, ensure_ascii=False, indent=2)
        log_message(f"Created historical snapshot at {history_file} with {len(stored_listings)} properties")
    
    # After saving to temp directory, copy files to persistent storage
    copy_files_to_persistent_storage()
    
    # Return only new_found to match expected signature in run_dashboard_server.py
    return new_found

if __name__ == "__main__":
    run_scraper() 