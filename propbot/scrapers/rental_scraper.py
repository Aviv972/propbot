import requests
import json
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from pathlib import Path
import psycopg2
from psycopg2 import extras
from decimal import Decimal

# Import our environment loader to ensure DATABASE_URL is available
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

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

# Get the correct paths using Path for cross-platform compatibility
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
RENTALS_DIR = RAW_DIR / "rentals"
HISTORY_DIR = RENTALS_DIR / "history"

# Use /tmp directory for Heroku deployments
TMP_DIR = Path('/tmp')
TMP_RENTALS_DIR = TMP_DIR / "propbot_rentals"
TMP_HISTORY_DIR = TMP_RENTALS_DIR / "history"

# Debug prints for paths
log_message(f"DEBUG: BASE_DIR absolute path: {os.path.abspath(BASE_DIR)}")
log_message(f"DEBUG: DATA_DIR absolute path: {os.path.abspath(DATA_DIR)}")
log_message(f"DEBUG: RAW_DIR absolute path: {os.path.abspath(RAW_DIR)}")
log_message(f"DEBUG: RENTALS_DIR absolute path: {os.path.abspath(RENTALS_DIR)}")
log_message(f"DEBUG: Current working directory: {os.getcwd()}")
log_message(f"DEBUG: Using TMP_RENTALS_DIR: {os.path.abspath(TMP_RENTALS_DIR)}")

# Ensure directories exist
RENTALS_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_DIR.mkdir(parents=True, exist_ok=True)
TMP_RENTALS_DIR.mkdir(parents=True, exist_ok=True)
TMP_HISTORY_DIR.mkdir(parents=True, exist_ok=True)

# Set output paths
OUTPUT_FILE = TMP_RENTALS_DIR / "rental_listings.json"
CREDITS_USED_FILE = TMP_RENTALS_DIR / "rental_credits_usage.json"

# Debug print for output file
log_message(f"DEBUG: OUTPUT_FILE absolute path: {os.path.abspath(OUTPUT_FILE)}")

# Database connection function
def get_connection():
    """Get a database connection"""
    # Get DATABASE_URL from environment
    db_url = os.environ.get('DATABASE_URL')
    
    if not db_url:
        log_message("No database URL found in environment variables")
        return None
    
    # Add sslmode=require if not already present in the URL
    if 'sslmode=' not in db_url:
        db_url += ('&' if '?' in db_url else '?') + 'sslmode=require'
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        log_message(f"Error connecting to database: {str(e)}")
        return None

def load_stored_listings():
    """Load rental property listings directly from the database."""
    try:
        # Get database connection
        conn = get_connection()
        if not conn:
            log_message("Could not connect to database")
            # Fall back to JSON file if database connection fails
            try:
                with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                    listings = json.load(f)
                    log_message(f"Loaded {len(listings)} rental listings from JSON file (database connection failed)")
                    return listings
            except FileNotFoundError:
                log_message(f"No existing file found at {OUTPUT_FILE}. Creating new dataset.")
                return []
            
        listings = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, url, title, price, size, rooms, 
                    price_per_sqm, location, neighborhood,
                    details, snapshot_date, first_seen_date,
                    created_at, updated_at
                FROM properties_rentals
                ORDER BY snapshot_date DESC
            """)
            
            for row in cur.fetchall():
                # Convert database row to property record format
                property_record = {
                    "title": row['title'],
                    "url": row['url'],
                    "price": float(row['price']) if row['price'] is not None else None,
                    "price_str": f"€{float(row['price']):,.0f}" if row['price'] is not None else "",
                    "details": row['details'],
                    "location": row['location'],
                    "last_updated": row['updated_at'].strftime("%Y-%m-%d %H:%M:%S") if row['updated_at'] else None,
                    "first_seen_date": row['first_seen_date'].strftime("%Y-%m-%d %H:%M:%S") if row['first_seen_date'] else None
                }
                listings.append(property_record)
                
        log_message(f"Loaded {len(listings)} rental listings from database")
        
        # Save to JSON file for backup/historical purposes
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(listings, f, ensure_ascii=False, indent=2)
        log_message(f"Saved database rental listings to {OUTPUT_FILE} for backup")
        
        return listings
    except Exception as e:
        log_message(f"Error loading rental listings from database: {str(e)}")
        # Fall back to JSON file if any error occurs
        try:
            with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                listings = json.load(f)
                log_message(f"Loaded {len(listings)} rental listings from JSON file (after database error)")
                return listings
        except FileNotFoundError:
            log_message(f"No existing file found at {OUTPUT_FILE}. Creating new dataset.")
            return []
    finally:
        # Ensure connection is closed
        if 'conn' in locals() and conn:
            conn.close()

def save_listings(listings):
    """Save listings to JSON file."""
    log_message(f"DEBUG: Attempting to save to absolute path: {os.path.abspath(OUTPUT_FILE)}")
    log_message(f"DEBUG: Directory exists: {os.path.isdir(os.path.dirname(OUTPUT_FILE))}")
    log_message(f"DEBUG: Current working directory: {os.getcwd()}")
    log_message(f"DEBUG: File parents: {OUTPUT_FILE.parent}")
    
    # Create directory again just in case
    try:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        log_message(f"DEBUG: Directory created/confirmed: {os.path.dirname(OUTPUT_FILE)}")
    except Exception as e:
        log_message(f"DEBUG: Error creating directory: {str(e)}")
    
    try:
        # First try to write to a temporary file to check permissions
        temp_file = Path(os.path.dirname(OUTPUT_FILE)) / "temp_test.txt"
        with open(temp_file, "w") as f:
            f.write("Test write permission")
        log_message(f"DEBUG: Successfully wrote to temp file at {temp_file}")
        os.remove(temp_file)
        log_message(f"DEBUG: Successfully removed temp file")
    except Exception as e:
        log_message(f"DEBUG: Error with test file: {str(e)}")
    
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(listings, f, indent=2, ensure_ascii=False)
        log_message(f"DEBUG: Successfully wrote data to file")
        log_message(f"Saved {len(listings)} rental listings to {OUTPUT_FILE}")
        log_message(f"DEBUG: File exists after save: {os.path.exists(OUTPUT_FILE)}")
        log_message(f"DEBUG: File size after save: {os.path.getsize(OUTPUT_FILE)} bytes")
        
        # Create a historical snapshot
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        history_file = TMP_HISTORY_DIR / f"rental_listings_{timestamp}.json"
        
        # Create history directory again just in case
        try:
            os.makedirs(os.path.dirname(history_file), exist_ok=True)
            log_message(f"DEBUG: History directory created/confirmed: {os.path.dirname(history_file)}")
        except Exception as e:
            log_message(f"DEBUG: Error creating history directory: {str(e)}")
        
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(listings, f, indent=2, ensure_ascii=False)
        log_message(f"Created historical snapshot at {history_file}")
        log_message(f"DEBUG: History file exists after save: {os.path.exists(history_file)}")
        log_message(f"DEBUG: History file size after save: {os.path.getsize(history_file)} bytes")
    except Exception as e:
        log_message(f"DEBUG: ERROR saving listings: {str(e)}")
        import traceback
        log_message(f"DEBUG: Stack trace: {traceback.format_exc()}")
        log_message(f"DEBUG: File permissions on directory: {os.access(os.path.dirname(OUTPUT_FILE), os.W_OK)}")

def load_credits_usage():
    """Load credits usage data from JSON file."""
    try:
        with open(CREDITS_USED_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"total_used": 0, "requests": []}

def update_credits_usage(credits_used):
    """Update credits usage data in JSON file."""
    usage_data = load_credits_usage()
    
    request_data = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "credits": credits_used
    }
    
    usage_data["requests"].append(request_data)
    usage_data["total_used"] += credits_used
    
    with open(CREDITS_USED_FILE, "w", encoding="utf-8") as f:
        json.dump(usage_data, f, indent=2)
    
    log_message(f"Updated credits usage: +{credits_used}, total: {usage_data['total_used']}")

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
        
        # Get current timestamp for snapshot_date
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
        # Create rental property record with the specific fields requested
        property_record = {
            "size": size,
            "num_rooms": num_rooms,
            "rent_price": rent_price,
            "location": location,
            "url": url,
            "details": details,  # Keep the full details for reference
            "title": title,      # Keep the title for reference
            "snapshot_date": current_time,
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

def run_rental_scraper():
    """Run the scraper to collect rental property listings for monthly analysis."""
    log_message("Starting Idealista rental property scraper")
    
    # Load previously stored listings
    stored_listings = load_stored_listings()
    
    # Track existing URLs to avoid duplicates
    existing_urls = {listing.get('url', '') for listing in stored_listings}
    log_message(f"Found {len(existing_urls)} unique URLs in existing data")
    
    # For rental analysis, we want to create a new monthly snapshot rather than updating the old one
    # So we'll keep the old data for historical analysis but focus on the new data
    
    # Create a new list for this month's rental data
    current_month = datetime.now().strftime("%Y-%m")
    monthly_listings = []
    
    current_url = TARGET_URL
    page_count = 0
    # Get max_pages from environment variable or use default
    max_pages = int(os.environ.get("MAX_RENTAL_PAGES", 32))
    log_message(f"Setting max_pages to {max_pages} from environment variable")
    consecutive_existing = 0
    max_consecutive_existing = 5
    
    while current_url and page_count < max_pages:
        page_count += 1
        html_content = fetch_page(current_url, page_count)
        
        if not html_content:
            log_message(f"Failed to fetch rental page {page_count}. Stopping.")
            break
        
        properties, next_page_url = extract_rental_properties(html_content)
        
        # Add only new properties to this month's dataset
        new_on_page = 0
        for prop in properties:
            prop_url = prop.get('url', '')
            if prop_url and prop_url not in existing_urls:
                monthly_listings.append(prop)
                existing_urls.add(prop_url)
                new_on_page += 1
                consecutive_existing = 0  # Reset consecutive counter
            else:
                consecutive_existing += 1
        
        log_message(f"Found {new_on_page} new rental properties on page {page_count}")
        
        # If we've seen too many consecutive existing properties, stop
        if consecutive_existing >= max_consecutive_existing and len(monthly_listings) > 0:
            log_message(f"Found {consecutive_existing} consecutive existing properties. Stopping to avoid unnecessary requests.")
            break
            
        current_url = next_page_url
        
        if current_url:
            log_message(f"Moving to next rental page: {current_url}")
            # Increase delay between pages to reduce chance of blocking
            delay = 5 + (page_count * 1)  # Gradually increase delay with each page
            log_message(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)  # More polite delay before fetching next page
    
    # Update the stored rental data with this month's listings
    if len(monthly_listings) > 0:
        log_message(f"Rental scraping completed: {len(monthly_listings)} properties collected for {current_month}")
        
        # Add this month's data to the stored listings
        monthly_data = {
            "month": current_month,
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "listings": monthly_listings
        }
        
        # Check if we already have data for this month and update or add accordingly
        month_exists = False
        for i, entry in enumerate(stored_listings):
            if isinstance(entry, dict) and entry.get('month') == current_month:
                # Update existing month's data
                stored_listings[i] = monthly_data
                month_exists = True
                break
                
        if not month_exists:
            # Add new month's data
            stored_listings.append(monthly_data)
            
        # Save updated data
        log_message(f"DEBUG: About to save {len(stored_listings)} rental listings")
        for i, entry in enumerate(stored_listings):
            if isinstance(entry, dict) and 'month' in entry:
                log_message(f"DEBUG: Entry {i}: month={entry['month']}, listings count={len(entry.get('listings', []))}")
        
        save_listings(stored_listings)
        
        # Check for tmp directory contents
        check_tmp_dir_contents()
        
        # Check if save was successful
        try:
            log_message(f"DEBUG: Verifying saved data...")
            if os.path.exists(OUTPUT_FILE):
                file_size = os.path.getsize(OUTPUT_FILE)
                log_message(f"DEBUG: Output file exists with size: {file_size} bytes")
                
                # Try to read back the file
                with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                    loaded_data = json.load(f)
                log_message(f"DEBUG: Successfully read back the saved file with {len(loaded_data)} entries")
            else:
                log_message(f"DEBUG: WARNING - Output file does not exist after save attempt")
        except Exception as e:
            log_message(f"DEBUG: Error verifying saved data: {str(e)}")
        
        # Generate updated CSV files
        generate_monthly_csv()
        
        # Copy files to persistent storage
        copy_files_to_persistent_storage()
        
        return len(monthly_listings)
    else:
        log_message("No new rental listings found.")
        return 0

def generate_monthly_csv():
    """Generate CSV files with monthly data for analysis."""
    stored_listings = load_stored_listings()
    
    # Check if we have a list of monthly entries
    if not stored_listings or not isinstance(stored_listings, list):
        log_message("No rental data suitable for CSV generation")
        return
    
    # Get unique months from the data
    all_months = set()
    for entry in stored_listings:
        if isinstance(entry, dict) and 'month' in entry:
            all_months.add(entry['month'])
    
    # Generate CSV for each month
    for month in all_months:
        entries = [entry for entry in stored_listings if isinstance(entry, dict) and entry.get('month') == month]
        
        if not entries:
            continue
            
        total_properties = 0
        all_listings = []
        
        for entry in entries:
            if 'listings' in entry and isinstance(entry['listings'], list):
                all_listings.extend(entry['listings'])
                total_properties += len(entry['listings'])
        
        if not all_listings:
            continue
            
        csv_filename = TMP_RENTALS_DIR / f"rental_data_{month}.csv"
        
        # Generate CSV with the combined listings
        try:
            import csv
            with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                # Get field names from the first listing
                fieldnames = ['size', 'num_rooms', 'rent_price', 'location', 'url', 'details', 'title', 'snapshot_date']
                
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
                for listing in all_listings:
                    # Ensure we only write the fields we want
                    row = {field: listing.get(field, '') for field in fieldnames}
                    writer.writerow(row)
                    
            log_message(f"CSV file generated with {total_properties} rental properties")
        except Exception as e:
            log_message(f"Error generating CSV file: {e}")
        
    log_message(f"Total months in rental database: {len(all_months)}")

def check_tmp_dir_contents():
    """Log the contents of the temporary directory."""
    try:
        log_message(f"DEBUG: Checking contents of TMP_RENTALS_DIR: {os.path.abspath(TMP_RENTALS_DIR)}")
        
        # List files in the rentals tmp directory
        if os.path.exists(TMP_RENTALS_DIR):
            files = os.listdir(TMP_RENTALS_DIR)
            log_message(f"DEBUG: Files in TMP_RENTALS_DIR: {files}")
            
            # Check size of the main output file
            if os.path.exists(OUTPUT_FILE):
                file_size = os.path.getsize(OUTPUT_FILE)
                log_message(f"DEBUG: Main output file size: {file_size} bytes")
                
                # Try to read the first few entries
                try:
                    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        num_entries = len(data)
                        log_message(f"DEBUG: Number of entries in main output file: {num_entries}")
                except Exception as e:
                    log_message(f"DEBUG: Error reading main output file: {str(e)}")
            else:
                log_message(f"DEBUG: Main output file does not exist")
        else:
            log_message(f"DEBUG: TMP_RENTALS_DIR does not exist")
            
        # Check the history directory
        if os.path.exists(TMP_HISTORY_DIR):
            history_files = os.listdir(TMP_HISTORY_DIR)
            log_message(f"DEBUG: Files in TMP_HISTORY_DIR: {history_files}")
        else:
            log_message(f"DEBUG: TMP_HISTORY_DIR does not exist")
    except Exception as e:
        log_message(f"DEBUG: Error checking tmp directory contents: {str(e)}")
        import traceback
        log_message(f"DEBUG: Stack trace: {traceback.format_exc()}")

def copy_files_to_persistent_storage():
    """Copy files from temporary storage to the actual data directories."""
    try:
        log_message(f"DEBUG: Copying files from temporary to persistent storage")
        
        # Ensure target directories exist
        RENTALS_DIR.mkdir(parents=True, exist_ok=True)
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        
        # Copy main output file
        if os.path.exists(OUTPUT_FILE):
            persistent_file = RENTALS_DIR / "rental_listings.json"
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
                source_file = TMP_HISTORY_DIR / filename
                target_file = HISTORY_DIR / filename
                
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
        
        # Copy CSV files if they exist
        csv_files = [f for f in os.listdir(TMP_RENTALS_DIR) if f.endswith('.csv')]
        for filename in csv_files:
            source_file = TMP_RENTALS_DIR / filename
            target_file = RENTALS_DIR / filename
            
            log_message(f"DEBUG: Copying CSV file {source_file} to {target_file}")
            
            # Read from temp and write to persistent
            with open(source_file, "r", encoding="utf-8") as source:
                content = source.read()
                with open(target_file, "w", encoding="utf-8") as target:
                    target.write(content)
            
            if os.path.exists(target_file):
                log_message(f"DEBUG: Successfully copied CSV file ({os.path.getsize(target_file)} bytes)")
            else:
                log_message(f"DEBUG: Failed to copy CSV file")
                
        log_message(f"DEBUG: Completed copying files to persistent storage")
        return True
    except Exception as e:
        log_message(f"DEBUG: Error copying files to persistent storage: {str(e)}")
        import traceback
        log_message(f"DEBUG: Stack trace: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    run_rental_scraper() 