import requests
import json
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import re

# Re-use functions from rental_scraper to maintain consistency
from rental_scraper import (
    log_message, load_stored_listings, save_listings, 
    load_credits_usage, update_credits_usage, 
    extract_rental_properties, generate_monthly_csv
)

# Load environment variables from .env file
load_dotenv()

# Configuration
API_KEY = os.getenv("SCRAPINGBEE_API_KEY")
BASE_API_URL = "https://app.scrapingbee.com/api/v1/"
OUTPUT_FILE = "rental_listings.json"
CREDITS_USED_FILE = "rental_credits_usage.json"

# Target URL and page range
BASE_URL = "https://www.idealista.pt/en/arrendar-casas/lisboa/com-tamanho-min_40,t1,t2,publicado_ultima-semana/"
START_PAGE = 15
END_PAGE = 20

def construct_page_url(base_url, page_num):
    """Construct a URL for a specific page."""
    if page_num == 1:
        return base_url
    else:
        # Check if base_url ends with a slash and remove it
        if base_url.endswith('/'):
            base_url = base_url[:-1]
        return f"{base_url}/pagina-{page_num}"

def fetch_page(url, page_num):
    """Fetch a page from Idealista using ScrapingBee API."""
    log_message(f"Fetching rental page {page_num}: {url}")
    
    # Configure premium proxies based on environment variable
    use_premium_proxies = os.getenv("USE_PREMIUM_PROXIES", "False").lower() == "true"
    premium_proxy = "true" if use_premium_proxies else "false"
    
    # Prepare request parameters
    params = {
        "api_key": API_KEY,
        "url": url,
        "premium_proxy": premium_proxy,
        "country_code": "pt",
        "wait": 5000,  # Wait 5 seconds for page to load
        "render_js": "false"
    }
    
    # Add logging for premium proxy status
    log_message(f"Using premium proxies: {premium_proxy}")
    
    try:
        response = requests.get(BASE_API_URL, params=params)
        
        # Extract credits used
        credits_used = response.headers.get('X-Credit-Used', '0')
        try:
            credits_used = int(credits_used)
        except ValueError:
            credits_used = 0
        
        # Update credit usage
        update_credits_usage(credits_used)
        log_message(f"Used {credits_used} credits for page {page_num}")
        
        if response.status_code == 200:
            log_message(f"Successfully fetched page {page_num}")
            return response.text
        else:
            log_message(f"Failed to fetch page {page_num}: Status {response.status_code}")
            log_message(f"Response: {response.text[:200]}...")
            return None
            
    except Exception as e:
        log_message(f"Error fetching page {page_num}: {str(e)}")
        return None

def scrape_additional_pages():
    """Scrape additional pages (15-20) and add to existing listings."""
    log_message(f"Starting additional rental scraping for pages {START_PAGE}-{END_PAGE}")
    
    # Load existing listings
    stored_listings = load_stored_listings()
    log_message(f"Loaded {len(stored_listings)} existing rental listings")
    
    # Track existing URLs to avoid duplicates
    existing_urls = {listing.get('url', '') for listing in stored_listings}
    log_message(f"Found {len(existing_urls)} unique URLs in existing data")
    
    new_listings = []
    
    # Scrape each page in the specified range
    for page_num in range(START_PAGE, END_PAGE + 1):
        page_url = construct_page_url(BASE_URL, page_num)
        html_content = fetch_page(page_url, page_num)
        
        if not html_content:
            log_message(f"Failed to fetch page {page_num}. Skipping.")
            continue
        
        properties, _ = extract_rental_properties(html_content)
        log_message(f"Extracted {len(properties)} properties from page {page_num}")
        
        # Add only new properties
        new_on_page = 0
        for prop in properties:
            if prop.get('url', '') not in existing_urls:
                new_listings.append(prop)
                existing_urls.add(prop.get('url', ''))
                new_on_page += 1
        
        log_message(f"Found {new_on_page} new properties on page {page_num}")
        
        # Add a delay between requests
        if page_num < END_PAGE:
            delay = 5  # 5 seconds between pages
            log_message(f"Waiting {delay} seconds before next request...")
            time.sleep(delay)
    
    # Add new listings to stored listings
    combined_listings = stored_listings + new_listings
    log_message(f"Added {len(new_listings)} new rental listings to the dataset")
    
    # Save updated listings
    save_listings(combined_listings)
    
    # Update monthly CSV
    current_month = datetime.now().strftime("%Y-%m")
    generate_monthly_csv(combined_listings, current_month)
    
    log_message(f"Additional rental scraping complete. Total listings: {len(combined_listings)}")
    return len(new_listings)

if __name__ == "__main__":
    new_listing_count = scrape_additional_pages()
    log_message(f"Script completed. Added {new_listing_count} new rental listings.") 