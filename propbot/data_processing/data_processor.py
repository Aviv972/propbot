#!/usr/bin/env python3
"""
PropBot Data Processor

This module provides a centralized data processing pipeline for PropBot.
It handles loading, merging, and processing of property data from both
sales and rental sources, and generates consolidated CSV files.
"""

import json
import logging
import os
import csv
import shutil
import numpy as np
import pandas as pd
import re
from datetime import datetime
from pathlib import Path
from ..config import load_config
from ..utils.extraction_utils import extract_size as extract_size_robust, extract_room_type

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("propbot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load configuration
config = load_config()

# Set up paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = SCRIPT_DIR.parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Ensure directories exist
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
(RAW_DIR / "sales").mkdir(parents=True, exist_ok=True)
(RAW_DIR / "rentals").mkdir(parents=True, exist_ok=True)

# Define file paths
SALES_RAW_FILE = RAW_DIR / "sales" / "idealista_listings.json"
RENTAL_RAW_FILE = RAW_DIR / "rentals" / "rental_listings.json"
SALES_CSV_FILE = PROCESSED_DIR / "sales_current.csv"  # Updated to match the expected filename
RENTAL_CSV_FILE = PROCESSED_DIR / "rentals.csv"
METADATA_FILE = PROCESSED_DIR / "metadata.json"

def extract_size(text):
    """
    Extract size in square meters from details text.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_size instead.
    
    Args:
        text: Text that may contain size information
        
    Returns:
        Extracted size as float or 0 if not found
    """
    if pd.isna(text) or not isinstance(text, str):
        return 0
        
    # First try to extract room type for context
    room_type = extract_room_type(text)
    
    # Use the robust implementation from extraction_utils with room type context
    size, confidence = extract_size_robust(text, room_type)
    
    if not confidence:
        logger.warning(f"Low confidence size extraction: '{text}' -> {size}. Room type: {room_type}")
    
    return size if size is not None else 0

def extract_room_type(details_str):
    """
    Extract room type (T0, T1, T2, etc.) from a details string.
    
    Args:
        details_str: String containing details about the property
        
    Returns:
        Room type string or None if not found
    """
    if not details_str:
        return None
        
    # Look for room type pattern
    room_match = re.search(r'T([0-4])', details_str)
    if room_match:
        return f"T{room_match.group(1)}"
        
    return None

def extract_price(price_str):
    """
    Extract numeric price value from a price string.
    
    Args:
        price_str: String containing price information (e.g., "275,000€", "1,400€/month")
        
    Returns:
        Float price value or None if parsing fails
    """
    if not price_str:
        return None
    
    # Log the raw price string for debugging
    logger.debug(f"Extracting price from: '{price_str}'")
        
    try:
        # Check if it's a rental price with €/month format
        is_rental = '/month' in price_str or '€/month' in price_str
        
        # Remove currency symbols and spaces first
        clean_price = price_str.replace('€', '').replace('EUR', '').replace(' ', '')
        
        # For sales listings, first try to extract a complete price with thousands separator
        if not is_rental:
            # Handle prices like 275.000 or 275,000
            price_match = re.search(r'(\d+)[\.,](\d{3})[\.,]?(\d+)?', clean_price)
            if price_match:
                # We have a full price with thousands separator
                if price_match.group(3):  # Has three groups: thousands, hundreds, decimals
                    price_value = float(f"{price_match.group(1)}{price_match.group(2)}.{price_match.group(3)}")
                else:  # Just thousands and hundreds
                    price_value = float(f"{price_match.group(1)}{price_match.group(2)}")
                logger.debug(f"Extracted price with thousands separator: {price_value}")
                return price_value
        
        # Extract the numeric part of the price (for both rental and sales if above didn't match)
        price_match = re.search(r'(\d+(?:[\.,]\d+)?)', clean_price)
        if price_match:
            # Replace comma with dot for decimal and convert to float
            price_clean = price_match.group(1).replace(',', '.')
            price_value = float(price_clean)
            
            # Fix the issue where rental prices like 1.40 should be 1400
            # If it's a rental price and the value is small, multiply by 1000
            if is_rental and price_value < 100:
                price_value = price_value * 1000
                logger.debug(f"Small rental price detected, adjusted to: {price_value}")
            
            # For sales listings, if the price is too small (less than 1000),
            # it's likely in thousands and needs to be multiplied
            elif not is_rental and price_value < 1000:
                price_value = price_value * 1000
                logger.debug(f"Small sales price detected, adjusted to: {price_value}")
            
            logger.debug(f"Extracted price: {price_value}")
            return price_value
            
        logger.warning(f"Failed to extract price from: '{price_str}'")
    except (ValueError, TypeError) as e:
        logger.warning(f"Error parsing price '{price_str}': {e}")
        
    return None

def extract_location(title_or_location):
    """
    Extract a clean location string from a title or location string.
    
    Args:
        title_or_location: String containing location information
        
    Returns:
        Cleaned location string
    """
    if not title_or_location:
        return ""
        
    # If it's a title, extract the location part
    if "in " in title_or_location:
        return title_or_location.split("in ", 1)[1]
        
    return title_or_location

def load_raw_json(file_path):
    """
    Load raw JSON data from a file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded JSON data or None if loading fails
    """
    logger.info(f"Loading data from {file_path}")
    try:
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        logger.info(f"Successfully loaded data from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Error loading data from {file_path}: {e}")
        return None

def process_rental_listings(rental_data):
    """
    Process rental listings from raw JSON data.
    
    Args:
        rental_data: Raw rental data from JSON
        
    Returns:
        List of processed rental properties
    """
    processed_rentals = []
    
    # Extract listings from the data structure
    listings = []
    if isinstance(rental_data, list):
        for item in rental_data:
            if isinstance(item, dict) and 'listings' in item:
                listings.extend(item['listings'])
            elif isinstance(item, dict) and all(key in item for key in ['size', 'num_rooms', 'rent_price']):
                listings.append(item)
    elif isinstance(rental_data, dict) and 'listings' in rental_data:
        listings = rental_data['listings']
    
    logger.info(f"Processing {len(listings)} rental listings")
    
    for listing in listings:
        # Skip if missing required fields
        if not listing.get('url'):
            continue
            
        # Extract size
        size_str = listing.get('size', '')
        size_value = extract_size(size_str)
        
        # Extract room type
        room_type = listing.get('num_rooms', '')
        if not room_type and 'details' in listing:
            room_type = extract_room_type(listing['details'])
            
        # Extract price
        price_str = listing.get('rent_price', '')
        price_value = extract_price(price_str)
        
        # Extract location
        location = listing.get('location', '')
        if not location and 'title' in listing:
            location = extract_location(listing['title'])
            
        # Create processed rental record
        rental = {
            'url': listing.get('url', ''),
            'price': price_value,
            'size': size_value,
            'room_type': room_type,
            'location': location,
            'is_rental': True,
            'details': listing.get('details', ''),
            'snapshot_date': listing.get('snapshot_date', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'first_seen_date': listing.get('first_seen_date', listing.get('snapshot_date', datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        }
        
        # Only add if we have the essential data
        if rental['url'] and rental['size'] and rental['price']:
            processed_rentals.append(rental)
    
    logger.info(f"Processed {len(processed_rentals)} valid rental listings")
    return processed_rentals

def process_sales_listings(sales_data):
    """
    Process sales listings from raw JSON data.
    
    Args:
        sales_data: Raw sales data from JSON
        
    Returns:
        List of processed sales properties
    """
    processed_sales = []
    
    # Extract listings from the data structure
    listings = []
    if isinstance(sales_data, list):
        listings = sales_data
    elif isinstance(sales_data, dict) and 'listings' in sales_data:
        listings = sales_data['listings']
    
    logger.info(f"Processing {len(listings)} sales listings")
    
    for listing in listings:
        # Skip if missing required fields
        if not listing.get('url'):
            continue
            
        # Extract size
        size_str = listing.get('details', '')
        size_value = extract_size(size_str)
        
        # Extract room type
        room_type = extract_room_type(listing.get('details', ''))
        
        # Extract price
        price_str = listing.get('price', '')
        price_value = extract_price(price_str)
        
        # Extract location
        location = listing.get('location', '')
        if not location and 'title' in listing:
            location = extract_location(listing['title'])
            
        # Extract price per square meter from Idealista
        price_per_sqm = listing.get('price_per_sqm', '')
        if price_per_sqm:
            try:
                price_per_sqm = float(price_per_sqm)
            except (ValueError, TypeError):
                # If conversion fails, calculate it from price and size
                if price_value and size_value and size_value > 0:
                    price_per_sqm = price_value / size_value
                else:
                    price_per_sqm = None
        else:
            # If no price_per_sqm provided, calculate it
            if price_value and size_value and size_value > 0:
                price_per_sqm = price_value / size_value
            else:
                price_per_sqm = None
            
        # Create processed sales record
        sale = {
            'url': listing.get('url', ''),
            'size': size_value,
            'room_type': room_type,
            'price': price_value,
            'location': location,
            'is_rental': False,
            'details': listing.get('details', ''),
            'price_per_sqm': price_per_sqm,
            'snapshot_date': listing.get('last_updated', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            'first_seen_date': listing.get('first_seen_date', listing.get('last_updated', datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        }
        
        # Only add if we have the essential data
        if sale['url'] and sale['size'] and sale['price']:
            processed_sales.append(sale)
    
    return processed_sales

def save_to_csv(data, file_path, backup=True):
    """
    Save processed data to a CSV file.
    
    Args:
        data: List of dictionaries to save
        file_path: Path to save the CSV file
        backup: Whether to create a backup of the existing file
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Saving {len(data)} records to {file_path}")
    
    # Create backup if file exists and backup is requested
    if backup and os.path.exists(file_path):
        backup_path = str(file_path) + f".backup.{datetime.now().strftime('%Y%m%d%H%M%S')}"
        shutil.copy2(file_path, backup_path)
        logger.info(f"Created backup at {backup_path}")
    
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Define columns order
        columns = [
            'url', 'price', 'size', 'room_type', 'location', 
            'is_rental', 'details', 'snapshot_date', 'price_per_sqm', 'first_seen_date'
        ]
        
        # Reorder columns and ensure all columns exist
        for col in columns:
            if col not in df.columns:
                df[col] = None
        
        # Save to CSV with only the specified columns
        df[columns].to_csv(file_path, index=False)
        
        logger.info(f"Successfully saved {len(df)} records to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving data to {file_path}: {e}")
        return False

def update_metadata(rental_count, sales_count):
    """
    Update the metadata file with processing information.
    
    Args:
        rental_count: Number of processed rental properties
        sales_count: Number of processed sales properties
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Load existing metadata or create new
        if os.path.exists(METADATA_FILE):
            with open(METADATA_FILE, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        else:
            metadata = {
                'created_at': datetime.now().isoformat(),
                'history': []
            }
        
        # Add new update record
        update_record = {
            'timestamp': datetime.now().isoformat(),
            'rental_count': rental_count,
            'sales_count': sales_count,
            'rental_file': str(RENTAL_CSV_FILE),
            'sales_file': str(SALES_CSV_FILE)
        }
        
        # Update metadata
        metadata['last_updated'] = update_record['timestamp']
        metadata['rental_count'] = rental_count
        metadata['sales_count'] = sales_count
        metadata['history'].append(update_record)
        
        # Save metadata
        with open(METADATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
            
        logger.info(f"Updated metadata: {rental_count} rentals, {sales_count} sales")
        return True
    except Exception as e:
        logger.error(f"Error updating metadata: {e}")
        return False

def process_data():
    """
    Process all property data and save to consolidated CSV files.
    
    Returns:
        True if successful, False otherwise
    """
    logger.info("Starting PropBot data processing")
    
    # Load raw data
    rental_data = load_raw_json(RENTAL_RAW_FILE)
    sales_data = load_raw_json(SALES_RAW_FILE)
    
    # Check if data loading was successful
    if rental_data is None and sales_data is None:
        logger.warning("No raw data found. Checking for existing processed files to update metadata.")
        # Check if there are existing processed files we can use
        if os.path.exists(RENTAL_CSV_FILE):
            rental_count = len(pd.read_csv(RENTAL_CSV_FILE))
            logger.info(f"Found existing rental data file with {rental_count} records")
            
            # Update metadata with the existing file information
            update_metadata(rental_count, 0)
            logger.info("Updated metadata with existing rental data")
            return True
        else:
            logger.error("No raw data or existing processed files found. Cannot proceed.")
            return False
    
    # Process data
    processed_rentals = []
    processed_sales = []
    
    if rental_data:
        processed_rentals = process_rental_listings(rental_data)
        logger.info(f"Processed {len(processed_rentals)} rental listings")
    
    if sales_data:
        processed_sales = process_sales_listings(sales_data)
        logger.info(f"Processed {len(processed_sales)} sales listings")
    
    # Save processed data
    rental_success = True
    sales_success = True
    
    if processed_rentals:
        rental_success = save_to_csv(processed_rentals, RENTAL_CSV_FILE)
    
    if processed_sales:
        sales_success = save_to_csv(processed_sales, SALES_CSV_FILE)
    
    # Update metadata
    if rental_success or sales_success:
        update_metadata(len(processed_rentals), len(processed_sales))
    
    logger.info("PropBot data processing completed")
    return rental_success and sales_success

if __name__ == "__main__":
    process_data() 