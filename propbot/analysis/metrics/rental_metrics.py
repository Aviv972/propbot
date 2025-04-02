import json
import csv
import re
import os
import statistics
from datetime import datetime
from fuzzywuzzy import fuzz
import logging
import numpy as np
from collections import defaultdict
import traceback
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Union
from statistics import mean, median
from sklearn.neighbors import BallTree
from ...utils.extraction_utils import extract_size as extract_size_robust, extract_room_type, validate_property_size
from ...config import CONFIG
# Add import for database utilities
from ...database_utils import get_connection

# Define default parameters here (originally in config.py)
DEFAULT_INVESTMENT_PARAMS = {
    "down_payment_rate": 0.20,  # 20% down payment
    "interest_rate": 0.035,  # 3.5% interest rate
    "loan_term_years": 30,  # 30-year mortgage
    "mortgage_type": "fixed",  # fixed rate mortgage
    "appreciation_rate": 0.03,  # 3% annual appreciation
    "rental_growth_rate": 0.02,  # 2% annual rent growth
    "expense_growth_rate": 0.025,  # 2.5% annual expense growth
    "vacancy_rate": 0.05,  # 5% vacancy
    "income_tax_rate": 0.28,  # 28% income tax
}

DEFAULT_EXPENSE_PARAMS = {
    "property_tax_rate": 0.005,  # 0.5% property tax
    "insurance_rate": 0.004,  # 0.4% insurance
    "maintenance_rate": 0.01,  # 1% maintenance
    "management_rate": 0.08,  # 8% property management
    "utilities": 0,  # No utilities
    "vacancy_rate": 0.08,  # 8% vacancy
    "closing_cost_rate": 0.03,  # 3% closing costs
    "renovation_cost_rate": 0.05,  # 5% renovation costs
}

# Import from the same package
from .rental_analysis import (
    log_message, 
    load_sales_data, 
    save_report_to_json, 
    save_report_to_csv
)

# Constants for analysis
MIN_COMPARABLE_PROPERTIES = 2  # Minimum number of comparable properties required
MAX_SALES_PRICE_PER_SQM = 10000  # Maximum sale price per square meter
MAX_RENTAL_PRICE_PER_SQM = 45  # Maximum rental price per square meter (monthly rent)

def get_rental_listings_from_database(max_price_per_sqm=MAX_RENTAL_PRICE_PER_SQM):
    """Query rental listings from the database.
    
    Args:
        max_price_per_sqm: Maximum acceptable price per square meter
        
    Returns:
        List of rental properties in standard format
    """
    logging.info("Querying rental listings from database...")
    
    filtered_rentals = []
    outliers = []
    
    conn = get_connection()
    if not conn:
        logging.error("Could not connect to database to fetch rental listings")
        return []
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Query the database for rental properties
                cur.execute("""
                    SELECT 
                        url, price, size, rooms, location, 
                        price_per_sqm, details, title
                    FROM 
                        properties_rentals 
                    WHERE 
                        price > 0 AND size > 0
                """)
                
                rows = cur.fetchall()
                logging.info(f"Retrieved {len(rows)} rental listings from database")
                
                # Convert rows to dictionaries
                for row in rows:
                    url, price, size, rooms, location, price_per_sqm, details, title = row
                    
                    # Skip invalid entries
                    if not (size > 0 and price > 0):
                        continue
                        
                    # Calculate price per sqm if not provided
                    if not price_per_sqm and size > 0:
                        price_per_sqm = price / size
                    
                    # Extract room type from details or rooms field
                    room_type = None
                    if details:
                        room_type = extract_room_type(details)
                    elif rooms:
                        room_type = f"T{rooms}"
                    
                    # Create rental property object
                    rental = {
                        'size': size,
                        'room_type': room_type,
                        'price': price,
                        'location': location,
                        'url': url,
                        'price_per_sqm': price_per_sqm
                    }
                    
                    # Filter based on price per sqm
                    if price_per_sqm > max_price_per_sqm:
                        outliers.append({
                            'url': url,
                            'price': price,
                            'size': size,
                            'price_per_sqm': price_per_sqm,
                            'location': location
                        })
                        continue
                        
                    filtered_rentals.append(rental)
        
        logging.info(f"Filtered out {len(outliers)} rental outliers with price per sqm > €{max_price_per_sqm}")
        logging.info(f"Retained {len(filtered_rentals)} rental properties from database after filtering")
        return filtered_rentals
        
    except Exception as e:
        logging.error(f"Error querying rental listings from database: {str(e)}")
        logging.error(traceback.format_exc())
        return []
    finally:
        conn.close()

def load_complete_rental_data(filename=None, max_price_per_sqm=MAX_RENTAL_PRICE_PER_SQM):
    """Load all rental data from the database or CSV file and perform filtering."""
    logging.info(f"Loading complete rental dataset...")
    
    # First try to get data from the database
    db_rentals = get_rental_listings_from_database(max_price_per_sqm)
    if db_rentals:
        logging.info(f"Successfully loaded {len(db_rentals)} rental properties from database")
        return db_rentals
    
    logging.info("No rental data found in database, falling back to file-based loading...")
    
    # If a specific filename was provided, use it
    if filename and os.path.exists(filename):
        logging.info(f"Using rental data file: {filename}")
    else:
        # Try different file paths, prioritizing our real data files
        possible_files = [
            # First try the real data files with current data
            "propbot/data/processed/rentals_current.csv",
            "data/processed/rentals_current.csv",
            "../../../data/processed/rentals_current.csv",
            # Then try the expected file from data_processor.py
            "propbot/data/processed/rentals.csv",
            "data/processed/rentals.csv",
            "../../../data/processed/rentals.csv",
            # Then fall back to legacy formats
            f"propbot/data/processed/rental_data_{datetime.now().strftime('%Y-%m')}.csv",
            f"data/processed/rental_data_{datetime.now().strftime('%Y-%m')}.csv",
            f"../../../data/processed/rental_data_{datetime.now().strftime('%Y-%m')}.csv",
            f"rental_data_{datetime.now().strftime('%Y-%m')}.csv",
            "propbot/data/processed/rental_data.csv",
            "data/processed/rental_data.csv",
            "../../../data/processed/rental_data.csv",
            "rental_data.csv"
        ]
        
        filename = None
        for file_path in possible_files:
            if os.path.exists(file_path):
                filename = file_path
                logging.info(f"Found rental data file: {filename}")
                break
        
        if not filename:
            logging.error("No rental data file found")
            return []
    
    try:
        # Try pandas first for better performance
        df = pd.read_csv(filename)
        logging.info(f"Loaded {len(df)} rows from {filename}")
        
        # Convert DataFrame to list of dictionaries
        rental_properties = []
        for _, row in df.iterrows():
            # Skip rows with missing required data
            if pd.isna(row.get('price', None)) or pd.isna(row.get('size', None)):
                continue
            
            price = float(row['price'])
            size = float(row['size'])
            
            # Skip invalid data
            if price <= 0 or size <= 0:
                continue
            
            # Calculate price per sqm
            price_per_sqm = price / size
            
            # Skip outliers
            if price_per_sqm > max_price_per_sqm:
                continue
            
            rental_property = {
                'url': row.get('url', ''),
                'price': price,
                'size': size,
                'rooms': row.get('rooms', None),
                'location': row.get('location', ''),
                'price_per_sqm': price_per_sqm,
                'details': row.get('details', ''),
                'title': row.get('title', '')
            }
            
            rental_properties.append(rental_property)
        
        logging.info(f"Loaded {len(rental_properties)} valid rental properties")
        return rental_properties
        
    except Exception as e:
        logging.error(f"Error loading rental data: {str(e)}")
        return []

def standardize_location(location_text):
    """Standardize location text for comparison.
    
    Args:
        location_text: Raw location text
        
    Returns:
        Standardized location text (lowercase, no special chars)
    """
    # Handle non-string inputs
    if location_text is None:
        return ""
    
    # Convert to string if not already a string
    if not isinstance(location_text, str):
        location_text = str(location_text)
    
    # Convert to lowercase
    text = location_text.lower()
    
    # Remove common address prefixes
    text = re.sub(r'\brua\b|\bavenue\b|\bavenida\b|\bpraca\b|\blargo\b', '', text)
    
    # Remove street numbers, floor indicators
    text = re.sub(r'\b\d+[a-z]?\b|\b\d+(st|nd|rd|th)\b', '', text)
    text = re.sub(r'\bfloor\b|\bandar\b|\bpiso\b', '', text)
    
    # Remove Portuguese stopwords
    text = re.sub(r'\bde\b|\bdo\b|\bda\b|\bdos\b|\bdas\b|\be\b|\bo\b|\ba\b|\bos\b|\bas\b', '', text)
    
    # Remove special characters and standardize whitespace
    text = re.sub(r'[^a-z0-9\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def extract_neighborhoods(location_text):
    """Extract neighborhood terms from a location string.
    
    Args:
        location_text: Standardized location text
        
    Returns:
        Set of neighborhood terms
    """
    # Handle non-string inputs
    if location_text is None:
        return set()
    
    # Convert to string if not already a string
    if not isinstance(location_text, str):
        location_text = str(location_text)
    
    # Skip empty locations
    if not location_text.strip():
        return set()
    
    # Split the location text into words
    words = location_text.split()
    
    # Filter out very short words (likely not neighborhood names)
    neighborhood_terms = set(word for word in words if len(word) > 2)
    
    return neighborhood_terms

def extract_parish(location_text):
    """Extract parish/district name from a location string.
    
    Args:
        location_text: Raw location text
        
    Returns:
        Parish name if found, otherwise None
    """
    # Handle non-string inputs
    if location_text is None:
        return None
    
    # Convert to string if not already a string
    if not isinstance(location_text, str):
        location_text = str(location_text)
    
    # Skip empty locations
    if not location_text.strip():
        return None
    
    # Common parishes/districts in Lisbon
    parishes = [
        "alcantara", "estrela", "alvalade", "areeiro", "arroios", "avenidas novas", 
        "beato", "belem", "benfica", "campo de ourique", "campolide", "carnide", 
        "lumiar", "marvila", "misericordia", "olivais", "parque das nacoes", 
        "penha de franca", "santa clara", "santa maria maior", "santo antonio", 
        "sao domingos de benfica", "sao vicente", "ajuda"
    ]
    
    location_lower = location_text.lower()
    
    # Check if any parish name is in the location
    for parish in parishes:
        if parish in location_lower:
            return parish
    
    return None

def extract_neighborhood(location_str):
    """Extract the neighborhood name from a location string."""
    # Handle non-string inputs
    if location_str is None:
        return None
    
    # Convert to string if not already a string
    if not isinstance(location_str, str):
        location_str = str(location_str)
    
    # Skip empty locations
    if not location_str.strip():
        return None
    
    # Common neighborhoods in Lisbon - expanded list of 38 neighborhoods
    neighborhoods = [
        "Alfama", "Baixa", "Chiado", "Bairro Alto", "Príncipe Real", "Mouraria", 
        "Graça", "Belém", "Alcântara", "Lapa", "Estrela", "Parque das Nações",
        "Campo de Ourique", "Avenidas Novas", "Alvalade", "Areeiro", "Benfica",
        "Santo António", "Misericórdia", "Santa Maria Maior", "São Vicente", 
        "Lumiar", "Carnide", "Campolide", "Ajuda", "Penha de França",
        # Additional neighborhoods
        "Cais do Sodré", "Avenida da Liberdade", "Marquês de Pombal", "Saldanha",
        "Anjos", "Intendente", "Arroios", "Alameda", "Roma", "Martim Moniz",
        "Rossio", "Santa Clara", "Marvila", "Olivais", "São Domingos de Benfica", "Beato"
    ]
    
    # Check for direct neighborhood matches
    for neighborhood in neighborhoods:
        if neighborhood.lower() in location_str.lower():
            return neighborhood
    
    # Try to extract from common location format patterns
    parts = location_str.split(',')
    if len(parts) >= 2:
        # Often the neighborhood is the last part
        potential_neighborhood = parts[-1].strip()
        if potential_neighborhood in neighborhoods:
            return potential_neighborhood
        
        # Or second to last part for more detailed addresses
        if len(parts) >= 3:
            potential_neighborhood = parts[-2].strip()
            if potential_neighborhood in neighborhoods:
                return potential_neighborhood
    
    return None

def find_comparable_properties(property_data, rental_data, min_size=None, max_size=None, size_range_percent=20, 
                             location_similarity_threshold=40, room_type_match=True):
    """Find comparable rental properties based on size, room type, and location.
    
    Args:
        property_data: Property to find comparables for
        rental_data: List of rental properties to compare against
        min_size: Minimum size to consider (optional)
        max_size: Maximum size to consider (optional)
        size_range_percent: Percentage range for size comparison
        location_similarity_threshold: Minimum percentage for location similarity
        room_type_match: Whether to require matching room types
        
    Returns:
        List of comparable rental properties
    """
    comparable_properties = []
    
    # Determine size range if not specified
    property_size = property_data.get("size")
    if property_size:
        if min_size is None:
            min_size = property_size * (1 - size_range_percent / 100)
        if max_size is None:
            max_size = property_size * (1 + size_range_percent / 100)
    
    # Get property room type
    property_room_type = property_data.get("room_type")
    
    # Iterate through rental data to find comparables
    for rental in rental_data:
        rental_size = rental.get("size")
        rental_room_type = rental.get("room_type")
        
        # Check size if specified
        if property_size and rental_size:
            if rental_size < min_size or rental_size > max_size:
                continue
        
        # Check room type if required
        if room_type_match and property_room_type and rental_room_type:
            if property_room_type != rental_room_type:
                continue
        
        # Check location similarity
        property_location = property_data.get("location", "")
        rental_location = rental.get("location", "")
        
        similarity = calculate_location_similarity(property_location, rental_location)
        
        if similarity < location_similarity_threshold:
            continue
        
        # Add to comparables
        comparable_properties.append(rental)
    
    return comparable_properties

def calculate_average_rent(comparables):
    """Calculate average monthly rent from comparable properties."""
    if not comparables or len(comparables) < MIN_COMPARABLE_PROPERTIES:
        return None
    
    total_rent = sum(prop['price'] for prop in comparables)
    return total_rent / len(comparables)

def generate_income_report(sales_data, rental_data, similarity_threshold=60, min_comparables=2, max_price_per_sqm=45):
    """Generate a rental income report for all sales properties.
    
    Args:
        sales_data: List of sales properties
        rental_data: List of rental properties
        similarity_threshold: Minimum location similarity percentage (0-100)
        min_comparables: Minimum number of comparable properties required for valid estimate
        max_price_per_sqm: Maximum acceptable price per square meter
        
    Returns:
        Dictionary containing rental income estimates for each property
    """
    report = {}
    total_properties = len(sales_data)
    valid_estimates = 0
    
    # Keep track of comparable counts for reporting
    comparable_counts = {}
    
    for property_item in sales_data:
        property_id = property_item.get('id', 'unknown')
        property_url = property_item.get('url', '')
        property_size = property_item.get('size', 0)
        property_price = property_item.get('price', 0)
        
        # Find comparable rental properties
        comparable_properties = find_comparable_properties(
            property_item, 
            rental_data, 
            location_similarity_threshold=similarity_threshold,
            min_size=None,
            max_size=None,
            size_range_percent=20,
            room_type_match=True
        )
        
        comparable_count = len(comparable_properties)
        
        # Update comparable count distribution
        if comparable_count in comparable_counts:
            comparable_counts[comparable_count] += 1
        else:
            comparable_counts[comparable_count] = 1
        
        report_item = {
            'url': property_url,
            'price': property_price,
            'comparable_count': comparable_count,
            'avg_price_per_sqm': 0,
            'estimated_monthly_rent': 0,
            'estimated_annual_rent': 0,
            'gross_rental_yield': 0
        }
        
        if comparable_count < min_comparables:
            # Not enough comparables
            report_item['reason'] = f"Insufficient comparables ({comparable_count})"
        else:
            # IMPROVED CALCULATION: Calculate price per square meter for each comparable
            price_per_sqm_values = []
            for comp in comparable_properties:
                comp_size = float(comp.get('size', 0))
                comp_price = float(comp.get('price', 0))
                if comp_size > 0 and comp_price > 0:
                    price_per_sqm = comp_price / comp_size
                    price_per_sqm_values.append(price_per_sqm)
            
            # Calculate average price per square meter
            if price_per_sqm_values:
                avg_price_per_sqm = sum(price_per_sqm_values) / len(price_per_sqm_values)
                
                # Apply minimum and maximum thresholds
                min_price_per_sqm = 20.0  # Minimum reasonable price per sqm
                if avg_price_per_sqm < min_price_per_sqm:
                    avg_price_per_sqm = min_price_per_sqm
                if avg_price_per_sqm > max_price_per_sqm:
                    avg_price_per_sqm = max_price_per_sqm
                
                # Calculate monthly rent based on property size AND COMPARABLE RENTALS
                if property_size > 0:
                    # IMPROVED CALCULATION: Use actual comparable rental properties
                    # Calculate average rent from comparables and adjust based on size differences
                    
                    # Get the rents from all comparable properties
                    comp_rents = [comp.get('price', 0) for comp in comparable_properties]
                    
                    # Calculate the base average rent from comparables
                    avg_comparable_rent = sum(comp_rents) / len(comp_rents) if comp_rents else 0
                    
                    # Track weighted values for better averaging
                    weighted_rents = []
                    total_weights = 0
                    
                    # Adjust each comparable based on size difference and similarity
                    for comp in comparable_properties:
                        comp_size = comp.get('size', 0)
                        comp_rent = comp.get('price', 0)
                        
                        # Skip invalid entries
                        if comp_size <= 0 or comp_rent <= 0:
                            continue
                            
                        # Calculate size difference factor (closer sizes get higher weight)
                        size_diff = abs(property_size - comp_size) / property_size if property_size > 0 else 1
                        size_factor = max(0.5, min(1.5, 1 / (1 + size_diff)))
                        
                        # Calculate location similarity weight
                        property_location = property_item.get('location', '')
                        comp_location = comp.get('location', '')
                        
                        location_similarity = calculate_location_similarity(property_location, comp_location)
                        location_factor = location_similarity / 100  # Convert to 0-1 scale
                        
                        # Calculate adjusted rent (scaled by size proportion)
                        size_proportion = property_size / comp_size if comp_size > 0 else 1
                        adjusted_rent = comp_rent * size_proportion
                        
                        # Weight by similarity and size match
                        weight = size_factor * location_factor
                        weighted_rents.append((adjusted_rent, weight))
                        total_weights += weight
                        
                    # Calculate weighted average rent
                    monthly_rent = 0
                    if weighted_rents and total_weights > 0:
                        monthly_rent = sum(rent * weight for rent, weight in weighted_rents) / total_weights
                    else:
                        # Fallback if weighted calculation fails
                        monthly_rent = avg_comparable_rent * (property_size / statistics.mean([comp.get('size', property_size) for comp in comparable_properties if comp.get('size', 0) > 0]) if comparable_properties else 1)
                    
                    # Ensure minimum rent
                    min_monthly_rent = 800  # Minimum reasonable rent in Euros for Lisbon
                    if monthly_rent < min_monthly_rent:
                        monthly_rent = min_monthly_rent
                    
                    # Apply sanity checks
                    # ADDITIONAL SANITY CHECK: Based on price-to-rent ratio for the area
                    # Most residential real estate has price-to-rent ratios between 12-20
                    # (yearly rent * price-to-rent ratio = property price)
                    min_price_to_rent_ratio = 12
                    max_price_to_rent_ratio = 25
                    
                    # Calculate acceptable rent range based on these ratios
                    min_monthly_rent_by_ratio = property_price / (max_price_to_rent_ratio * 12)
                    max_monthly_rent_by_ratio = property_price / (min_price_to_rent_ratio * 12)
                    
                    # Ensure rent stays within reasonable bounds of the property price
                    if monthly_rent < min_monthly_rent_by_ratio:
                        monthly_rent = min_monthly_rent_by_ratio
                        logging.debug(f"Rent for {property_url} increased to meet minimum price-to-rent ratio")
                    elif monthly_rent > max_monthly_rent_by_ratio:
                        monthly_rent = max_monthly_rent_by_ratio
                        logging.debug(f"Rent for {property_url} capped to maintain reasonable price-to-rent ratio")
                    
                    # ADDITIONAL SANITY CHECK: Absolute maximum monthly rent
                    absolute_max_monthly_rent = 5000  # No property should rent for more than €5000/month in most cases
                    if monthly_rent > absolute_max_monthly_rent:
                        logging.warning(f"Monthly rent for {property_url} calculated as €{monthly_rent:.2f} - capping at €{absolute_max_monthly_rent}")
                        monthly_rent = absolute_max_monthly_rent
                    
                    # Calculate annual rent
                    annual_rent = monthly_rent * 12
                    
                    # ADDITIONAL SANITY CHECK: Ensure the annual rent is at most 30% of the property price
                    max_annual_rent_pct = 0.3  # 30% of property price per year
                    max_annual_rent = property_price * max_annual_rent_pct
                    
                    if annual_rent > max_annual_rent:
                        annual_rent = max_annual_rent
                        monthly_rent = annual_rent / 12
                        logging.warning(f"Annual rent for {property_url} was too high - capping at {max_annual_rent_pct*100}% of property price")
                    
                    # Calculate rental yield
                    rental_yield = 0
                    if property_price > 0:
                        rental_yield = (annual_rent / property_price) * 100
                    
                    # Store results
                    report_item['avg_price_per_sqm'] = avg_price_per_sqm
                    report_item['estimated_monthly_rent'] = monthly_rent
                    report_item['estimated_annual_rent'] = annual_rent
                    report_item['gross_rental_yield'] = rental_yield
                    report_item['reason'] = "Valid estimate"
                    valid_estimates += 1
                else:
                    report_item['reason'] = "Invalid property size"
            else:
                report_item['reason'] = "No valid rental prices found"
        
        # Add to report
        report[property_url] = report_item
    
    # Log summary
    logging.info(f"Properties with valid estimates: {valid_estimates} out of {total_properties} ({valid_estimates/total_properties*100:.1f}%)")
    logging.info("Distribution of comparable counts:")
    for count, num_properties in sorted(comparable_counts.items()):
        logging.info(f"  {count} comparables: {num_properties} properties")
    
    return report

def run_improved_analysis(similarity_threshold=40, min_comparable_properties=MIN_COMPARABLE_PROPERTIES):
    """Run improved rental income analysis with a complete rental dataset.
    
    Args:
        similarity_threshold: Minimum location similarity percentage (0-100) to use for comparable properties
        min_comparable_properties: Minimum number of comparable properties required for a valid estimate
    """
    logging.info(f"Starting improved rental income analysis with complete rental dataset")
    logging.info(f"Using location similarity threshold: {similarity_threshold}%")
    logging.info(f"Requiring minimum of {min_comparable_properties} comparable properties for valid estimates")
    logging.info(f"Maximum acceptable price per sqm: €45")
    
    # Import needed modules at the function level to avoid UnboundLocalError
    import os
    from pathlib import Path
    
    # Try to load standardized sales data first, fall back to legacy file if needed
    standardized_sales_files = [
        "propbot/data/processed/sales.csv",
        "data/processed/sales.csv",
        "../../../data/processed/sales.csv"
    ]
    
    sales_file = None
    for file_path in standardized_sales_files:
        if os.path.exists(file_path):
            sales_file = file_path
            logging.info(f"Loading sales data from standardized CSV: {sales_file}")
            break
    
    # Fall back to legacy file if standardized file not found
    if not sales_file:
        sales_file = 'idealista_listings.json'
        logging.info(f"Loading sales data from legacy file: {sales_file}")
    
    # Load property data (sale listings)
    property_data = load_property_data(sales_file)
    logging.info(f"Loaded {len(property_data)} properties for sale")
    
    # Load rental data - first from database, falling back to file if needed
    from_db = True
    db_rentals = get_rental_listings_from_database()
    if db_rentals:
        rental_data = db_rentals
        logging.info(f"Loaded {len(rental_data)} rental properties from database")
    else:
        from_db = False
        rental_data = load_complete_rental_data()
        logging.info(f"Loaded {len(rental_data)} rental properties from files (database access failed)")
    
    if not rental_data:
        logging.error("No rental data found in database or files. Cannot proceed with analysis.")
        return {}
    
    # Debug a sample property to verify the process works as expected
    try:
        sample_property = property_data[0]
        logging.info(f"Debugging sample property: {sample_property.get('url', 'unknown')}")
        
        sample_comparables = find_comparable_properties(
            sample_property, 
            rental_data, 
            location_similarity_threshold=similarity_threshold,
            min_size=None,
            max_size=None,
            size_range_percent=20,
            room_type_match=True
        )
        
        logging.info(f"Sample property has {len(sample_comparables)} comparables (from {'database' if from_db else 'files'})")
        
        if len(sample_comparables) < min_comparable_properties:
            logging.info(f"WARNING: Sample property has fewer than {min_comparable_properties} comparables; no valid estimate will be generated")
            
        # Generate the income report
        logging.info(f"Generating rental income report (similarity threshold: {similarity_threshold}%)...")
        logging.info(f"Requiring minimum of {min_comparable_properties} comparable properties for valid estimates")
        
        income_report = generate_income_report(
            property_data, 
            rental_data, 
            similarity_threshold=similarity_threshold,
            min_comparables=min_comparable_properties,
            max_price_per_sqm=45.0
        )
        
        # Save the report to files - use absolute paths with proper environment detection
        processed_dir = None
        
        # Check for Heroku environment first (should use /app/propbot)
        if os.path.exists('/app/propbot/data'):
            processed_dir = '/app/propbot/data/processed'
            logging.info(f"Using Heroku environment path: {processed_dir}")
        else:
            # Fall back to calculating relative path from current file
            current_file_path = os.path.abspath(__file__)
            project_root = os.path.abspath(os.path.join(os.path.dirname(current_file_path), "../../../"))
            
            # Use config module path if available
            try:
                from ...config import DATA_DIR, PROCESSED_DATA_DIR
                if os.path.exists(PROCESSED_DATA_DIR):
                    processed_dir = PROCESSED_DATA_DIR
                    logging.info(f"Using config module path: {processed_dir}")
                else:
                    processed_dir = os.path.join(project_root, "data", "processed")
                    logging.info(f"Using calculated path: {processed_dir}")
            except (ImportError, AttributeError):
                processed_dir = os.path.join(project_root, "data", "processed")
                logging.info(f"Using calculated path: {processed_dir}")
        
        # Ensure the directory exists
        os.makedirs(processed_dir, exist_ok=True)
        
        json_filename = os.path.join(processed_dir, "rental_income_report_improved.json")
        csv_filename = os.path.join(processed_dir, "rental_income_report_improved.csv")
        
        save_report_to_json(income_report, json_filename)
        save_report_to_csv(income_report, csv_filename)
        
        logging.info(f"Report saved to {json_filename} and {csv_filename}")
        logging.info("Improved rental income analysis completed")
        return True
    except Exception as e:
        logging.error(f"Error running improved rental analysis: {str(e)}")
        return False

def load_property_data(filename=None):
    """
    Load property data from a specified file or one of the possible locations.
    
    Args:
        filename (str, optional): The path to the file containing property data.
            If None, the function will try to load from one of the possible file locations.
    
    Returns:
        list: A list of property dictionaries with validated data.
    """
    logging.info("Loading property data...")
    
    if filename and os.path.exists(filename):
        logging.info(f"Loading property data from specified file: {filename}")
    else:
        # First try to load from real data CSV format
        possible_files = [
            # First try the real data files with current data
            "propbot/data/processed/sales_current.csv",
            "data/processed/sales_current.csv",
            "../../../data/processed/sales_current.csv",
            # Then try the expected file from data_processor.py
            "propbot/data/processed/sales.csv",
            "data/processed/sales.csv",
            "../../../data/processed/sales.csv",
            # Then fall back to standardized CSV formats
            "propbot/data/processed/sales_data.csv",
            "data/processed/sales_data.csv",
            "../../../data/processed/sales_data.csv",
            # Then try legacy JSON formats
            "propbot/data/processed/property_data.json",
            "data/processed/property_data.json",
            "../../../data/processed/property_data.json",
            "property_data.json"
        ]
        
        filename = None
        for file_path in possible_files:
            if os.path.exists(file_path):
                filename = file_path
                logging.info(f"Using property data file: {filename}")
                break
        
        if not filename:
            logging.error(f"Error: Property data file not found")
            logging.error(f"Looked for: {', '.join(possible_files)}")
            return []
    
    properties = []
    outliers = []
    
    try:
        # Use pandas if available for easier CSV handling
        try:
            df = pd.read_csv(filename)
            logging.info(f"Loaded {len(df)} rows from {filename}")
            
            for _, row in df.iterrows():
                # Get room_type directly from CSV if available, otherwise use empty string
                room_type = row.get('room_type', '')
                
                # Convert size to numeric, handling potential strings and None values
                size_str = row.get('size')
                size_value = 0
                if size_str is not None:
                    try:
                        # If it's a string, try to extract number
                        if isinstance(size_str, str):
                            size_match = re.search(r'(\d+(?:\.\d+)?)', size_str)
                            if size_match:
                                size_value = float(size_match.group(1))
                        else:
                            # Try direct conversion
                            size_value = float(size_str)
                    except (ValueError, TypeError):
                        size_value = 0
                
                # Convert price to numeric, handling potential strings
                price_str = row.get('price')
                price_value = 0
                if price_str is not None:
                    try:
                        price_value = float(price_str)
                    except (ValueError, TypeError):
                        price_value = 0
                
                location = row.get('location', '')
                url = row.get('url', '')
                
                # Skip invalid entries - must have both size and price
                if not (size_value > 0 and price_value > 0):
                    continue
                
                # Create property item
                property_item = {
                    'url': url,
                    'price': price_value,
                    'location': location,
                    'size': size_value,
                    'room_type': room_type
                }
                
                # Don't filter out sales properties by price per sqm
                # We expect sales prices to be much higher than rental prices
                properties.append(property_item)
                
        except ImportError:
            # Fall back to CSV reader if pandas is not available
            logging.info("Pandas not available. Using CSV reader.")
            with open(filename, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    # Get room_type directly from CSV if available
                    room_type = row.get('room_type', '')
                    
                    # Convert size to numeric, handling potential strings
                    size_str = row.get('size')
                    size_value = 0
                    if size_str is not None:
                        try:
                            # If it's a string, try to extract number
                            if isinstance(size_str, str):
                                size_match = re.search(r'(\d+(?:\.\d+)?)', size_str)
                                if size_match:
                                    size_value = float(size_match.group(1))
                            else:
                                # Try direct conversion
                                size_value = float(size_str)
                        except (ValueError, TypeError):
                            size_value = 0
                    
                    # Convert price to numeric, handling potential strings
                    price_str = row.get('price')
                    price_value = 0
                    if price_str is not None:
                        try:
                            price_value = float(price_str)
                        except (ValueError, TypeError):
                            price_value = 0
                    
                    location = row.get('location', '')
                    url = row.get('url', '')
                    
                    # Skip invalid entries - must have both size and price
                    if not (size_value > 0 and price_value > 0):
                        continue
                    
                    # Create property item
                    property_item = {
                        'url': url,
                        'price': price_value,
                        'location': location,
                        'size': size_value,
                        'room_type': room_type
                    }
                    
                    # Don't filter out sales properties by price per sqm
                    # We expect sales prices to be much higher than rental prices
                    properties.append(property_item)
        
        logging.info(f"Loaded {len(properties)} valid sales properties")
        return properties
        
    except Exception as e:
        logging.error(f"Error loading property data: {str(e)}")
        logging.error(traceback.format_exc())
        return []

def extract_size(details_str):
    """
    Extract size in square meters from details text.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_size instead.
    
    Args:
        details_str: Text that may contain size information
        
    Returns:
        Extracted size as float or 0 if not found
    """
    if pd.isna(details_str) or not isinstance(details_str, str):
        return 0
    
    # Use the robust implementation from extraction_utils
    size, _ = extract_size_robust(details_str)
    return size if size is not None else 0

def calculate_location_similarity(location1, location2):
    """Calculate the similarity percentage between two locations.
    
    Args:
        location1: First location string
        location2: Second location string
        
    Returns:
        Similarity score (0-100)
    """
    # Handle cases where inputs are not strings (e.g., None, float, int)
    if not location1 or not location2:
        return 0
    
    # Convert to string first in case location is a number
    loc1 = str(location1) if location1 is not None else ""
    loc2 = str(location2) if location2 is not None else ""
    
    # Skip processing if either string is empty after conversion
    if not loc1 or not loc2:
        return 0
    
    # Convert to lowercase for consistent comparison
    loc1 = loc1.lower()
    loc2 = loc2.lower()
    
    # Extract neighborhoods
    neighborhoods1 = [n.strip() for n in loc1.split(',')]
    neighborhoods2 = [n.strip() for n in loc2.split(',')]
    
    # Check for exact neighborhood match first (highest similarity)
    for n1 in neighborhoods1:
        if n1 in neighborhoods2:
            return 100
    
    # If no exact match, use fuzzy token matching
    return fuzz.token_sort_ratio(loc1, loc2)

def save_report_to_csv(report, filename):
    """Save the rental income report to a CSV file.
    
    Args:
        report: Dictionary of rental income report items
        filename: Name of the CSV file to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = [
                'url', 'price', 'comparable_count', 'avg_price_per_sqm',
                'estimated_monthly_rent', 'estimated_annual_rent', 'gross_rental_yield', 'reason'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for url, entry in report.items():
                # Create a copy of the entry with all required fields
                row = {
                    'url': url,
                    'price': entry.get('price', 0),
                    'comparable_count': entry.get('comparable_count', 0),
                    'avg_price_per_sqm': entry.get('avg_price_per_sqm', 0),
                    'estimated_monthly_rent': entry.get('estimated_monthly_rent', 0),
                    'estimated_annual_rent': entry.get('estimated_annual_rent', 0),
                    'gross_rental_yield': entry.get('gross_rental_yield', 0),
                    'reason': entry.get('reason', 'Valid estimate' if entry.get('estimated_monthly_rent', 0) > 0 else 'Unknown')
                }
                writer.writerow(row)
        
        logging.info(f"CSV report saved to {filename}")
        return True
    except Exception as e:
        logging.error(f"Error saving report to CSV: {str(e)}")
        logging.error(traceback.format_exc())
        return False

def save_report_to_json(report, filename):
    """Save the rental income report to a JSON file.
    
    Args:
        report: Dictionary of rental income report items
        filename: Name of the JSON file to save
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w') as file:
            json.dump(report, file, indent=4)
        
        logging.info(f"JSON report saved to {filename}")
        return True
    except Exception as e:
        logging.info(f"Error saving report: {str(e)}")
        return False

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run with a similarity threshold of 40% to find more matches
    run_improved_analysis(similarity_threshold=40) 