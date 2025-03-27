import json
import csv
import re
import os
import logging
from datetime import datetime
from fuzzywuzzy import fuzz, process
import pandas as pd
from pathlib import Path
from ...utils.extraction_utils import extract_size as extract_size_robust, extract_room_type
from ...config import CONFIG

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def log_message(message):
    """Log a message using the logger."""
    logging.info(message)

def load_sales_data(filename=None):
    """
    Load the JSON file containing properties for sale.
    Validate that each record includes purchase price, size (sqm), location, and room type.
    """
    if filename is None:
        # Try different paths relative to the module
        possible_paths = [
            "propbot/data/raw/sales/idealista_listings.json",
            "data/raw/sales/idealista_listings.json",
            "../../../data/raw/sales/idealista_listings.json",
            "idealista_listings.json"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                filename = path
                break
        
        if filename is None:
            log_message(f"Error: Sales data file not found")
            return []
    
    log_message(f"Loading sales data from {filename}")
    sales_properties = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        for item in data:
            # Validate required fields
            if all(k in item for k in ['url', 'title', 'price', 'location', 'details']):
                # Extract size and room type from details
                size = extract_size(item.get('details', ''))
                room_type = extract_room_type(item.get('details', ''))
                
                # Debug log for the first few properties to verify parsing
                if len(sales_properties) < 5:
                    log_message(f"Parsed property details: {item.get('details', '')} -> Size: {size}, Room Type: {room_type}")
                
                if size and room_type:
                    sales_properties.append({
                        'url': item['url'],
                        'title': item['title'],
                        'price': item['price'],
                        'location': item['location'],
                        'size': size,
                        'room_type': room_type,
                        'details': item.get('details', '')
                    })
        
        log_message(f"Loaded {len(sales_properties)} valid properties for sale")
        return sales_properties
    
    except Exception as e:
        log_message(f"Error loading sales data: {str(e)}")
        return []

def extract_size(details_str):
    """
    Extract size in square meters from details text.
    
    Note: This function is deprecated and remains only for backward compatibility.
    Use propbot.utils.extraction_utils.extract_size instead.
    
    Args:
        details_str: Text that may contain size information
        
    Returns:
        Extracted size as float or None if not found
    """
    if not details_str:
        return None
    
    # Use the robust implementation from extraction_utils
    size, _ = extract_size_robust(details_str)
    return size

def extract_room_type(details):
    """Extract room type (T0, T1, T2, etc.) from details string."""
    if not details:
        return None
    
    # Look for room type patterns in the special format "T[room_number][size] m²"
    special_format_match = re.search(r'(T[0-4])\d+\s*m²', details)
    if special_format_match:
        return special_format_match.group(1)
    
    # Regular format - just look for T0, T1, T2, etc.
    room_match = re.search(r'T[0-4]', details)
    if room_match:
        return room_match.group(0)
    
    return None

def load_rental_data(filename=None):
    """
    Load rental property data from CSV file.
    """
    if filename is None:
        # Try different paths
        possible_paths = [
            "propbot/data/raw/rentals/rental_data.csv",
            "data/raw/rentals/rental_data.csv",
            "../../../data/raw/rentals/rental_data.csv",
            "rental_data.csv"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                filename = path
                break
        
        if filename is None:
            log_message(f"Error: Rental data file not found")
            return []
    
    log_message(f"Loading rental data from {filename}")
    rental_properties = []
    
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Skip empty rows
                if not row.get('url') or not row.get('rent_price'):
                    continue
                
                # Extract size from format like "250 m²"
                size_str = row.get('size', '')
                size = None
                
                # First check for the special format "T[room_number][size] m²" that might be in the size field
                special_format_match = re.search(r'T[0-4](\d+)\s*m²', size_str)
                if special_format_match:
                    try:
                        size = float(special_format_match.group(1))
                    except (ValueError, TypeError):
                        size = None
                else:
                    # Regular case: "46 m²"
                    size_match = re.search(r'(\d+)\s*m²', size_str)
                    if size_match:
                        try:
                            size = float(size_match.group(1))
                        except (ValueError, TypeError):
                            size = None
                
                # Extract price from format like "2,600€/month"
                price_str = row.get('rent_price', '')
                price = None
                if price_str:
                    # Remove commas and convert to float
                    price_match = re.search(r'([\d,.]+)', price_str)
                    if price_match:
                        price_clean = price_match.group(1).replace(',', '')
                        try:
                            price = float(price_clean)
                        except (ValueError, TypeError):
                            price = None
                
                # Get room type
                room_type = row.get('num_rooms', '')
                
                # Get location
                location = row.get('location', '')
                
                if size and price and room_type and location:
                    # Add debug log for the first few rental properties
                    if len(rental_properties) < 5:
                        log_message(f"Parsed rental property: Size {size_str} -> {size}, Room Type: {room_type}, Price: {price}")
                    
                    rental_properties.append({
                        'url': row.get('url', ''),
                        'title': f"{room_type} - {size} m²",
                        'price': price,
                        'location': location,
                        'size': size,
                        'room_type': room_type
                    })
        
        log_message(f"Loaded {len(rental_properties)} valid rental properties")
        return rental_properties
    
    except Exception as e:
        log_message(f"Error loading rental data: {str(e)}")
        return []

def create_location_mapping():
    """Create a mapping dictionary for standardizing locations."""
    # Common location variations in Lisbon neighborhoods
    mapping = {
        # Format: 'variation': 'standard name'
        
        # Municipality areas
        'centro': 'centro',
        'centro histórico': 'centro',
        'histórico': 'centro',
        
        # Specific neighborhoods
        'anjos': 'arroios',
        'arroios': 'arroios',
        'anjos arroios': 'arroios',
        'pena arroios': 'arroios',
        'pena': 'arroios',
        
        'alfama': 'alfama',
        'alfama santa maria maior': 'alfama',
        'santa maria maior': 'alfama',
        'rossio': 'alfama',
        'rossio martim moniz santa maria maior': 'alfama',
        'martim moniz': 'alfama',
        
        'ajuda': 'ajuda',
        'centro ajuda': 'ajuda',
        'boa hora ajuda': 'ajuda',
        'alto da ajuda ajuda': 'ajuda',
        'alto da ajuda': 'ajuda',
        'calçada da ajuda': 'ajuda',
        'belém ajuda': 'ajuda',
        'rio seco casalinho ajuda': 'ajuda',
        
        'alcântara': 'alcântara',
        'largo do calvário': 'alcântara',
        'largo do calvário lx factory alcântara': 'alcântara',
        'lx factory': 'alcântara',
        'lx factory alcântara': 'alcântara',
        'alto de alcântara': 'alcântara', 
        'alto de alcântara alcântara': 'alcântara',
        'alvito': 'alcântara',
        'alvito quinta do jacinto alcântara': 'alcântara',
        'quinta do jacinto': 'alcântara',
        
        'alvalade': 'alvalade',
        'são joão de brito': 'alvalade',
        'são joão de brito alvalade': 'alvalade',
        
        'areeiro': 'areeiro',
        'bairro dos actores': 'areeiro',
        'bairro dos actores areeiro': 'areeiro',
        'barão sabrosa': 'areeiro',
        'barão sabrosa penha de frança': 'areeiro',
        
        'avenidas novas': 'avenidas novas',
        'entrecampos': 'avenidas novas',
        'entrecampos avenidas novas': 'avenidas novas',
        
        'baixa': 'baixa-chiado',
        'baixa chiado': 'baixa-chiado',
        'chiado': 'baixa-chiado',
        
        'beato': 'beato',
        
        'belém': 'belém',
        'centro belém': 'belém',
        
        'benfica': 'benfica',
        'portas de benfica': 'benfica',
        'mercado de benfica': 'benfica',
        'portas de benfica mercado de benfica benfica': 'benfica',
        'estrada de benfica': 'benfica',
        'arneiros': 'benfica',
        'arneiros benfica': 'benfica',
        'bairro de santa cruz': 'benfica',
        'bairro de santa cruz benfica': 'benfica',
        'são domingos de benfica': 'benfica',
        
        'bica': 'bica-misericórdia',
        'bica misericórdia': 'bica-misericórdia',
        'misericórdia': 'bica-misericórdia',
        'santa catarina': 'bica-misericórdia',
        'santa catarina misericórdia': 'bica-misericórdia',
        
        'campo de ourique': 'campo de ourique',
        'centro campo de ourique': 'campo de ourique',
        'santa isabel': 'campo de ourique',
        'santa isabel campo de ourique': 'campo de ourique',
        'prazeres': 'campo de ourique',
        'prazeres estrela': 'campo de ourique',
        'prazeres maria pia': 'campo de ourique',
        'prazeres maria pia campo de ourique': 'campo de ourique',
        'maria pia': 'campo de ourique',
        
        'campolide': 'campolide',
        'centro campolide': 'campolide',
        'bairro da serafina': 'campolide',
        'bairro da serafina campolide': 'campolide',
        'praça de espanha': 'campolide',
        'praça de espanha sete rios campolide': 'campolide',
        'sete rios': 'campolide',
        'bairro calçada dos mestres': 'campolide',
        
        'carnide': 'carnide',
        'bairro novo': 'carnide',
        'bairro novo carnide': 'carnide',
        
        'estrela': 'estrela',
        'lapa': 'estrela',
        'lapa estrela': 'estrela',
        'santos o velho': 'estrela',
        'madragoa': 'estrela',
        'santos o velho madragoa estrela': 'estrela',
        
        'graça': 'graça',
        'são vicente': 'graça',
        'graça são vicente': 'graça',
        
        'marvila': 'marvila',
        
        'olivais': 'olivais',
        
        'parque das nações': 'parque das nações',
        
        'penha de frança': 'penha de frança',
        'centro penha de frança': 'penha de frança',
        
        'santa clara': 'santa clara'
    }
    
    return mapping

def standardize_location(location, mapping):
    """Standardize a location string for better matching."""
    if not location:
        return None
    
    # Convert to lowercase and remove extra spaces
    location = location.lower().strip()
    
    # Remove common prefixes and numbers
    location = re.sub(r'rua|avenida|travessa|largo|praça|estrada|beco|calçada|bairro', '', location)
    location = re.sub(r'\d+', '', location)
    
    # Remove common symbols and extra whitespace
    location = re.sub(r'[,\.;:\-–—_/\\]', ' ', location)
    location = re.sub(r'\s+', ' ', location).strip()
    
    # Direct mapping lookup
    if location in mapping:
        return mapping[location]
    
    # Try fuzzy matching for locations not found in mapping
    if location:
        best_match = process.extractOne(location, mapping.keys(), scorer=fuzz.token_sort_ratio)
        if best_match and best_match[1] > 60:  # Lower threshold from 75% to 60% for more matches
            return mapping[best_match[0]]
    
    # If no match found, return the original standardized location
    return location

def find_comparable_properties(property_data, rental_properties, size_tolerance_pct=0.3, size_adjustment_factor=0.2):
    """Find comparable rental properties to a property for sale.
    
    Args:
        property_data: Property for sale to find comparables for
        rental_properties: List of rental properties to compare against
        size_tolerance_pct: Percentage tolerance for size matching (default: 30%)
        size_adjustment_factor: Factor to apply to rental sizes to make them comparable to sales sizes (default: 0.2)
                              This means rental sizes will be multiplied by 0.2 (rental_size * 0.2) to make them
                              comparable to sales sizes, which handles the systematic size discrepancy.
    """
    if not property_data or not rental_properties:
        return []
    
    # Create mapping dictionary
    mapping = create_location_mapping()
    
    # Standardize the property location
    std_property_location = standardize_location(property_data['location'], mapping)
    if not std_property_location:
        log_message(f"No location found for: {property_data['url']}")
        return []
    
    # Find rentals with matching location and room type
    matches = []
    for rental in rental_properties:
        # Standardize rental location
        std_rental_location = standardize_location(rental['location'], mapping)
        if not std_rental_location:
            continue
        
        # Check if locations match using fuzzy matching
        location_match = False
        if std_property_location == std_rental_location:
            location_match = True
        else:
            # Use fuzzy matching for locations
            similarity = fuzz.token_sort_ratio(std_property_location, std_rental_location)
            if similarity >= 75:  # Keep original 75% similarity threshold
                location_match = True
        
        # Skip if locations don't match
        if not location_match:
            continue
        
        # Check room type match (exact match only)
        if property_data['room_type'] != rental['room_type']:
            continue
        
        # Adjust rental size for more accurate comparison
        adjusted_rental_size = rental['size'] * size_adjustment_factor
        
        # Check size tolerance (30%)
        min_size = property_data['size'] - (property_data['size'] * size_tolerance_pct)
        max_size = property_data['size'] + (property_data['size'] * size_tolerance_pct)
        
        if min_size <= adjusted_rental_size <= max_size:
            matches.append(rental)
    
    if not matches:
        log_message(f"Found 0 comparable properties for {property_data['url']}")
        if not std_property_location:
            log_message(f"No location match found for: {property_data['location']}")
    else:
        log_message(f"Found {len(matches)} comparable properties for {property_data['url']}")
    
    return matches

def calculate_average_rent(comparables):
    """Calculate average monthly rent from comparable properties."""
    if not comparables:
        return None
    
    total_rent = sum(prop['price'] for prop in comparables)
    return total_rent / len(comparables)

def estimate_rental_income(property_data, rental_properties):
    """Estimate monthly rental income for a property."""
    # Find comparable rental properties
    comparables = find_comparable_properties(property_data, rental_properties)
    
    # Calculate average rent
    avg_rent = calculate_average_rent(comparables)
    
    if avg_rent:
        return {
            'property_url': property_data['url'],
            'property_title': property_data['title'],
            'property_price': property_data['price'],
            'property_location': property_data['location'],
            'property_size': property_data['size'],
            'property_room_type': property_data['room_type'],
            'estimated_monthly_rent': avg_rent,
            'comparable_count': len(comparables),
            'comparable_properties': [prop['url'] for prop in comparables]
        }
    
    return None

def generate_income_report(properties_for_sale, rental_properties):
    """Generate a report of estimated rental income for properties."""
    log_message("Generating rental income report...")
    income_estimates = []
    
    for prop in properties_for_sale:
        estimate = estimate_rental_income(prop, rental_properties)
        if estimate:
            income_estimates.append(estimate)
    
    log_message(f"Generated income estimates for {len(income_estimates)} properties")
    return income_estimates

def save_report_to_json(income_estimates, filename="rental_income_report.json"):
    """Save income estimates to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(income_estimates, file, indent=2)
        log_message(f"Report saved to {filename}")
    except Exception as e:
        log_message(f"Error saving report: {str(e)}")

def save_report_to_csv(income_estimates, filename="rental_income_report.csv"):
    """Save income estimates to a CSV file."""
    if not income_estimates:
        log_message("No data to save to CSV")
        return
    
    try:
        with open(filename, 'w', encoding='utf-8', newline='') as file:
            fieldnames = [
                'property_url', 'property_title', 'property_location', 
                'property_size', 'property_room_type', 'property_price',
                'estimated_monthly_rent', 'comparable_count'
            ]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for estimate in income_estimates:
                # Create a copy of the estimate without the comparable_properties list
                row = {k: v for k, v in estimate.items() if k != 'comparable_properties'}
                writer.writerow(row)
        
        log_message(f"CSV report saved to {filename}")
    except Exception as e:
        log_message(f"Error saving CSV report: {str(e)}")

def run_analysis():
    """
    Run the complete rental income analysis workflow.
    """
    log_message("Starting rental income analysis")
    
    # Load the data
    properties_for_sale = load_sales_data()
    
    # Determine the current month for loading the rental data
    current_month = datetime.now().strftime("%Y-%m")
    rental_filename = f"rental_data_{current_month}.csv"
    rental_properties = load_rental_data(rental_filename)
    
    if not properties_for_sale or not rental_properties:
        log_message("Error: Could not load required data")
        return False
    
    # Generate the report
    income_estimates = generate_income_report(properties_for_sale, rental_properties)
    
    # Save the results
    json_saved = save_report_to_json(income_estimates)
    csv_saved = save_report_to_csv(income_estimates)
    
    log_message("Rental income analysis completed")
    return json_saved and csv_saved

if __name__ == "__main__":
    run_analysis() 