#!/usr/bin/env python3
"""
PropBot Property Segmentation and Classification Module

This module handles neighborhood analysis, classification of properties,
and comparison with neighborhood averages.
"""

import logging
import re
import json
import csv
import pandas as pd
import numpy as np
from collections import defaultdict
from difflib import SequenceMatcher
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Classification criteria
CLASSIFICATION_CRITERIA = {
    "excellent_deal": {
        "price_diff_percentage": -0.15,  # At least 15% below neighborhood average
        "gross_yield": 0.06,  # At least 6% gross yield
        "cap_rate": 0.045,  # At least 4.5% cap rate
        "cash_on_cash": 0.05  # At least 5% cash on cash return
    },
    "good_deal": {
        "price_diff_percentage": -0.05,  # At least 5% below neighborhood average
        "gross_yield": 0.05,  # At least 5% gross yield
        "cap_rate": 0.035,  # At least 3.5% cap rate
        "cash_on_cash": 0.03  # At least 3% cash on cash return
    },
    "fair_deal": {
        "price_diff_percentage": 0.05,  # Within 5% of neighborhood average
        "gross_yield": 0.04,  # At least 4% gross yield
        "cap_rate": 0.03,  # At least 3% cap rate
        "cash_on_cash": 0.02  # At least 2% cash on cash return
    },
    "poor_deal": {
        "price_diff_percentage": 0.15,  # Up to 15% above neighborhood average
        "gross_yield": 0.03,  # At least 3% gross yield
        "cap_rate": 0.02,  # At least 2% cap rate
        "cash_on_cash": 0.01  # At least 1% cash on cash return
    }
}

# Standardized neighborhood names mapping
NEIGHBORHOOD_MAPPING = {
    # Common neighborhood misspellings or alternative names
    "arroios": "Arroios",
    "avenidas novas": "Avenidas Novas",
    "av novas": "Avenidas Novas",
    "avenida novas": "Avenidas Novas",
    "avenida nova": "Avenidas Novas",
    "alfama": "Alfama",
    "baixa": "Baixa",
    "bairro alto": "Bairro Alto",
    "benfica": "Benfica",
    "campo de ourique": "Campo de Ourique",
    "campo ourique": "Campo de Ourique",
    "campolide": "Campolide",
    "chiado": "Chiado",
    "estrela": "Estrela",
    "graca": "Graça",
    "graça": "Graça",
    "intendente": "Intendente",
    "lapa": "Lapa",
    "mouraria": "Mouraria",
    "parque das nacoes": "Parque das Nações",
    "parque das nações": "Parque das Nações",
    "parque nacoes": "Parque das Nações",
    "principe real": "Príncipe Real",
    "príncipe real": "Príncipe Real",
    "restelo": "Restelo",
    "santos": "Santos",
    "sao bento": "São Bento",
    "são bento": "São Bento",
    "s. bento": "São Bento",
    "almirante reis": "Almirante Reis",
    "anjos": "Anjos",
    "ajuda": "Ajuda",
    "alcantara": "Alcântara",
    "alcântara": "Alcântara",
    "alvalade": "Alvalade",
    "areeiro": "Areeiro",
    "amoreiras": "Amoreiras",
    "belem": "Belém",
    "belém": "Belém",
    "beato": "Beato",
    "cais do sodre": "Cais do Sodré",
    "cais do sodré": "Cais do Sodré",
    "campo grande": "Campo Grande",
    "campo pequeno": "Campo Pequeno",
    "carnide": "Carnide",
    "entrecampos": "Entrecampos",
    "lumiar": "Lumiar",
    "marvila": "Marvila",
    "martim moniz": "Martim Moniz",
    "olaias": "Olaias",
    "olivais": "Olivais",
    "oriente": "Oriente",
    "penha de franca": "Penha de França",
    "penha de frança": "Penha de França",
    "prazeres": "Prazeres",
    "rato": "Rato",
    "rossio": "Rossio",
    "saldanha": "Saldanha",
    "santa apolonia": "Santa Apolónia",
    "santa apolónia": "Santa Apolónia",
    "telheiras": "Telheiras"
}

def standardize_location(location):
    """
    Standardize location name by matching with known neighborhoods.
    
    Args:
        location: The location string to standardize
        
    Returns:
        Standardized location name
    """
    if not location or location.strip() == "":
        return "Unknown"
    
    # Convert to lowercase for matching
    location_lower = location.lower().strip()
    
    # Direct match in mapping
    if location_lower in NEIGHBORHOOD_MAPPING:
        return NEIGHBORHOOD_MAPPING[location_lower]
    
    # Check for partial matches in location string
    for key, value in NEIGHBORHOOD_MAPPING.items():
        if key in location_lower:
            return value
    
    # Try to find best match based on similarity
    best_match = None
    best_score = 0
    
    for key, value in NEIGHBORHOOD_MAPPING.items():
        score = SequenceMatcher(None, location_lower, key).ratio()
        if score > 0.8 and score > best_score:  # 80% similarity threshold
            best_score = score
            best_match = value
    
    if best_match:
        return best_match
    
    # If no match found, return capitalized original
    return location.strip().title()

def extract_neighborhood(address):
    """
    Extract neighborhood from address string.
    
    Args:
        address: Full address string
        
    Returns:
        Extracted and standardized neighborhood name
    """
    if not address:
        return "Unknown"
    
    # Try to extract neighborhood after the last comma
    parts = address.split(',')
    if len(parts) > 1:
        potential_neighborhood = parts[-1].strip()
        if potential_neighborhood:
            return standardize_location(potential_neighborhood)
    
    # Try to extract neighborhood from address parts
    address_lower = address.lower()
    
    for key in NEIGHBORHOOD_MAPPING:
        if key in address_lower:
            return NEIGHBORHOOD_MAPPING[key]
    
    # If all else fails, return Unknown
    return "Unknown"

def extract_parish(address):
    """
    Extract parish name from address string.
    A parish (freguesia) is an administrative division in Portugal.
    
    Args:
        address: Full address string
        
    Returns:
        Extracted parish name or None if not found
    """
    # Common parish (freguesia) names in Lisbon
    parish_list = [
        "Ajuda", "Alcântara", "Alvalade", "Areeiro", "Arroios",
        "Avenidas Novas", "Beato", "Belém", "Benfica", "Campo de Ourique",
        "Campolide", "Carnide", "Estrela", "Lumiar", "Marvila",
        "Misericórdia", "Olivais", "Parque das Nações", "Penha de França",
        "Santa Clara", "Santa Maria Maior", "Santo António", "São Domingos de Benfica",
        "São Vicente"
    ]
    
    if not address:
        return None
    
    # Convert to title case for matching
    address_parts = [part.strip().title() for part in address.split(',')]
    
    # Check for exact matches in address parts
    for part in address_parts:
        if part in parish_list:
            return part
    
    # Check for partial matches
    for parish in parish_list:
        # Create regex pattern to find the parish name
        # This handles cases where the parish is mentioned without being a separate part
        pattern = r'\b' + re.escape(parish) + r'\b'
        if re.search(pattern, address, re.IGNORECASE):
            return parish
    
    return None

def calculate_location_similarity(loc1, loc2):
    """
    Calculate similarity between two location strings.
    
    Args:
        loc1: First location string
        loc2: Second location string
        
    Returns:
        Similarity score between 0 and 1
    """
    if not loc1 or not loc2:
        return 0
    
    # Standardize locations
    std_loc1 = standardize_location(loc1)
    std_loc2 = standardize_location(loc2)
    
    # If standardized locations match, return high similarity
    if std_loc1 == std_loc2:
        return 1.0
    
    # Calculate string similarity
    similarity = SequenceMatcher(None, std_loc1.lower(), std_loc2.lower()).ratio()
    
    return similarity

def load_neighborhood_data(filepath=None):
    """
    Load neighborhood data from file or use default data.
    
    Args:
        filepath: Path to neighborhood data file (optional)
        
    Returns:
        Dictionary of neighborhood statistics
    """
    if filepath and os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                if filepath.endswith('.json'):
                    data = json.load(f)
                elif filepath.endswith('.csv'):
                    df = pd.read_csv(filepath)
                    data = {}
                    for _, row in df.iterrows():
                        neighborhood = row.get('neighborhood')
                        if neighborhood:
                            data[neighborhood] = {
                                'avg_price_per_sqm': row.get('avg_price_per_sqm', 0),
                                'avg_rent_per_sqm': row.get('avg_rent_per_sqm', 0),
                                'avg_gross_yield': row.get('avg_gross_yield', 0),
                                'avg_cap_rate': row.get('avg_cap_rate', 0),
                                'avg_price': row.get('avg_price', 0),
                                'avg_rent': row.get('avg_rent', 0),
                                'property_count': row.get('property_count', 0)
                            }
            logger.info(f"Loaded neighborhood data from {filepath}")
            return data
        except Exception as e:
            logger.error(f"Error loading neighborhood data: {e}")
    
    # Default neighborhood data with average prices per sqm in Lisbon
    # These are sample/estimated values - should be replaced with actual data
    default_data = {
        "Alfama": {"avg_price_per_sqm": 4800, "avg_rent_per_sqm": 20, "avg_gross_yield": 0.05},
        "Baixa": {"avg_price_per_sqm": 5200, "avg_rent_per_sqm": 22, "avg_gross_yield": 0.051},
        "Bairro Alto": {"avg_price_per_sqm": 4700, "avg_rent_per_sqm": 21, "avg_gross_yield": 0.053},
        "Chiado": {"avg_price_per_sqm": 6000, "avg_rent_per_sqm": 25, "avg_gross_yield": 0.05},
        "Príncipe Real": {"avg_price_per_sqm": 5500, "avg_rent_per_sqm": 23, "avg_gross_yield": 0.05},
        "Avenidas Novas": {"avg_price_per_sqm": 4300, "avg_rent_per_sqm": 18, "avg_gross_yield": 0.05},
        "Campo de Ourique": {"avg_price_per_sqm": 4200, "avg_rent_per_sqm": 17, "avg_gross_yield": 0.049},
        "Estrela": {"avg_price_per_sqm": 4500, "avg_rent_per_sqm": 19, "avg_gross_yield": 0.051},
        "Graça": {"avg_price_per_sqm": 4100, "avg_rent_per_sqm": 18, "avg_gross_yield": 0.053},
        "Intendente": {"avg_price_per_sqm": 3600, "avg_rent_per_sqm": 16, "avg_gross_yield": 0.053},
        "Parque das Nações": {"avg_price_per_sqm": 4900, "avg_rent_per_sqm": 19, "avg_gross_yield": 0.047},
        "Belém": {"avg_price_per_sqm": 4000, "avg_rent_per_sqm": 16, "avg_gross_yield": 0.048},
        "Alvalade": {"avg_price_per_sqm": 3800, "avg_rent_per_sqm": 16, "avg_gross_yield": 0.051},
        "Benfica": {"avg_price_per_sqm": 2800, "avg_rent_per_sqm": 13, "avg_gross_yield": 0.056},
        "Alcântara": {"avg_price_per_sqm": 3900, "avg_rent_per_sqm": 16, "avg_gross_yield": 0.049},
        "Santos": {"avg_price_per_sqm": 4300, "avg_rent_per_sqm": 18, "avg_gross_yield": 0.05},
        "São Bento": {"avg_price_per_sqm": 4600, "avg_rent_per_sqm": 20, "avg_gross_yield": 0.052},
        "Lapa": {"avg_price_per_sqm": 5100, "avg_rent_per_sqm": 21, "avg_gross_yield": 0.049},
        "Arroios": {"avg_price_per_sqm": 3700, "avg_rent_per_sqm": 16, "avg_gross_yield": 0.052},
        "Mouraria": {"avg_price_per_sqm": 3800, "avg_rent_per_sqm": 17, "avg_gross_yield": 0.054},
        "Cais do Sodré": {"avg_price_per_sqm": 4400, "avg_rent_per_sqm": 19, "avg_gross_yield": 0.052},
        "Anjos": {"avg_price_per_sqm": 3500, "avg_rent_per_sqm": 15, "avg_gross_yield": 0.051},
        "Ajuda": {"avg_price_per_sqm": 2900, "avg_rent_per_sqm": 14, "avg_gross_yield": 0.058},
        "Areeiro": {"avg_price_per_sqm": 3600, "avg_rent_per_sqm": 15, "avg_gross_yield": 0.05},
        "Beato": {"avg_price_per_sqm": 3200, "avg_rent_per_sqm": 14, "avg_gross_yield": 0.053},
        "Lumiar": {"avg_price_per_sqm": 3300, "avg_rent_per_sqm": 14, "avg_gross_yield": 0.051},
        "Marvila": {"avg_price_per_sqm": 3000, "avg_rent_per_sqm": 13, "avg_gross_yield": 0.052},
        "Olivais": {"avg_price_per_sqm": 2900, "avg_rent_per_sqm": 13, "avg_gross_yield": 0.054},
        "Campolide": {"avg_price_per_sqm": 3400, "avg_rent_per_sqm": 15, "avg_gross_yield": 0.053},
        "Penha de França": {"avg_price_per_sqm": 3300, "avg_rent_per_sqm": 14, "avg_gross_yield": 0.051},
        "Santa Apolónia": {"avg_price_per_sqm": 3900, "avg_rent_per_sqm": 17, "avg_gross_yield": 0.052},
        "Unknown": {"avg_price_per_sqm": 4000, "avg_rent_per_sqm": 17, "avg_gross_yield": 0.051},
    }
    
    logger.info("Using default neighborhood data")
    return default_data

def calculate_neighborhood_avg_from_data(property_data_list):
    """
    Calculate neighborhood averages from a list of property data.
    
    Args:
        property_data_list: List of property dictionaries
        
    Returns:
        Dictionary of neighborhood statistics
    """
    if not property_data_list:
        return {}
    
    # Initialize dictionaries to store sum and count
    neighborhood_stats = defaultdict(lambda: {
        'price_sum': 0, 
        'size_sum': 0, 
        'rent_sum': 0, 
        'count': 0,
        'price_per_sqm_sum': 0,
        'rent_per_sqm_sum': 0,
        'yield_sum': 0,
        'cap_rate_sum': 0
    })
    
    # Aggregate data by neighborhood
    for prop in property_data_list:
        neighborhood = prop.get('neighborhood', 'Unknown')
        neighborhood = standardize_location(neighborhood)
        
        price = prop.get('price', 0)
        size = prop.get('size', 0)
        monthly_rent = prop.get('monthly_rent', 0)
        
        if price <= 0 or size <= 0:
            continue
        
        price_per_sqm = price / size if size > 0 else 0
        rent_per_sqm = monthly_rent / size if size > 0 and monthly_rent > 0 else 0
        gross_yield = (monthly_rent * 12) / price if price > 0 and monthly_rent > 0 else 0
        cap_rate = prop.get('cap_rate', 0)
        
        neighborhood_stats[neighborhood]['price_sum'] += price
        neighborhood_stats[neighborhood]['size_sum'] += size
        neighborhood_stats[neighborhood]['rent_sum'] += monthly_rent
        neighborhood_stats[neighborhood]['price_per_sqm_sum'] += price_per_sqm
        neighborhood_stats[neighborhood]['rent_per_sqm_sum'] += rent_per_sqm
        neighborhood_stats[neighborhood]['yield_sum'] += gross_yield
        neighborhood_stats[neighborhood]['cap_rate_sum'] += cap_rate
        neighborhood_stats[neighborhood]['count'] += 1
    
    # Calculate averages
    result = {}
    for neighborhood, stats in neighborhood_stats.items():
        count = stats['count']
        if count > 0:
            result[neighborhood] = {
                'avg_price': stats['price_sum'] / count,
                'avg_size': stats['size_sum'] / count,
                'avg_rent': stats['rent_sum'] / count,
                'avg_price_per_sqm': stats['price_per_sqm_sum'] / count,
                'avg_rent_per_sqm': stats['rent_per_sqm_sum'] / count,
                'avg_gross_yield': stats['yield_sum'] / count,
                'avg_cap_rate': stats['cap_rate_sum'] / count,
                'property_count': count
            }
    
    return result

def calculate_price_difference(property_data, neighborhood_data=None):
    """
    Calculate difference from neighborhood average price per sqm.
    
    Args:
        property_data: Dictionary containing property information
        neighborhood_data: Dictionary of neighborhood statistics (optional)
        
    Returns:
        Dictionary with price difference information
    """
    # Extract property data
    price = property_data.get('price', 0)
    size = property_data.get('size', 0)
    neighborhood = property_data.get('neighborhood', 'Unknown')
    
    # Standardize neighborhood name
    neighborhood = standardize_location(neighborhood)
    
    # Calculate property's price per sqm
    if size <= 0:
        return {
            'neighborhood': neighborhood,
            'neighborhood_avg_price_per_sqm': 0,
            'price_per_sqm': 0,
            'difference': 0,
            'difference_percentage': 0
        }
    
    price_per_sqm = price / size
    
    # If no neighborhood data provided, use defaults
    if neighborhood_data is None:
        neighborhood_data = load_neighborhood_data()
    
    # Get average price per sqm for the neighborhood
    neighborhood_stats = neighborhood_data.get(neighborhood, neighborhood_data.get('Unknown', {'avg_price_per_sqm': 0}))
    avg_price_per_sqm = neighborhood_stats.get('avg_price_per_sqm', 0)
    
    # Calculate absolute and percentage differences
    difference = price_per_sqm - avg_price_per_sqm
    difference_percentage = difference / avg_price_per_sqm if avg_price_per_sqm > 0 else 0
    
    return {
        'neighborhood': neighborhood,
        'neighborhood_avg_price_per_sqm': avg_price_per_sqm,
        'price_per_sqm': price_per_sqm,
        'difference': difference,
        'difference_percentage': difference_percentage
    }

def classify_property(property_data, neighborhood_data=None):
    """
    Classify property based on investment metrics and neighborhood comparison.
    
    Args:
        property_data: Dictionary containing property and investment metrics
        neighborhood_data: Dictionary of neighborhood statistics (optional)
        
    Returns:
        Classification string and reasoning
    """
    # Get price difference from neighborhood average
    price_diff = calculate_price_difference(property_data, neighborhood_data)
    price_diff_percentage = price_diff.get('difference_percentage', 0)
    
    # Get investment metrics
    gross_yield = property_data.get('gross_yield', 0)
    cap_rate = property_data.get('cap_rate', 0)
    cash_on_cash = property_data.get('cash_on_cash_return', 0)
    
    # Define conditions for each classification
    criteria = CLASSIFICATION_CRITERIA
    
    # Determine classification
    if (price_diff_percentage <= criteria['excellent_deal']['price_diff_percentage'] and
            gross_yield >= criteria['excellent_deal']['gross_yield'] and
            cap_rate >= criteria['excellent_deal']['cap_rate'] and
            cash_on_cash >= criteria['excellent_deal']['cash_on_cash']):
        classification = "Excellent Deal"
        reasoning = f"Price is {abs(price_diff_percentage)*100:.1f}% below neighborhood average, with strong returns."
    
    elif (price_diff_percentage <= criteria['good_deal']['price_diff_percentage'] and
            gross_yield >= criteria['good_deal']['gross_yield'] and
            cap_rate >= criteria['good_deal']['cap_rate'] and
            cash_on_cash >= criteria['good_deal']['cash_on_cash']):
        classification = "Good Deal"
        reasoning = f"Price is {abs(price_diff_percentage)*100:.1f}% below neighborhood average, with good returns."
    
    elif (price_diff_percentage <= criteria['fair_deal']['price_diff_percentage'] and
            gross_yield >= criteria['fair_deal']['gross_yield'] and
            cap_rate >= criteria['fair_deal']['cap_rate'] and
            cash_on_cash >= criteria['fair_deal']['cash_on_cash']):
        classification = "Fair Deal"
        reasoning = f"Price is close to neighborhood average, with acceptable returns."
    
    elif (price_diff_percentage <= criteria['poor_deal']['price_diff_percentage'] and
            gross_yield >= criteria['poor_deal']['gross_yield'] and
            cap_rate >= criteria['poor_deal']['cap_rate'] and
            cash_on_cash >= criteria['poor_deal']['cash_on_cash']):
        classification = "Poor Deal"
        reasoning = f"Price is above neighborhood average, with low returns."
    
    else:
        classification = "Not Recommended"
        reasoning = "Returns are too low compared to investment."
    
    return {
        'classification': classification,
        'reasoning': reasoning
    }

def generate_complete_property_analysis(property_data, investment_params=None, expense_params=None, neighborhood_data=None):
    """
    Generate complete property analysis with all required metrics.
    
    Args:
        property_data: Dictionary containing property information
        investment_params: Custom investment parameters (optional)
        expense_params: Custom expense parameters (optional)
        neighborhood_data: Dictionary of neighborhood statistics (optional)
        
    Returns:
        Dictionary with complete property analysis
    """
    # Import here to avoid circular imports
    from .investment_metrics import calculate_all_investment_metrics
    
    # Ensure neighborhood is standardized
    if 'neighborhood' in property_data:
        property_data['neighborhood'] = standardize_location(property_data['neighborhood'])
    elif 'address' in property_data:
        property_data['neighborhood'] = extract_neighborhood(property_data['address'])
    
    # Calculate investment metrics
    metrics = calculate_all_investment_metrics(property_data, investment_params, expense_params)
    
    # Calculate neighborhood price comparison
    price_diff = calculate_price_difference(property_data, neighborhood_data)
    
    # Classify property
    all_metrics = {**property_data, **metrics}  # Merge dictionaries
    classification = classify_property(all_metrics, neighborhood_data)
    
    # Combine all results
    result = {
        # Basic property information
        "price": metrics.get("price", 0),
        "size": metrics.get("size", 0),
        "monthly_rent": metrics.get("monthly_rent", 0),
        "annual_rent": metrics.get("annual_rent", 0),
        
        # Expenses and taxes
        "recurring_expenses": metrics.get("recurring_expenses", 0),
        "one_time_expenses": metrics.get("one_time_expenses", 0),
        "taxes": metrics.get("taxes", 0),
        
        # Investment metrics
        "noi": metrics.get("noi", 0),
        "cap_rate": metrics.get("cap_rate", 0),
        "gross_yield": metrics.get("gross_yield", 0),
        "cash_on_cash_return": metrics.get("cash_on_cash_return", 0),
        "monthly_cash_flow": metrics.get("monthly_cash_flow", 0),
        
        # Neighborhood analysis
        "neighborhood": price_diff.get("neighborhood", "Unknown"),
        "price_per_sqm": price_diff.get("price_per_sqm", 0),
        "neighborhood_avg_price_per_sqm": price_diff.get("neighborhood_avg_price_per_sqm", 0),
        "difference_from_avg": price_diff.get("difference", 0),
        "difference_percentage": price_diff.get("difference_percentage", 0),
        
        # Classification
        "classification": classification.get("classification", "Unknown"),
        "reasoning": classification.get("reasoning", "")
    }
    
    return result

if __name__ == "__main__":
    # Example usage
    property_data = {
        "price": 300000,
        "size": 75,
        "monthly_rent": 1200,
        "address": "Rua Example, Alfama, Lisboa",
        "neighborhood": "Alfama"
    }
    
    # Load neighborhood data
    neighborhood_data = load_neighborhood_data()
    
    # Calculate price difference
    price_diff = calculate_price_difference(property_data, neighborhood_data)
    print(f"Neighborhood: {price_diff['neighborhood']}")
    print(f"Property Price per sqm: €{price_diff['price_per_sqm']:.2f}")
    print(f"Neighborhood Average: €{price_diff['neighborhood_avg_price_per_sqm']:.2f}")
    print(f"Difference: €{price_diff['difference']:.2f} ({price_diff['difference_percentage']*100:.1f}%)")
    
    # Generate complete analysis
    from .investment_metrics import calculate_all_investment_metrics
    metrics = calculate_all_investment_metrics(property_data)
    all_metrics = {**property_data, **metrics}
    classification = classify_property(all_metrics, neighborhood_data)
    print(f"Classification: {classification['classification']}")
    print(f"Reasoning: {classification['reasoning']}") 