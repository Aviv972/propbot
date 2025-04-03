"""Rental metrics calculation module."""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

import pandas as pd

from .db_functions import (
    get_rental_listings_from_database,
    get_rental_last_update,
    set_rental_last_update
)

logger = logging.getLogger(__name__)

# Constants
MAX_RENTAL_PRICE_PER_SQM = 45  # Maximum reasonable rental price per square meter

def load_complete_rental_data() -> List[Dict[str, Any]]:
    """Load rental data from database."""
    logger.info("Loading rental data from database...")
    
    # Load from database
    rental_data = get_rental_listings_from_database()
    if rental_data:
        logger.info(f"Loaded {len(rental_data)} rental properties from database")
        return filter_valid_rentals(rental_data)
    
    logger.error("No rental data found in database")
return []
    
def filter_valid_rentals(rental_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter out invalid rental properties."""
    valid_rentals = []
    outliers = []
    
    for rental in rental_data:
        # Skip if missing required data
        if not all(k in rental and rental[k] is not None for k in ['price', 'size']):
            continue
            
        # Convert price and size to float if they're strings
        try:
            price = float(rental['price']) if isinstance(rental['price'], (str, int)) else rental['price']
            size = float(rental['size']) if isinstance(rental['size'], (str, int)) else rental['size']
            
            # Skip if invalid price or size
            if price is None or size is None or price <= 0 or size <= 0:
                continue
        
            # Calculate price per sqm if missing
            price_per_sqm = rental.get('price_per_sqm')
            if price_per_sqm is None:
                price_per_sqm = price / size
            
            # Skip if price per sqm is too high
            if price_per_sqm > MAX_RENTAL_PRICE_PER_SQM:
                outliers.append(rental)
                continue
        
            rental['price_per_sqm'] = price_per_sqm
            valid_rentals.append(rental)
        except (TypeError, ValueError) as e:
            # Log error and skip this rental
            logger.warning(f"Error processing rental: {e} - {rental.get('url', 'unknown URL')}")
            continue
    
    logger.info(f"Filtered to {len(valid_rentals)} valid rental properties")
    if outliers:
        logger.info(f"Excluded {len(outliers)} outliers with price_per_sqm > {MAX_RENTAL_PRICE_PER_SQM}")
    
    return valid_rentals

def calculate_rental_metrics(rental_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate rental market metrics."""
    if not rental_data:
        logger.warning("No rental data available for metrics calculation")
        return {}
        
    metrics = {
        'total_properties': len(rental_data),
        'avg_price': sum(r['price'] for r in rental_data) / len(rental_data),
        'avg_size': sum(r['size'] for r in rental_data) / len(rental_data),
        'avg_price_per_sqm': sum(r['price_per_sqm'] for r in rental_data) / len(rental_data),
        'min_price': min(r['price'] for r in rental_data),
        'max_price': max(r['price'] for r in rental_data),
        'min_size': min(r['size'] for r in rental_data),
        'max_size': max(r['size'] for r in rental_data),
        'min_price_per_sqm': min(r['price_per_sqm'] for r in rental_data),
        'max_price_per_sqm': max(r['price_per_sqm'] for r in rental_data),
    }
    
    # Add location-based metrics
    location_prices = {}
    for rental in rental_data:
        location = rental.get('location', 'Unknown')
        if location not in location_prices:
            location_prices[location] = []
        location_prices[location].append(rental['price_per_sqm'])
    
    metrics['location_avg_price_per_sqm'] = {
        loc: sum(prices) / len(prices)
        for loc, prices in location_prices.items()
    }
    
    return metrics

def update_rental_metrics() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Update rental metrics from latest data."""
    rental_data = load_complete_rental_data()
    metrics = calculate_rental_metrics(rental_data)
    
    # Update last update timestamp in database
    if rental_data:
        set_rental_last_update(datetime.now())
    
    return rental_data, metrics 

def run_improved_analysis(similarity_threshold: int = 40, min_comparable_properties: int = 2) -> Dict[str, Any]:
    """Run improved rental analysis with location-based comparison.
    
    Args:
        similarity_threshold: Minimum similarity score for comparable properties
        min_comparable_properties: Minimum number of comparable properties required
        
    Returns:
        Dictionary containing analysis results
    """
    logger.info("Running improved rental analysis...")
    
    # Load rental data
    rental_data = load_complete_rental_data()
    if not rental_data:
        logger.error("No rental data available for analysis")
        return {}
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(rental_data)
    
    # Calculate price per square meter if missing
    if 'price_per_sqm' not in df.columns:
        df['price_per_sqm'] = df['price'] / df['size']
    
    # Group by location and calculate metrics
    location_metrics = {}
    for location, group in df.groupby('location'):
        if len(group) >= min_comparable_properties:
            location_metrics[location] = {
                'avg_price': group['price'].mean(),
                'avg_size': group['size'].mean(),
                'avg_price_per_sqm': group['price_per_sqm'].mean(),
                'count': len(group)
            }
    
    # Calculate overall metrics
    overall_metrics = {
        'total_properties': len(df),
        'avg_price': df['price'].mean(),
        'avg_size': df['size'].mean(),
        'avg_price_per_sqm': df['price_per_sqm'].mean(),
        'locations_analyzed': len(location_metrics)
    }
    
    # Prepare results
    results = {
        'overall_metrics': overall_metrics,
        'location_metrics': location_metrics,
        'analysis_date': datetime.now().isoformat()
    }
    
    logger.info(f"Analysis completed: {overall_metrics['total_properties']} properties analyzed across {overall_metrics['locations_analyzed']} locations")
    return results 