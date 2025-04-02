"""Rental analysis module."""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

from .db_functions import (
    get_rental_listings_from_database,
    get_sales_listings_from_database
)

logger = logging.getLogger(__name__)

def analyze_rental_yields(
    rental_data: Optional[List[Dict[str, Any]]] = None,
    sales_data: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Analyze rental yields by location."""
    logger.info("Analyzing rental yields...")
    
    # Load data from database if not provided
    if rental_data is None:
        rental_data = get_rental_listings_from_database()
    if sales_data is None:
        sales_data = get_sales_listings_from_database()
    
    if not rental_data or not sales_data:
        logger.error("No rental or sales data available for analysis")
        return {}
    
    # Convert to DataFrames for easier analysis
    rentals_df = pd.DataFrame(rental_data)
    sales_df = pd.DataFrame(sales_data)
    
    # Calculate yields by location
    location_yields = {}
    for location in sales_df['location'].unique():
        location_rentals = rentals_df[rentals_df['location'] == location]
        location_sales = sales_df[sales_df['location'] == location]
        
        if len(location_rentals) == 0 or len(location_sales) == 0:
            continue
            
        avg_rental_price = location_rentals['price'].mean()
        avg_sales_price = location_sales['price'].mean()
        
        if avg_sales_price > 0:
            annual_yield = (avg_rental_price * 12) / avg_sales_price * 100
            location_yields[location] = {
                'annual_yield': annual_yield,
                'avg_rental_price': avg_rental_price,
                'avg_sales_price': avg_sales_price,
                'rental_count': len(location_rentals),
                'sales_count': len(location_sales)
            }
    
    # Calculate size-based metrics
    size_metrics = analyze_size_metrics(rentals_df, sales_df)
    
    # Combine results
    analysis = {
        'location_yields': location_yields,
        'size_metrics': size_metrics,
        'total_rentals': len(rental_data),
        'total_sales': len(sales_data),
        'analysis_date': datetime.now().isoformat()
    }
    
    return analysis

def analyze_size_metrics(rentals_df: pd.DataFrame, sales_df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze price relationships with property size."""
    metrics = {}
    
    # Rental price vs size regression
    if len(rentals_df) > 0:
        X = rentals_df['size'].values.reshape(-1, 1)
        y = rentals_df['price'].values
        reg = LinearRegression().fit(X, y)
        metrics['rental_size_coefficient'] = float(reg.coef_[0])
        metrics['rental_size_intercept'] = float(reg.intercept_)
        metrics['rental_size_r2'] = float(reg.score(X, y))
    
    # Sales price vs size regression
    if len(sales_df) > 0:
        X = sales_df['size'].values.reshape(-1, 1)
        y = sales_df['price'].values
        reg = LinearRegression().fit(X, y)
        metrics['sales_size_coefficient'] = float(reg.coef_[0])
        metrics['sales_size_intercept'] = float(reg.intercept_)
        metrics['sales_size_r2'] = float(reg.score(X, y))
    
    # Size distribution metrics
    if len(rentals_df) > 0:
        metrics['rental_size_mean'] = float(rentals_df['size'].mean())
        metrics['rental_size_median'] = float(rentals_df['size'].median())
        metrics['rental_size_std'] = float(rentals_df['size'].std())
    
    if len(sales_df) > 0:
        metrics['sales_size_mean'] = float(sales_df['size'].mean())
        metrics['sales_size_median'] = float(sales_df['size'].median())
        metrics['sales_size_std'] = float(sales_df['size'].std())
    
    return metrics

def save_analysis_results(analysis: Dict[str, Any], output_dir: str = "data/reports") -> str:
    """Save analysis results to a JSON file."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"rental_analysis_{timestamp}.json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as f:
        json.dump(analysis, f, indent=2)
    
    logger.info(f"Saved rental analysis results to {filepath}")
    return filepath

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analysis = analyze_rental_yields()
    if analysis:
        save_analysis_results(analysis) 