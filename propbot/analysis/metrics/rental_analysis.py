"""Rental analysis module."""
import os
import json
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

from .db_functions import (
    get_rental_listings_from_database,
    get_sales_listings_from_database
)

logger = logging.getLogger(__name__)

def convert_decimal_to_float(value):
    """Convert decimal values to float, handling None values."""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

def convert_dataframe_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Convert all numeric columns in a DataFrame to float."""
    numeric_columns = ['price', 'size', 'price_per_sqm', 'rooms', 'bathrooms']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_decimal_to_float)
    return df

def analyze_rental_yields(
    rental_data: Optional[List[Dict[str, Any]]] = None,
    sales_data: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """Analyze rental yields by location."""
    logger.info("Analyzing rental yields...")
    
    try:
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
        
        # Convert all numeric columns to float
        rentals_df = convert_dataframe_numeric(rentals_df)
        sales_df = convert_dataframe_numeric(sales_df)
        
        # Calculate yields by location
        location_yields = {}
        for location in sales_df['location'].unique():
            location_rentals = rentals_df[rentals_df['location'] == location]
            location_sales = sales_df[sales_df['location'] == location]
            
            if len(location_rentals) == 0 or len(location_sales) == 0:
                continue
                
            avg_rental_price = location_rentals['price'].mean()
            avg_sales_price = location_sales['price'].mean()
            
            if pd.notna(avg_sales_price) and avg_sales_price > 0 and pd.notna(avg_rental_price):
                annual_yield = (avg_rental_price * 12) / avg_sales_price * 100
                location_yields[location] = {
                    'annual_yield': float(annual_yield),
                    'avg_rental_price': float(avg_rental_price),
                    'avg_sales_price': float(avg_sales_price),
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
    except Exception as e:
        logger.error(f"Error in rental analysis: {str(e)}")
        return {}

def analyze_size_metrics(rentals_df: pd.DataFrame, sales_df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze price relationships with property size."""
    metrics = {}
    
    try:
        # Filter out rows with missing values
        valid_rentals = rentals_df.dropna(subset=['size', 'price'])
        valid_sales = sales_df.dropna(subset=['size', 'price'])
        
        # Rental price vs size regression
        if len(valid_rentals) > 0:
            X = valid_rentals['size'].values.reshape(-1, 1)
            y = valid_rentals['price'].values
            reg = LinearRegression().fit(X, y)
            metrics['rental_size_coefficient'] = float(reg.coef_[0])
            metrics['rental_size_intercept'] = float(reg.intercept_)
            metrics['rental_size_r2'] = float(reg.score(X, y))
        
        # Sales price vs size regression
        if len(valid_sales) > 0:
            X = valid_sales['size'].values.reshape(-1, 1)
            y = valid_sales['price'].values
            reg = LinearRegression().fit(X, y)
            metrics['sales_size_coefficient'] = float(reg.coef_[0])
            metrics['sales_size_intercept'] = float(reg.intercept_)
            metrics['sales_size_r2'] = float(reg.score(X, y))
        
        # Size distribution metrics
        if len(valid_rentals) > 0:
            metrics['rental_size_mean'] = float(valid_rentals['size'].mean())
            metrics['rental_size_median'] = float(valid_rentals['size'].median())
            metrics['rental_size_std'] = float(valid_rentals['size'].std())
        
        if len(valid_sales) > 0:
            metrics['sales_size_mean'] = float(valid_sales['size'].mean())
            metrics['sales_size_median'] = float(valid_sales['size'].median())
            metrics['sales_size_std'] = float(valid_sales['size'].std())
        
        return metrics
    except Exception as e:
        logger.error(f"Error in size metrics analysis: {str(e)}")
        return {}

def save_analysis_results(analysis: Dict[str, Any], output_dir: str = "data/reports") -> str:
    """Save analysis results to a JSON file."""
    try:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"rental_analysis_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        # Ensure all numeric values are converted to float
        def convert_numeric_values(obj):
            if isinstance(obj, dict):
                return {k: convert_numeric_values(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numeric_values(i) for i in obj]
            elif isinstance(obj, (Decimal, np.float64, np.int64)):
                return float(obj)
            return obj
        
        converted_analysis = convert_numeric_values(analysis)
        
        with open(filepath, 'w') as f:
            json.dump(converted_analysis, f, indent=2)
        
        logger.info(f"Saved rental analysis results to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        return ""

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analysis = analyze_rental_yields()
    if analysis:
        save_analysis_results(analysis) 