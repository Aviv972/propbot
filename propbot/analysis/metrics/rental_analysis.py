"""
Rental Analysis Module

This module analyzes rental yields and related metrics for properties.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from decimal import Decimal
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
from datetime import datetime

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

from .db_functions import get_rental_listings_from_database, get_sales_listings_from_database

# Configure logging
logger = logging.getLogger(__name__)

def convert_decimal_to_float(value: Any) -> Optional[float]:
    """
    Convert a value to float, handling None and Decimal types.
    
    Args:
        value: Value to convert
        
    Returns:
        Float value or None if conversion fails
    """
    if value is None or pd.isna(value):
        return None
    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value)
    except (ValueError, TypeError):
        return None
    
def convert_dataframe_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Convert specified numeric columns in a DataFrame to float.
    
    Args:
        df: Input DataFrame
        columns: List of column names to convert
        
    Returns:
        DataFrame with converted columns
    """
    if df is None or df.empty:
        return pd.DataFrame()
    
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_decimal_to_float)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def convert_numeric_values(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert all numeric values in a dictionary to float.
    
    Args:
        data: Input dictionary
        
    Returns:
        Dictionary with numeric values converted to float
    """
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result[key] = convert_numeric_values(value)
        elif isinstance(value, (Decimal, np.float32, np.float64, np.int32, np.int64)):
            result[key] = float(value)
        elif pd.isna(value):
            result[key] = None
        else:
            result[key] = value
    return result

def analyze_rental_yields(rental_data: Optional[pd.DataFrame] = None,
                        sales_data: Optional[pd.DataFrame] = None,
                        location: Optional[str] = None) -> Dict[str, Any]:
    """
    Analyze rental yields by location.
    
    Args:
        rental_data: DataFrame with rental listings (optional, will load from DB if None)
        sales_data: DataFrame with sales listings (optional, will load from DB if None)
        location: Location to analyze (e.g., neighborhood)
        
    Returns:
        Dictionary with analysis results
    """
    try:
        # Load data from database if not provided
        if rental_data is None:
            rental_data = pd.DataFrame(get_rental_listings_from_database())
        if sales_data is None:
            sales_data = pd.DataFrame(get_sales_listings_from_database())
            
        # Initialize empty DataFrames if None
        rental_data = pd.DataFrame() if rental_data is None else rental_data.copy()
        sales_data = pd.DataFrame() if sales_data is None else sales_data.copy()
        
        # Early return if rental data is empty
        if rental_data.empty:
            logger.warning("No rental data available for analysis")
            return {
                'location': location,
                'avg_rental_price': None,
                'avg_sales_price': None,
                'annual_yield': None,
                'total_rentals': 0,
                'total_sales': 0,
                'size_metrics': {},
                'analysis_date': datetime.now().isoformat()
            }
        
        # Convert numeric columns to float
        numeric_columns = ['price', 'size', 'price_per_sqm']
        rental_data = convert_dataframe_numeric(rental_data, numeric_columns)
        sales_data = convert_dataframe_numeric(sales_data, numeric_columns)
        
        # Filter by location if specified
        if location:
            if not rental_data.empty and 'location' in rental_data.columns:
                rental_data = rental_data[rental_data['location'] == location]
            if not sales_data.empty and 'location' in sales_data.columns:
                sales_data = sales_data[sales_data['location'] == location]
        
        # Drop rows with invalid prices (None, NaN, 0, or negative)
        rental_data = rental_data[
            rental_data['price'].notna() & 
            (rental_data['price'] > 0)
        ]
        sales_data = sales_data[
            sales_data['price'].notna() & 
            (sales_data['price'] > 0)
        ]
        
        # Calculate average rental and sales prices
        avg_rental_price = rental_data['price'].mean() if not rental_data.empty else None
        avg_sales_price = sales_data['price'].mean() if not sales_data.empty else None
        
        # Calculate annual yield if both prices are available and valid
        annual_yield = None
        if avg_rental_price is not None and avg_sales_price is not None:
            if not pd.isna(avg_rental_price) and not pd.isna(avg_sales_price):
                try:
                    avg_rental_price = float(avg_rental_price)
                    avg_sales_price = float(avg_sales_price)
                    if avg_sales_price > 0:  # Only check sales price > 0, rental price already filtered
                        annual_yield = (avg_rental_price * 12) / avg_sales_price
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to convert rental or sales price to float: {e}")
                    annual_yield = None
        
        # Get size metrics
        size_metrics = analyze_size_metrics(rental_data, sales_data)
        
        # Prepare results
        results = {
            'location': location,
            'avg_rental_price': convert_decimal_to_float(avg_rental_price),
            'avg_sales_price': convert_decimal_to_float(avg_sales_price),
            'annual_yield': convert_decimal_to_float(annual_yield),
            'total_rentals': len(rental_data) if not rental_data.empty else 0,
            'total_sales': len(sales_data) if not sales_data.empty else 0,
            'size_metrics': size_metrics,
            'analysis_date': datetime.now().isoformat()
        }
        
        # Convert all numeric values to float
        results = convert_numeric_values(results)
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing rental yields: {e}")
        return {}

def analyze_size_metrics(rental_data: Optional[pd.DataFrame], 
                        sales_data: Optional[pd.DataFrame]) -> Dict[str, Any]:
    """
    Analyze relationship between property size and price.
    
    Args:
        rental_data: DataFrame with rental listings
        sales_data: DataFrame with sales listings
        
    Returns:
        Dictionary with size metrics
    """
    try:
        metrics = {}
        
        # Analyze rental data
        if rental_data is not None and not rental_data.empty:
            valid_rentals = rental_data.dropna(subset=['size', 'price'])
            if not valid_rentals.empty:
                rental_coef = np.polyfit(valid_rentals['size'], valid_rentals['price'], 1)
                metrics['rental_size_coef'] = float(rental_coef[0])
                metrics['rental_size_intercept'] = float(rental_coef[1])
                if 'price_per_sqm' in valid_rentals.columns:
                    metrics['rental_avg_price_per_sqm'] = float(valid_rentals['price_per_sqm'].mean())
        
        # Analyze sales data
        if sales_data is not None and not sales_data.empty:
            valid_sales = sales_data.dropna(subset=['size', 'price'])
            if not valid_sales.empty:
                sales_coef = np.polyfit(valid_sales['size'], valid_sales['price'], 1)
                metrics['sales_size_coef'] = float(sales_coef[0])
                metrics['sales_size_intercept'] = float(sales_coef[1])
                if 'price_per_sqm' in valid_sales.columns:
                    metrics['sales_avg_price_per_sqm'] = float(valid_sales['price_per_sqm'].mean())
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error analyzing size metrics: {e}")
        return {}

def save_analysis_results(results: Dict[str, Any], output_dir: Union[str, Path] = None) -> bool:
    """
    Save analysis results to a JSON file.
    
    Args:
        results: Analysis results dictionary
        output_dir: Directory to save the results, defaults to propbot/data/processed
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Set default output directory if not provided
        if output_dir is None:
            # Try to find the project root and data directory
            current_dir = Path(__file__).resolve().parent
            project_root = current_dir.parent.parent.parent  # Go up three levels
            output_dir = project_root / "propbot" / "data" / "processed"
            
            # Fallback paths if the project structure is different
            if not output_dir.exists():
                fallback_paths = [
                    Path("/app/propbot/data/processed"),
                    Path.cwd() / "propbot" / "data" / "processed",
                    Path.cwd() / "data" / "processed",
                    Path.home() / "propbot" / "data" / "processed"
                ]
                
                for path in fallback_paths:
                    if path.exists():
                        output_dir = path
                        break
                        
            logger.info(f"Using default output directory: {output_dir}")
        
        # Convert Path to string
        output_dir = str(output_dir)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate output filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(output_dir, f'rental_analysis_{timestamp}.json')
        
        # Convert all numeric values to float before saving
        results = convert_numeric_values(results)
        
        # Save results
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved analysis results to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving analysis results: {e}")
        return False

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    analysis = analyze_rental_yields()
    if analysis:
        save_analysis_results(analysis) 