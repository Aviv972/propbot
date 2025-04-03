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

from .db_functions import (
    get_rental_listings_from_database, 
    get_sales_listings_from_database,
    save_rental_estimate,
    save_multiple_rental_estimates
)

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
        
        # Also create the rental_income_report_improved.json file
        rental_income_report = os.path.join(output_dir, 'rental_income_report_improved.json')
        
        # Convert all numeric values to float before saving
        results = convert_numeric_values(results)
        
        # Save results to both files
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        # Also save to rental_income_report_improved.json
        with open(rental_income_report, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Saved analysis results to {output_file}")
        logger.info(f"Also saved to {rental_income_report}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving analysis results: {e}")
        return False

def generate_rental_estimates(
    rental_data: Optional[pd.DataFrame] = None,
    sales_data: Optional[pd.DataFrame] = None,
    min_comparable_properties: int = 3,
    size_tolerance_percentage: float = 0.20  # 20% size tolerance
) -> Dict[str, Dict]:
    """
    Generate rental estimates for sales properties based on comparable rental listings.
    
    Args:
        rental_data: DataFrame with rental listings (optional, will load from DB if None)
        sales_data: DataFrame with sales listings (optional, will load from DB if None)
        min_comparable_properties: Minimum number of comparable properties required
        size_tolerance_percentage: Size tolerance percentage for comparable properties
        
    Returns:
        Dictionary mapping property URLs to rental estimates
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
            logger.warning("No rental data available for generating estimates")
            return {}
        
        # Convert numeric columns to float
        numeric_columns = ['price', 'size', 'price_per_sqm', 'rooms']
        rental_data = convert_dataframe_numeric(rental_data, numeric_columns)
        sales_data = convert_dataframe_numeric(sales_data, numeric_columns)
        
        # Filter out invalid rental data
        valid_rentals = rental_data[
            rental_data['price'].notna() & 
            (rental_data['price'] > 0) &
            rental_data['size'].notna() &
            (rental_data['size'] > 0)
        ]
        
        if valid_rentals.empty:
            logger.warning("No valid rental data available after filtering")
            return {}
            
        # Calculate price_per_sqm if not already present
        if 'price_per_sqm' not in valid_rentals.columns or valid_rentals['price_per_sqm'].isna().all():
            valid_rentals['price_per_sqm'] = valid_rentals['price'] / valid_rentals['size']
            
        # Group rentals by neighborhood
        neighborhood_rentals = valid_rentals.groupby('neighborhood')
        
        # Calculate neighborhood averages
        neighborhood_avg = {}
        for neighborhood, group in neighborhood_rentals:
            if len(group) >= min_comparable_properties:
                neighborhood_avg[neighborhood] = {
                    'avg_price': group['price'].mean(),
                    'avg_size': group['size'].mean(),
                    'avg_price_per_sqm': group['price_per_sqm'].mean(),
                    'count': len(group)
                }
        
        # Calculate citywide averages for fallback
        citywide_avg_price_per_sqm = valid_rentals['price_per_sqm'].mean()
        citywide_avg_rooms_price = {}
        
        for rooms_count, group in valid_rentals.groupby('rooms'):
            if not pd.isna(rooms_count) and len(group) >= min_comparable_properties:
                citywide_avg_rooms_price[rooms_count] = group['price'].mean()
        
        # Generate estimates for each sales property
        estimates = {}
        estimates_list = []
        
        for _, property_row in sales_data.iterrows():
            url = property_row.get('url')
            if not url:
                continue
                
            neighborhood = property_row.get('neighborhood')
            size = property_row.get('size')
            rooms = property_row.get('rooms')
            
            # Skip if size is missing or invalid
            if pd.isna(size) or size <= 0:
                continue
                
            # Find comparable properties in the same neighborhood
            comparable_properties = []
            property_estimate = {
                'url': url,
                'neighborhood': neighborhood,
                'size': size,
                'rooms': rooms,
                'estimated_monthly_rent': None,
                'price_per_sqm': None,
                'comparable_count': 0,
                'confidence': 'low'
            }
            
            # Try to find comparable properties with similar size in the same neighborhood
            if neighborhood and neighborhood in neighborhood_avg:
                size_lower = size * (1 - size_tolerance_percentage)
                size_upper = size * (1 + size_tolerance_percentage)
                
                neighborhood_properties = valid_rentals[valid_rentals['neighborhood'] == neighborhood]
                
                # Filter by size range
                size_filtered = neighborhood_properties[
                    (neighborhood_properties['size'] >= size_lower) &
                    (neighborhood_properties['size'] <= size_upper)
                ]
                
                # Further filter by room count if available
                if not pd.isna(rooms) and rooms > 0:
                    room_filtered = size_filtered[size_filtered['rooms'] == rooms]
                    if len(room_filtered) >= min_comparable_properties:
                        comparable_properties = room_filtered
                    else:
                        comparable_properties = size_filtered
                else:
                    comparable_properties = size_filtered
                    
                # Calculate estimate based on comparable properties
                if len(comparable_properties) >= min_comparable_properties:
                    avg_price = comparable_properties['price'].mean()
                    avg_price_per_sqm = comparable_properties['price_per_sqm'].mean()
                    
                    property_estimate['estimated_monthly_rent'] = avg_price
                    property_estimate['price_per_sqm'] = avg_price_per_sqm
                    property_estimate['comparable_count'] = len(comparable_properties)
                    property_estimate['confidence'] = 'high' if len(comparable_properties) >= 5 else 'medium'
            
            # If no comparable properties found, use neighborhood average price per sqm
            if not property_estimate['estimated_monthly_rent'] and neighborhood and neighborhood in neighborhood_avg:
                neighborhood_data = neighborhood_avg[neighborhood]
                avg_price_per_sqm = neighborhood_data['avg_price_per_sqm']
                
                property_estimate['estimated_monthly_rent'] = avg_price_per_sqm * size
                property_estimate['price_per_sqm'] = avg_price_per_sqm
                property_estimate['comparable_count'] = neighborhood_data['count']
                property_estimate['confidence'] = 'medium'
            
            # Fallback to citywide average if still no estimate
            if not property_estimate['estimated_monthly_rent']:
                # Try by room count first
                if not pd.isna(rooms) and rooms in citywide_avg_rooms_price:
                    property_estimate['estimated_monthly_rent'] = citywide_avg_rooms_price[rooms]
                    property_estimate['price_per_sqm'] = property_estimate['estimated_monthly_rent'] / size if size > 0 else None
                    property_estimate['confidence'] = 'low'
                # Then by price per sqm
                elif citywide_avg_price_per_sqm:
                    property_estimate['estimated_monthly_rent'] = citywide_avg_price_per_sqm * size
                    property_estimate['price_per_sqm'] = citywide_avg_price_per_sqm
                    property_estimate['confidence'] = 'low'
            
            # Add estimate to results
            if property_estimate['estimated_monthly_rent']:
                estimates[url] = property_estimate
                estimates_list.append(property_estimate)
        
        # Save estimates to database
        if estimates_list:
            save_multiple_rental_estimates(estimates_list)
            logger.info(f"Generated and saved {len(estimates_list)} rental estimates to database")
        
        return estimates
        
    except Exception as e:
        logger.error(f"Error generating rental estimates: {e}")
        return {}

def run_rental_analysis_pipeline():
    """Run the complete rental analysis pipeline and save results to database."""
    try:
        # Step 1: Load rental and sales data
        rental_data = pd.DataFrame(get_rental_listings_from_database())
        sales_data = pd.DataFrame(get_sales_listings_from_database())
        
        logger.info(f"Loaded {len(rental_data)} rental listings and {len(sales_data)} sales listings")
        
        # Step 2: Generate rental estimates for individual properties
        rental_estimates = generate_rental_estimates(
            rental_data=rental_data,
            sales_data=sales_data,
            min_comparable_properties=3,
            size_tolerance_percentage=0.20
        )
        
        # Step 3: Analyze overall rental yields
        rental_yields = analyze_rental_yields(
            rental_data=rental_data,
            sales_data=sales_data
        )
        
        # Step 4: Save analysis results
        save_analysis_results(rental_yields)
        
        return {
            "total_properties_analyzed": len(sales_data),
            "rental_estimates_generated": len(rental_estimates),
            "analysis_date": datetime.now().isoformat(),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"Error in rental analysis pipeline: {e}")
        return {
            "error": str(e),
            "analysis_date": datetime.now().isoformat(),
            "success": False
        }

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = run_rental_analysis_pipeline()
    logger.info(f"Rental analysis pipeline completed with result: {result['success']}") 