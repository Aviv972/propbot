#!/usr/bin/env python3
"""
Data Validator Module

This module provides functions to validate and verify data quality
for property listings, rental information, and calculated metrics.
It helps identify and flag potential data issues before they affect analysis.
"""

import os
import re
import json
import pandas as pd
import numpy as np
from pathlib import Path

# Set up paths
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = BASE_DIR / "data"
PROCESSED_DIR = DATA_DIR / "processed"
OUTPUT_DIR = DATA_DIR / "output" / "reports"

def validate_property_sizes(data_frame, column="size", max_reasonable_size=500, min_size=10):
    """
    Validate property sizes and flag potentially incorrect values.
    
    Args:
        data_frame: Pandas DataFrame containing property data
        column: Column name containing size data
        max_reasonable_size: Maximum reasonable property size in square meters
        min_size: Minimum reasonable property size in square meters
        
    Returns:
        Tuple of (valid_df, flagged_df, validation_stats)
    """
    if column not in data_frame.columns:
        return data_frame, pd.DataFrame(), {"error": f"Column {column} not found"}
    
    # Make a copy to avoid modifying the original
    df = data_frame.copy()
    
    # Filter out missing values
    df = df.dropna(subset=[column])
    
    # Check for room type prefixes (T1, T2, etc.) that might affect size parsing
    def check_t_prefix_issue(row):
        size = row[column]
        room_type = row.get('room_type', '')
        
        # Skip if missing data
        if pd.isna(size) or pd.isna(room_type):
            return False
            
        # Check if room type is T1, T2, etc. and size has corresponding prefix
        if isinstance(room_type, str) and room_type.startswith('T'):
            try:
                # Extract digit from room type (e.g., '2' from 'T2')
                room_digit = room_type[1:2]
                if room_digit.isdigit():
                    # Check if size starts with this digit and is unusually large
                    size_str = str(int(size)) if isinstance(size, (int, float)) else str(size)
                    if size_str.startswith(room_digit) and len(size_str) >= 3:
                        return True
            except (IndexError, ValueError):
                pass
        return False
    
    # Flag potentially problematic sizes
    df['size_issue'] = False
    
    # Size is unreasonably large
    size_too_large = df[column] > max_reasonable_size
    df.loc[size_too_large, 'size_issue'] = True
    df.loc[size_too_large, 'issue_type'] = 'too_large'
    
    # Size is unreasonably small
    size_too_small = df[column] < min_size
    df.loc[size_too_small, 'size_issue'] = True
    df.loc[size_too_small, 'issue_type'] = 'too_small'
    
    # Size might have T-prefix issue
    df['t_prefix_issue'] = df.apply(check_t_prefix_issue, axis=1)
    df.loc[df['t_prefix_issue'], 'size_issue'] = True
    df.loc[df['t_prefix_issue'], 'issue_type'] = 't_prefix_issue'
    
    # Split into valid and flagged dataframes
    flagged_df = df[df['size_issue']]
    valid_df = df[~df['size_issue']]
    
    # Calculate statistics
    stats = {
        "total_properties": len(df),
        "valid_properties": len(valid_df),
        "flagged_properties": len(flagged_df),
        "percent_flagged": round(len(flagged_df) / max(len(df), 1) * 100, 2),
        "issues": {
            "too_large": int(size_too_large.sum()),
            "too_small": int(size_too_small.sum()),
            "t_prefix_issue": int(df['t_prefix_issue'].sum())
        }
    }
    
    return valid_df, flagged_df, stats

def validate_prices(data_frame, price_column="price", max_price=5000000, min_price=50000, is_rental=False):
    """
    Validate property prices and flag potentially incorrect values.
    
    Args:
        data_frame: Pandas DataFrame containing property data
        price_column: Column name containing price data
        max_price: Maximum reasonable property price
        min_price: Minimum reasonable property price
        is_rental: Whether the data is for rentals (different price ranges)
        
    Returns:
        Tuple of (valid_df, flagged_df, validation_stats)
    """
    if price_column not in data_frame.columns:
        return data_frame, pd.DataFrame(), {"error": f"Column {price_column} not found"}
    
    # Make a copy to avoid modifying the original
    df = data_frame.copy()
    
    # Filter out missing values
    df = df.dropna(subset=[price_column])
    
    # Adjust min/max for rentals
    if is_rental:
        max_price = 10000  # Maximum reasonable monthly rent
        min_price = 300    # Minimum reasonable monthly rent
    
    # Flag potentially problematic prices
    df['price_issue'] = False
    
    # Price is unreasonably high
    price_too_high = df[price_column] > max_price
    df.loc[price_too_high, 'price_issue'] = True
    df.loc[price_too_high, 'issue_type'] = 'too_high'
    
    # Price is unreasonably low
    price_too_low = df[price_column] < min_price
    df.loc[price_too_low, 'price_issue'] = True
    df.loc[price_too_low, 'issue_type'] = 'too_low'
    
    # Split into valid and flagged dataframes
    flagged_df = df[df['price_issue']]
    valid_df = df[~df['price_issue']]
    
    # Calculate statistics
    stats = {
        "total_properties": len(df),
        "valid_properties": len(valid_df),
        "flagged_properties": len(flagged_df),
        "percent_flagged": round(len(flagged_df) / max(len(df), 1) * 100, 2),
        "issues": {
            "too_high": int(price_too_high.sum()),
            "too_low": int(price_too_low.sum())
        }
    }
    
    return valid_df, flagged_df, stats

def validate_yield_calculations(data_frame, yield_column="gross_yield", max_yield=15, min_yield=1):
    """
    Validate yield calculations and flag potentially incorrect values.
    
    Args:
        data_frame: Pandas DataFrame containing property data
        yield_column: Column name containing yield data
        max_yield: Maximum reasonable yield percentage
        min_yield: Minimum reasonable yield percentage
        
    Returns:
        Tuple of (valid_df, flagged_df, validation_stats)
    """
    if yield_column not in data_frame.columns:
        return data_frame, pd.DataFrame(), {"error": f"Column {yield_column} not found"}
    
    # Make a copy to avoid modifying the original
    df = data_frame.copy()
    
    # Filter out missing values
    df = df.dropna(subset=[yield_column])
    
    # Flag potentially problematic yields
    df['yield_issue'] = False
    
    # Yield is unreasonably high
    yield_too_high = df[yield_column] > max_yield
    df.loc[yield_too_high, 'yield_issue'] = True
    df.loc[yield_too_high, 'issue_type'] = 'too_high'
    
    # Yield is unreasonably low
    yield_too_low = df[yield_column] < min_yield
    df.loc[yield_too_low, 'yield_issue'] = True
    df.loc[yield_too_low, 'issue_type'] = 'too_low'
    
    # Split into valid and flagged dataframes
    flagged_df = df[df['yield_issue']]
    valid_df = df[~df['yield_issue']]
    
    # Calculate statistics
    stats = {
        "total_properties": len(df),
        "valid_properties": len(valid_df),
        "flagged_properties": len(flagged_df),
        "percent_flagged": round(len(flagged_df) / max(len(df), 1) * 100, 2),
        "issues": {
            "too_high": int(yield_too_high.sum()),
            "too_low": int(yield_too_low.sum())
        }
    }
    
    return valid_df, flagged_df, stats

def run_data_validation():
    """
    Run all data validation checks and generate a validation report.
    
    Returns:
        Dictionary with validation results
    """
    report = {
        "timestamp": pd.Timestamp.now().isoformat(),
        "validation_results": {}
    }
    
    # Validate rental data
    try:
        rentals_path = PROCESSED_DIR / "rentals_current.csv"
        if os.path.exists(rentals_path):
            rentals_df = pd.read_csv(rentals_path)
            
            # Validate sizes
            _, _, size_stats = validate_property_sizes(rentals_df)
            report["validation_results"]["rental_sizes"] = size_stats
            
            # Validate prices
            _, _, price_stats = validate_prices(rentals_df, is_rental=True)
            report["validation_results"]["rental_prices"] = price_stats
    except Exception as e:
        report["validation_results"]["rental_validation_error"] = str(e)
    
    # Validate sales data
    try:
        sales_path = PROCESSED_DIR / "sales_current.csv"
        if os.path.exists(sales_path):
            sales_df = pd.read_csv(sales_path)
            
            # Validate sizes
            _, _, size_stats = validate_property_sizes(sales_df)
            report["validation_results"]["sales_sizes"] = size_stats
            
            # Validate prices
            _, _, price_stats = validate_prices(sales_df, is_rental=False)
            report["validation_results"]["sales_prices"] = price_stats
    except Exception as e:
        report["validation_results"]["sales_validation_error"] = str(e)
    
    # Validate investment metrics
    try:
        metrics_path = OUTPUT_DIR / "investment_metrics_current.csv"
        if os.path.exists(metrics_path):
            metrics_df = pd.read_csv(metrics_path)
            
            # Validate yields
            _, _, yield_stats = validate_yield_calculations(metrics_df)
            report["validation_results"]["yield_calculations"] = yield_stats
    except Exception as e:
        report["validation_results"]["metrics_validation_error"] = str(e)
    
    # Save report
    try:
        report_path = OUTPUT_DIR / "data_validation_report.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
    except Exception as e:
        print(f"Error saving validation report: {e}")
    
    return report

if __name__ == "__main__":
    validation_report = run_data_validation()
    print(json.dumps(validation_report, indent=2)) 