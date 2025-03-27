#!/usr/bin/env python3
"""
Price Estimator Module

This module handles property price estimation based on comparable properties,
location, size, and other attributes. It uses various data sources and methods
to provide accurate price estimates for properties.
"""

import os
import json
import logging
import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Union, Any
from pathlib import Path
from statistics import mean, median, stdev

# Configure logging
logger = logging.getLogger(__name__)

class PriceEstimator:
    """A class to estimate property prices based on comparable properties."""
    
    def __init__(self, rental_data_path: str = None, sales_data_path: str = None):
        """
        Initialize the price estimator with property data.
        
        Args:
            rental_data_path: Path to rental property data JSON file
            sales_data_path: Path to sales property data JSON file
        """
        self.rental_data = self._load_property_data(rental_data_path or "propbot/data/processed/rental_properties.json")
        self.sales_data = self._load_property_data(sales_data_path or "propbot/data/processed/sales_properties.json")
        
        # Convert to DataFrame for easier analysis
        self.rental_df = self._convert_to_dataframe(self.rental_data)
        self.sales_df = self._convert_to_dataframe(self.sales_data)
        
        logger.info(f"Initialized PriceEstimator with {len(self.rental_data)} rental and {len(self.sales_data)} sales properties")
    
    def _load_property_data(self, file_path: str) -> List[Dict]:
        """
        Load property data from a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            List of property dictionaries
        """
        try:
            if not os.path.exists(file_path):
                logger.warning(f"Property data file not found: {file_path}")
                return []
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Handle both list and dict formats
            if isinstance(data, dict) and 'properties' in data:
                return data['properties']
            elif isinstance(data, list):
                return data
            else:
                logger.warning(f"Unexpected data format in {file_path}")
                return []
                
        except Exception as e:
            logger.error(f"Error loading property data from {file_path}: {e}")
            return []
    
    def _convert_to_dataframe(self, properties: List[Dict]) -> pd.DataFrame:
        """
        Convert property data to a pandas DataFrame.
        
        Args:
            properties: List of property dictionaries
            
        Returns:
            DataFrame of properties
        """
        if not properties:
            return pd.DataFrame()
        
        # Create DataFrame
        df = pd.DataFrame(properties)
        
        # Ensure essential columns exist
        for col in ['price', 'size', 'neighborhood', 'rooms', 'bathrooms', 'property_type']:
            if col not in df.columns:
                df[col] = None
        
        # Convert numerical columns
        for col in ['price', 'size', 'rooms', 'bathrooms']:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass
        
        return df
    
    def get_price_per_sqm(self, property_data: Dict) -> float:
        """
        Calculate price per square meter for a property.
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            Price per square meter
        """
        try:
            price = float(property_data.get('price', 0))
            size = float(property_data.get('size', 0))
            
            if price <= 0 or size <= 0:
                return 0
            
            return price / size
        except:
            return 0
    
    def get_comparable_properties(self, 
                                 property_data: Dict, 
                                 is_rental: bool = False,
                                 max_results: int = 5,
                                 neighborhood_match: bool = True,
                                 size_range_pct: float = 0.2,
                                 room_match: bool = True) -> List[Dict]:
        """
        Find comparable properties based on location, size, and type.
        
        Args:
            property_data: Property data dictionary
            is_rental: Whether to find rental or sales comparable properties
            max_results: Maximum number of comparable properties to return
            neighborhood_match: Whether to require neighborhood match
            size_range_pct: Size range percentage for matching (e.g., 0.2 = ±20%)
            room_match: Whether to require room count match
            
        Returns:
            List of comparable property dictionaries
        """
        try:
            # Select appropriate dataset
            df = self.rental_df if is_rental else self.sales_df
            
            if df.empty:
                logger.warning("No property data available for comparables")
                return []
            
            # Extract property attributes
            neighborhood = property_data.get('neighborhood')
            size = float(property_data.get('size', 0))
            rooms = property_data.get('rooms')
            property_type = property_data.get('property_type')
            
            # Initial filtering
            filters = []
            
            if neighborhood and neighborhood_match:
                filters.append(df['neighborhood'] == neighborhood)
            
            if size > 0:
                min_size = size * (1 - size_range_pct)
                max_size = size * (1 + size_range_pct)
                filters.append((df['size'] >= min_size) & (df['size'] <= max_size))
            
            if property_type:
                filters.append(df['property_type'] == property_type)
            
            if rooms and room_match and not pd.isna(rooms):
                filters.append(df['rooms'] == rooms)
            
            # Apply filters
            if filters:
                filtered_df = df[np.logical_and.reduce(filters)]
            else:
                filtered_df = df
            
            # If no matches, relax constraints
            if len(filtered_df) < 2 and neighborhood_match:
                return self.get_comparable_properties(
                    property_data, 
                    is_rental, 
                    max_results,
                    False,  # Don't require neighborhood match
                    size_range_pct,
                    room_match
                )
            
            if len(filtered_df) < 2 and room_match:
                return self.get_comparable_properties(
                    property_data, 
                    is_rental, 
                    max_results,
                    neighborhood_match,
                    size_range_pct,
                    False  # Don't require room match
                )
            
            # Sort by similarity
            if size > 0:
                filtered_df['size_diff_pct'] = abs(filtered_df['size'] - size) / size
                filtered_df = filtered_df.sort_values('size_diff_pct')
            
            # Convert to list of dictionaries
            comparable_properties = filtered_df.head(max_results).to_dict('records')
            
            logger.info(f"Found {len(comparable_properties)} comparable properties")
            return comparable_properties
            
        except Exception as e:
            logger.error(f"Error finding comparable properties: {e}")
            return []
    
    def estimate_property_price(self, 
                              property_data: Dict, 
                              is_rental: bool = False,
                              comparable_count: int = 5) -> Dict:
        """
        Estimate property price based on comparable properties.
        
        Args:
            property_data: Property data dictionary
            is_rental: Whether to estimate rental or sales price
            comparable_count: Number of comparable properties to consider
            
        Returns:
            Dictionary with price estimate and statistics
        """
        try:
            # Get comparable properties
            comparables = self.get_comparable_properties(
                property_data,
                is_rental,
                max_results=comparable_count
            )
            
            if not comparables:
                logger.warning("No comparable properties found for estimation")
                return {
                    "estimated_price": None,
                    "price_per_sqm": None,
                    "comparables": [],
                    "comparable_count": 0,
                    "confidence": "low"
                }
            
            # Extract prices and sizes
            prices = [prop.get('price', 0) for prop in comparables if prop.get('price')]
            sizes = [prop.get('size', 0) for prop in comparables if prop.get('size')]
            
            # Calculate price per sqm
            price_per_sqm_values = []
            for prop in comparables:
                price = prop.get('price')
                size = prop.get('size')
                if price and size and size > 0:
                    price_per_sqm_values.append(price / size)
            
            # Calculate median price per sqm
            if price_per_sqm_values:
                median_price_per_sqm = median(price_per_sqm_values)
            else:
                median_price_per_sqm = 0
            
            # Estimate price based on median price per sqm and property size
            property_size = property_data.get('size', 0)
            if property_size > 0 and median_price_per_sqm > 0:
                estimated_price = property_size * median_price_per_sqm
            else:
                # If size information is missing, use median of comparable prices
                estimated_price = median(prices) if prices else None
            
            # Calculate statistics
            price_stats = self._calculate_price_statistics(prices) if prices else {}
            
            # Determine confidence level
            confidence = self._determine_confidence_level(comparables, property_data)
            
            # Prepare result
            result = {
                "estimated_price": round(estimated_price) if estimated_price else None,
                "price_per_sqm": round(median_price_per_sqm, 2) if median_price_per_sqm else None,
                "comparables": comparables,
                "comparable_count": len(comparables),
                "confidence": confidence,
                "statistics": price_stats
            }
            
            logger.info(f"Estimated {'rental' if is_rental else 'sales'} price: {result['estimated_price']} "
                       f"(confidence: {confidence})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error estimating property price: {e}")
            return {
                "estimated_price": None,
                "error": str(e),
                "comparable_count": 0,
                "confidence": "low"
            }
    
    def _calculate_price_statistics(self, prices: List[float]) -> Dict:
        """
        Calculate statistics for a list of prices.
        
        Args:
            prices: List of property prices
            
        Returns:
            Dictionary with price statistics
        """
        if not prices:
            return {}
        
        # Filter out zeros and None values
        valid_prices = [p for p in prices if p]
        
        if not valid_prices:
            return {}
        
        stats = {
            "min": min(valid_prices),
            "max": max(valid_prices),
            "mean": mean(valid_prices),
            "median": median(valid_prices)
        }
        
        # Calculate standard deviation if we have enough values
        if len(valid_prices) > 1:
            stats["std_dev"] = stdev(valid_prices)
            stats["std_dev_percent"] = (stats["std_dev"] / stats["mean"]) * 100
        
        return stats
    
    def _determine_confidence_level(self, comparables: List[Dict], property_data: Dict) -> str:
        """
        Determine the confidence level of the price estimate.
        
        Args:
            comparables: List of comparable properties
            property_data: Original property data
            
        Returns:
            Confidence level: "high", "medium", or "low"
        """
        if not comparables:
            return "low"
        
        # Factors influencing confidence
        count_factor = min(len(comparables) / 5, 1.0)  # More comparables = higher confidence
        
        # Check if neighborhood matches
        neighborhood = property_data.get('neighborhood')
        neighborhood_matches = sum(1 for prop in comparables if prop.get('neighborhood') == neighborhood)
        neighborhood_factor = neighborhood_matches / len(comparables)
        
        # Check size similarity
        size = property_data.get('size', 0)
        if size > 0:
            size_diffs = []
            for prop in comparables:
                prop_size = prop.get('size', 0)
                if prop_size > 0:
                    size_diffs.append(abs(prop_size - size) / size)
            
            size_factor = 1 - (sum(size_diffs) / len(size_diffs)) if size_diffs else 0
        else:
            size_factor = 0.5  # Neutral if no size information
        
        # Check property type match
        property_type = property_data.get('property_type')
        type_matches = sum(1 for prop in comparables if prop.get('property_type') == property_type)
        type_factor = type_matches / len(comparables) if property_type else 0.5
        
        # Calculate weighted confidence score
        weights = {
            "count": 0.25,
            "neighborhood": 0.3,
            "size": 0.25,
            "type": 0.2
        }
        
        confidence_score = (
            weights["count"] * count_factor +
            weights["neighborhood"] * neighborhood_factor +
            weights["size"] * size_factor +
            weights["type"] * type_factor
        )
        
        # Determine confidence level based on score
        if confidence_score >= 0.7:
            return "high"
        elif confidence_score >= 0.4:
            return "medium"
        else:
            return "low"
    
    def estimate_rental_yield(self, property_data: Dict) -> Dict:
        """
        Estimate rental yield for a property.
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            Dictionary with yield estimates and statistics
        """
        try:
            # Estimate sales price if not provided
            if not property_data.get('price'):
                sales_estimate = self.estimate_property_price(property_data, is_rental=False)
                sales_price = sales_estimate.get('estimated_price')
            else:
                sales_price = property_data.get('price')
            
            # Create a copy of property_data for rental estimation
            rental_property_data = property_data.copy()
            
            # Remove price to force estimation
            if 'price' in rental_property_data:
                del rental_property_data['price']
            
            # Estimate rental price
            rental_estimate = self.estimate_property_price(rental_property_data, is_rental=True)
            monthly_rental = rental_estimate.get('estimated_price')
            
            # Calculate annual rental income
            if monthly_rental:
                annual_rental = monthly_rental * 12
            else:
                annual_rental = None
            
            # Calculate gross yield
            if sales_price and annual_rental:
                gross_yield = (annual_rental / sales_price) * 100
            else:
                gross_yield = None
            
            # Calculate net yield (assuming typical expenses)
            if gross_yield:
                # Typical expenses as percentage of gross rental income
                property_tax_pct = 0.5  # IMI - Portuguese property tax
                management_pct = 5.0    # Property management fees
                maintenance_pct = 5.0   # Maintenance costs
                insurance_pct = 0.5     # Property insurance
                vacancy_pct = 8.0       # Vacancy rate
                
                # Total expenses percentage
                expenses_pct = property_tax_pct + management_pct + maintenance_pct + insurance_pct + vacancy_pct
                
                # Calculate net yield
                net_yield = gross_yield * (1 - expenses_pct / 100)
            else:
                net_yield = None
            
            # Prepare result
            result = {
                "sales_price": sales_price,
                "monthly_rental": monthly_rental,
                "annual_rental": annual_rental,
                "gross_yield_percent": round(gross_yield, 2) if gross_yield else None,
                "net_yield_percent": round(net_yield, 2) if net_yield else None,
                "rental_estimate": rental_estimate,
                "confidence": rental_estimate.get('confidence')
            }
            
            logger.info(f"Estimated rental yield: {result['gross_yield_percent']}% gross, {result['net_yield_percent']}% net")
            
            return result
            
        except Exception as e:
            logger.error(f"Error estimating rental yield: {e}")
            return {
                "gross_yield_percent": None,
                "net_yield_percent": None,
                "error": str(e),
                "confidence": "low"
            }
    
    def save_price_estimates(self, properties: List[Dict], output_file: str) -> Dict:
        """
        Generate and save price estimates for multiple properties.
        
        Args:
            properties: List of property dictionaries
            output_file: Path to save the results
            
        Returns:
            Statistics about the estimation process
        """
        start_time = pd.Timestamp.now()
        
        results = []
        successful_count = 0
        failed_count = 0
        
        for prop in properties:
            try:
                # Skip properties that already have price and price_per_sqm
                if prop.get('price') and prop.get('price_per_sqm'):
                    prop['estimation_skipped'] = True
                    results.append(prop)
                    continue
                
                # Determine whether it's a rental
                is_rental = prop.get('property_type', '').lower() == 'rental'
                
                # Get price estimate
                estimate = self.estimate_property_price(prop, is_rental=is_rental)
                
                # Add estimate to property data
                if estimate.get('estimated_price'):
                    prop['estimated_price'] = estimate['estimated_price']
                    prop['price_per_sqm'] = estimate['price_per_sqm']
                    prop['price_confidence'] = estimate['confidence']
                    successful_count += 1
                else:
                    prop['price_estimation_failed'] = True
                    failed_count += 1
                
                # Add to results
                results.append(prop)
                
            except Exception as e:
                logger.error(f"Error processing property for estimation: {e}")
                prop['price_estimation_error'] = str(e)
                failed_count += 1
                results.append(prop)
        
        # Save results
        try:
            output_path = Path(output_file)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved price estimates to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving price estimates: {e}")
        
        # Generate statistics
        stats = {
            "total_properties": len(properties),
            "successful_estimates": successful_count,
            "failed_estimates": failed_count,
            "output_file": output_file,
            "processing_time": (pd.Timestamp.now() - start_time).total_seconds()
        }
        
        return stats


def estimate_prices(input_file: str, output_file: str = None, rental_data: str = None, sales_data: str = None) -> Dict:
    """
    Estimate prices for properties from an input file.
    
    Args:
        input_file: Path to input JSON file with properties
        output_file: Path to save output. If None, derives from input file name.
        rental_data: Path to rental property data
        sales_data: Path to sales property data
        
    Returns:
        Statistics about the estimation process
    """
    # Derive output file name if not provided
    if output_file is None:
        input_path = Path(input_file)
        output_file = str(input_path.parent / f"{input_path.stem}_with_estimates{input_path.suffix}")
    
    try:
        # Load properties
        with open(input_file, 'r', encoding='utf-8') as f:
            properties = json.load(f)
        
        # Ensure properties is a list
        if isinstance(properties, dict) and 'properties' in properties:
            properties = properties['properties']
        elif not isinstance(properties, list):
            properties = [properties]  # Single property
        
        # Initialize price estimator
        estimator = PriceEstimator(rental_data, sales_data)
        
        # Generate and save estimates
        stats = estimator.save_price_estimates(properties, output_file)
        
        return stats
        
    except Exception as e:
        logger.error(f"Error in price estimation process: {e}")
        return {
            "error": str(e),
            "success": False
        }


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/analysis/price_estimator.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Estimate property prices')
    parser.add_argument('input_file', help='Path to input JSON file with properties')
    parser.add_argument('--output_file', help='Path to save output file with estimates')
    parser.add_argument('--rental_data', help='Path to rental property data')
    parser.add_argument('--sales_data', help='Path to sales property data')
    
    args = parser.parse_args()
    
    result = estimate_prices(args.input_file, args.output_file, args.rental_data, args.sales_data)
    print(json.dumps(result, indent=2)) 