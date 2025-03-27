#!/usr/bin/env python3
"""
Location Analyzer Module

This module analyzes property data by neighborhood and location, generating reports 
and metrics about different areas to help with investment decisions.
"""

import os
import json
import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List, Optional, Union, Any, Tuple
from pathlib import Path
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class LocationAnalyzer:
    """A class to analyze property data by neighborhood and location."""
    
    def __init__(self, 
                rental_data_path: str = None, 
                sales_data_path: str = None,
                output_dir: str = None):
        """
        Initialize the location analyzer with property data.
        
        Args:
            rental_data_path: Path to rental property data JSON file
            sales_data_path: Path to sales property data JSON file
            output_dir: Directory to save output files
        """
        self.rental_data = self._load_property_data(rental_data_path or "propbot/data/processed/rental_properties.json")
        self.sales_data = self._load_property_data(sales_data_path or "propbot/data/processed/sales_properties.json")
        
        # Convert to DataFrame for easier analysis
        self.rental_df = self._convert_to_dataframe(self.rental_data)
        self.sales_df = self._convert_to_dataframe(self.sales_data)
        
        # Set output directory
        self.output_dir = output_dir or "propbot/data/analysis/locations"
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized LocationAnalyzer with {len(self.rental_data)} rental and {len(self.sales_data)} sales properties")
    
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
        
        # Add price_per_sqm column
        if 'price' in df.columns and 'size' in df.columns:
            df['price_per_sqm'] = df.apply(
                lambda row: row['price'] / row['size'] if row['price'] and row['size'] and row['size'] > 0 else None, 
                axis=1
            )
        
        return df
    
    def get_unique_neighborhoods(self) -> List[str]:
        """
        Get a list of unique neighborhoods in the data.
        
        Returns:
            List of unique neighborhood names
        """
        rental_neighborhoods = set(self.rental_df['neighborhood'].dropna().unique())
        sales_neighborhoods = set(self.sales_df['neighborhood'].dropna().unique())
        
        all_neighborhoods = rental_neighborhoods.union(sales_neighborhoods)
        
        # Filter out None, empty strings, and unknown values
        filtered_neighborhoods = [n for n in all_neighborhoods if n and n.lower() != 'unknown']
        
        return sorted(filtered_neighborhoods)
    
    def get_neighborhood_stats(self, neighborhood: str, property_type: str = 'sales') -> Dict:
        """
        Get statistics for a specific neighborhood.
        
        Args:
            neighborhood: Name of the neighborhood
            property_type: Type of properties to analyze ('sales' or 'rental')
            
        Returns:
            Dictionary with statistics about the neighborhood
        """
        # Select the appropriate DataFrame
        df = self.sales_df if property_type == 'sales' else self.rental_df
        
        # Filter for the specified neighborhood
        neighborhood_df = df[df['neighborhood'] == neighborhood]
        
        if neighborhood_df.empty:
            logger.warning(f"No {property_type} data found for neighborhood: {neighborhood}")
            return {
                "neighborhood": neighborhood,
                "property_count": 0,
                "average_price": None,
                "median_price": None,
                "price_range": None,
                "average_price_per_sqm": None,
                "median_price_per_sqm": None,
                "average_size": None,
                "median_size": None,
                "size_range": None,
                "property_types": []
            }
        
        # Calculate statistics
        price_stats = neighborhood_df['price'].describe()
        price_per_sqm_stats = neighborhood_df['price_per_sqm'].describe()
        size_stats = neighborhood_df['size'].describe()
        
        # Get property types distribution
        property_types = neighborhood_df['property_type'].value_counts().to_dict()
        
        # Prepare result
        stats = {
            "neighborhood": neighborhood,
            "property_count": len(neighborhood_df),
            "average_price": round(price_stats['mean']) if 'mean' in price_stats else None,
            "median_price": round(price_stats['50%']) if '50%' in price_stats else None,
            "price_range": {
                "min": round(price_stats['min']) if 'min' in price_stats else None,
                "max": round(price_stats['max']) if 'max' in price_stats else None
            },
            "average_price_per_sqm": round(price_per_sqm_stats['mean'], 2) if 'mean' in price_per_sqm_stats else None,
            "median_price_per_sqm": round(price_per_sqm_stats['50%'], 2) if '50%' in price_per_sqm_stats else None,
            "average_size": round(size_stats['mean'], 1) if 'mean' in size_stats else None,
            "median_size": round(size_stats['50%'], 1) if '50%' in size_stats else None,
            "size_range": {
                "min": round(size_stats['min'], 1) if 'min' in size_stats else None,
                "max": round(size_stats['max'], 1) if 'max' in size_stats else None
            },
            "property_types": property_types
        }
        
        logger.info(f"Generated {property_type} statistics for {neighborhood} with {stats['property_count']} properties")
        
        return stats
    
    def calculate_rental_yield_by_neighborhood(self, neighborhood: str) -> Dict:
        """
        Calculate average rental yield for a neighborhood.
        
        Args:
            neighborhood: Name of the neighborhood
            
        Returns:
            Dictionary with rental yield statistics
        """
        # Filter DataFrames for the specified neighborhood
        sales_df = self.sales_df[self.sales_df['neighborhood'] == neighborhood]
        rental_df = self.rental_df[self.rental_df['neighborhood'] == neighborhood]
        
        if sales_df.empty or rental_df.empty:
            logger.warning(f"Insufficient data for rental yield calculation in {neighborhood}")
            return {
                "neighborhood": neighborhood,
                "average_gross_yield_percent": None,
                "median_gross_yield_percent": None,
                "data_points": 0,
                "confidence": "low"
            }
        
        # Calculate average price per sqm for both sales and rentals
        if 'price_per_sqm' in sales_df.columns and not sales_df['price_per_sqm'].dropna().empty:
            avg_sales_price_per_sqm = sales_df['price_per_sqm'].dropna().median()
        else:
            avg_sales_price_per_sqm = sales_df['price'].dropna().median() / sales_df['size'].dropna().median()
        
        if 'price_per_sqm' in rental_df.columns and not rental_df['price_per_sqm'].dropna().empty:
            avg_monthly_rental_per_sqm = rental_df['price_per_sqm'].dropna().median()
        else:
            avg_monthly_rental_per_sqm = rental_df['price'].dropna().median() / rental_df['size'].dropna().median()
        
        # Calculate annual rental income per sqm
        annual_rental_per_sqm = avg_monthly_rental_per_sqm * 12
        
        # Calculate gross rental yield
        gross_yield_percent = (annual_rental_per_sqm / avg_sales_price_per_sqm) * 100
        
        # Calculate yields for each comparable property type and size
        yields = []
        
        # Group properties by size range
        size_ranges = [(0, 50), (50, 80), (80, 120), (120, 200), (200, float('inf'))]
        
        for min_size, max_size in size_ranges:
            sales_in_range = sales_df[(sales_df['size'] >= min_size) & (sales_df['size'] < max_size)]
            rentals_in_range = rental_df[(rental_df['size'] >= min_size) & (rental_df['size'] < max_size)]
            
            if not sales_in_range.empty and not rentals_in_range.empty:
                median_sales_price = sales_in_range['price'].median()
                median_monthly_rental = rentals_in_range['price'].median()
                
                if median_sales_price and median_monthly_rental:
                    annual_rental = median_monthly_rental * 12
                    range_yield_percent = (annual_rental / median_sales_price) * 100
                    
                    yields.append({
                        "size_range": f"{min_size}-{max_size}m²",
                        "median_sales_price": round(median_sales_price),
                        "median_monthly_rental": round(median_monthly_rental),
                        "yield_percent": round(range_yield_percent, 2)
                    })
        
        # Determine confidence level
        confidence = "low"
        if len(sales_df) >= 10 and len(rental_df) >= 10:
            confidence = "high"
        elif len(sales_df) >= 5 and len(rental_df) >= 5:
            confidence = "medium"
        
        # Prepare result
        result = {
            "neighborhood": neighborhood,
            "average_gross_yield_percent": round(gross_yield_percent, 2),
            "sales_price_per_sqm": round(avg_sales_price_per_sqm, 2),
            "annual_rental_per_sqm": round(annual_rental_per_sqm, 2),
            "size_range_yields": yields,
            "sales_data_points": len(sales_df),
            "rental_data_points": len(rental_df),
            "confidence": confidence
        }
        
        logger.info(f"Calculated rental yield for {neighborhood}: {result['average_gross_yield_percent']}% "
                   f"(confidence: {confidence})")
        
        return result
    
    def get_price_trends_by_neighborhood(self, neighborhood: str, property_type: str = 'sales') -> Dict:
        """
        Analyze price trends for a neighborhood.
        
        Args:
            neighborhood: Name of the neighborhood
            property_type: Type of properties to analyze ('sales' or 'rental')
            
        Returns:
            Dictionary with price trend analysis
        """
        # Select the appropriate DataFrame
        df = self.sales_df if property_type == 'sales' else self.rental_df
        
        # Filter for the specified neighborhood
        neighborhood_df = df[df['neighborhood'] == neighborhood]
        
        if neighborhood_df.empty:
            logger.warning(f"No {property_type} data found for neighborhood: {neighborhood}")
            return {
                "neighborhood": neighborhood,
                "has_date_data": False,
                "price_trend": None
            }
        
        # Check if 'date_added' column exists and has enough data
        if 'date_added' not in neighborhood_df.columns or neighborhood_df['date_added'].isna().sum() > 0.5 * len(neighborhood_df):
            logger.warning(f"Insufficient date data for {neighborhood} {property_type} trend analysis")
            return {
                "neighborhood": neighborhood,
                "has_date_data": False,
                "price_trend": None
            }
        
        try:
            # Convert date_added to datetime
            neighborhood_df['date_added'] = pd.to_datetime(neighborhood_df['date_added'])
            
            # Group by month and calculate average price and price per sqm
            monthly_data = neighborhood_df.groupby(pd.Grouper(key='date_added', freq='M')).agg({
                'price': 'mean',
                'price_per_sqm': 'mean',
                'size': 'mean'
            }).reset_index()
            
            # Exclude months with few data points
            monthly_data = monthly_data[monthly_data['price'].notna()]
            
            if len(monthly_data) <= 1:
                logger.warning(f"Not enough monthly data points for {neighborhood} {property_type} trend analysis")
                return {
                    "neighborhood": neighborhood,
                    "has_date_data": True,
                    "enough_data_points": False,
                    "price_trend": None
                }
            
            # Calculate month-over-month change
            monthly_data['price_change_pct'] = monthly_data['price'].pct_change() * 100
            monthly_data['price_per_sqm_change_pct'] = monthly_data['price_per_sqm'].pct_change() * 100
            
            # Calculate average monthly change
            avg_monthly_price_change = monthly_data['price_change_pct'].mean()
            avg_monthly_price_per_sqm_change = monthly_data['price_per_sqm_change_pct'].mean()
            
            # Calculate projected annual change
            annual_price_change = ((1 + avg_monthly_price_change / 100) ** 12 - 1) * 100
            annual_price_per_sqm_change = ((1 + avg_monthly_price_per_sqm_change / 100) ** 12 - 1) * 100
            
            # Prepare result
            trend = {
                "neighborhood": neighborhood,
                "has_date_data": True,
                "enough_data_points": True,
                "months_analyzed": len(monthly_data),
                "price_trend": {
                    "avg_monthly_change_percent": round(avg_monthly_price_change, 2),
                    "projected_annual_change_percent": round(annual_price_change, 2),
                    "latest_avg_price": round(monthly_data['price'].iloc[-1]),
                    "oldest_avg_price": round(monthly_data['price'].iloc[0]),
                    "total_change_percent": round((monthly_data['price'].iloc[-1] / monthly_data['price'].iloc[0] - 1) * 100, 2)
                },
                "price_per_sqm_trend": {
                    "avg_monthly_change_percent": round(avg_monthly_price_per_sqm_change, 2),
                    "projected_annual_change_percent": round(annual_price_per_sqm_change, 2),
                    "latest_avg_price_per_sqm": round(monthly_data['price_per_sqm'].iloc[-1], 2),
                    "oldest_avg_price_per_sqm": round(monthly_data['price_per_sqm'].iloc[0], 2),
                    "total_change_percent": round((monthly_data['price_per_sqm'].iloc[-1] / monthly_data['price_per_sqm'].iloc[0] - 1) * 100, 2)
                },
                "monthly_data": monthly_data[['date_added', 'price', 'price_per_sqm', 'price_change_pct', 'price_per_sqm_change_pct']].to_dict('records')
            }
            
            logger.info(f"Analyzed {property_type} price trends for {neighborhood}: "
                       f"{trend['price_trend']['projected_annual_change_percent']}% projected annual change")
            
            return trend
            
        except Exception as e:
            logger.error(f"Error analyzing price trends for {neighborhood}: {e}")
            return {
                "neighborhood": neighborhood,
                "has_date_data": True,
                "error": str(e)
            }
    
    def generate_neighborhood_comparison(self) -> Dict:
        """
        Generate a comparison of all neighborhoods.
        
        Returns:
            Dictionary with neighborhood comparison data
        """
        neighborhoods = self.get_unique_neighborhoods()
        
        comparison = []
        
        for neighborhood in neighborhoods:
            try:
                # Get basic stats
                sales_stats = self.get_neighborhood_stats(neighborhood, 'sales')
                rental_stats = self.get_neighborhood_stats(neighborhood, 'rental')
                
                # Get rental yield
                yield_stats = self.calculate_rental_yield_by_neighborhood(neighborhood)
                
                # Combine data
                neighborhood_data = {
                    "neighborhood": neighborhood,
                    "sales_count": sales_stats['property_count'],
                    "rental_count": rental_stats['property_count'],
                    "median_sales_price": sales_stats['median_price'],
                    "median_rental_price": rental_stats['median_price'],
                    "median_price_per_sqm": sales_stats['median_price_per_sqm'],
                    "median_size": sales_stats['median_size'],
                    "gross_yield_percent": yield_stats['average_gross_yield_percent'],
                    "yield_confidence": yield_stats['confidence']
                }
                
                comparison.append(neighborhood_data)
            except Exception as e:
                logger.error(f"Error processing neighborhood {neighborhood}: {e}")
        
        # Sort by yield (descending)
        comparison = sorted(
            comparison, 
            key=lambda x: (x['gross_yield_percent'] if x['gross_yield_percent'] else 0), 
            reverse=True
        )
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "total_neighborhoods": len(comparison),
            "neighborhoods": comparison
        }
        
        logger.info(f"Generated comparison for {len(comparison)} neighborhoods")
        
        return result
    
    def generate_neighborhood_report(self, neighborhood: str) -> Dict:
        """
        Generate a comprehensive report for a specific neighborhood.
        
        Args:
            neighborhood: Name of the neighborhood
            
        Returns:
            Dictionary with comprehensive neighborhood report
        """
        report = {
            "neighborhood": neighborhood,
            "report_date": datetime.now().isoformat(),
            "sales": self.get_neighborhood_stats(neighborhood, 'sales'),
            "rentals": self.get_neighborhood_stats(neighborhood, 'rental'),
            "rental_yield": self.calculate_rental_yield_by_neighborhood(neighborhood),
            "sales_trends": self.get_price_trends_by_neighborhood(neighborhood, 'sales'),
            "rental_trends": self.get_price_trends_by_neighborhood(neighborhood, 'rental')
        }
        
        # Add report summary
        summary = {
            "property_count": {
                "sales": report['sales']['property_count'],
                "rentals": report['rentals']['property_count']
            },
            "price_metrics": {
                "median_sales_price": report['sales']['median_price'],
                "median_rental_price": report['rentals']['median_price'],
                "median_sales_price_per_sqm": report['sales']['median_price_per_sqm'],
                "median_rental_price_per_sqm": report['rentals']['median_price_per_sqm']
            },
            "investment_metrics": {
                "gross_yield_percent": report['rental_yield']['average_gross_yield_percent'],
                "yield_confidence": report['rental_yield']['confidence']
            },
            "trend_summary": {
                "sales_annual_change_percent": report['sales_trends'].get('price_trend', {}).get('projected_annual_change_percent') 
                    if report['sales_trends'].get('enough_data_points', False) else None,
                "rental_annual_change_percent": report['rental_trends'].get('price_trend', {}).get('projected_annual_change_percent')
                    if report['rental_trends'].get('enough_data_points', False) else None
            }
        }
        
        report['summary'] = summary
        
        # Save report to file
        report_file = f"{self.output_dir}/{neighborhood.lower().replace(' ', '_')}_report.json"
        
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved neighborhood report to {report_file}")
        except Exception as e:
            logger.error(f"Error saving neighborhood report: {e}")
        
        return report
    
    def save_location_comparison_chart(self, 
                                     metric: str = 'gross_yield_percent', 
                                     top_n: int = 15,
                                     chart_type: str = 'bar') -> str:
        """
        Generate and save a chart comparing neighborhoods by a specific metric.
        
        Args:
            metric: Metric to compare ('gross_yield_percent', 'median_price_per_sqm', etc.)
            top_n: Number of top neighborhoods to include
            chart_type: Type of chart ('bar' or 'scatter')
            
        Returns:
            Path to the saved chart file
        """
        # Get comparison data
        comparison = self.generate_neighborhood_comparison()
        
        # Filter neighborhoods with the specified metric
        filtered_data = [n for n in comparison['neighborhoods'] if n.get(metric) is not None]
        
        # Sort by the specified metric (descending)
        sorted_data = sorted(filtered_data, key=lambda x: x.get(metric, 0), reverse=True)
        
        # Take top N
        top_data = sorted_data[:top_n]
        
        if not top_data:
            logger.warning(f"No data available for metric: {metric}")
            return None
        
        try:
            # Create figure
            plt.figure(figsize=(12, 8))
            
            # Get neighborhood names and metric values
            neighborhoods = [d['neighborhood'] for d in top_data]
            values = [d.get(metric, 0) for d in top_data]
            
            # Create appropriate chart
            if chart_type == 'bar':
                plt.barh(neighborhoods, values, color='skyblue')
                plt.xlabel(metric.replace('_', ' ').title())
                plt.title(f"Top {top_n} Neighborhoods by {metric.replace('_', ' ').title()}")
                
                # Add values on bars
                for i, v in enumerate(values):
                    plt.text(v, i, f" {v:.2f}" if isinstance(v, float) else f" {v}", va='center')
                
            elif chart_type == 'scatter':
                # For scatter plot, use two metrics (e.g., yield vs price)
                second_metric = 'median_price_per_sqm' if metric != 'median_price_per_sqm' else 'gross_yield_percent'
                second_values = [d.get(second_metric, 0) for d in top_data]
                
                plt.scatter(second_values, values)
                
                # Add neighborhood labels
                for i, neighborhood in enumerate(neighborhoods):
                    plt.annotate(neighborhood, (second_values[i], values[i]))
                
                plt.xlabel(second_metric.replace('_', ' ').title())
                plt.ylabel(metric.replace('_', ' ').title())
                plt.title(f"{metric.replace('_', ' ').title()} vs {second_metric.replace('_', ' ').title()}")
            
            # Save chart
            metric_name = metric.replace('_', '-')
            chart_file = f"{self.output_dir}/top_{top_n}_{metric_name}_{chart_type}_chart.png"
            plt.tight_layout()
            plt.savefig(chart_file)
            plt.close()
            
            logger.info(f"Saved {chart_type} chart to {chart_file}")
            
            return chart_file
            
        except Exception as e:
            logger.error(f"Error creating chart: {e}")
            return None
    
    def batch_generate_neighborhood_reports(self) -> Dict:
        """
        Generate reports for all neighborhoods.
        
        Returns:
            Dictionary with batch processing statistics
        """
        neighborhoods = self.get_unique_neighborhoods()
        
        success_count = 0
        error_count = 0
        reports = []
        
        for neighborhood in neighborhoods:
            try:
                logger.info(f"Generating report for {neighborhood}")
                report = self.generate_neighborhood_report(neighborhood)
                reports.append({
                    "neighborhood": neighborhood,
                    "success": True,
                    "report_file": f"{self.output_dir}/{neighborhood.lower().replace(' ', '_')}_report.json"
                })
                success_count += 1
            except Exception as e:
                logger.error(f"Error generating report for {neighborhood}: {e}")
                reports.append({
                    "neighborhood": neighborhood,
                    "success": False,
                    "error": str(e)
                })
                error_count += 1
        
        # Generate comparison chart
        yield_chart = self.save_location_comparison_chart('gross_yield_percent', 15, 'bar')
        price_chart = self.save_location_comparison_chart('median_price_per_sqm', 15, 'bar')
        
        # Save comparison data
        comparison = self.generate_neighborhood_comparison()
        comparison_file = f"{self.output_dir}/neighborhood_comparison.json"
        
        try:
            with open(comparison_file, 'w', encoding='utf-8') as f:
                json.dump(comparison, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved neighborhood comparison to {comparison_file}")
        except Exception as e:
            logger.error(f"Error saving neighborhood comparison: {e}")
        
        # Generate statistics
        stats = {
            "total_neighborhoods": len(neighborhoods),
            "successful_reports": success_count,
            "failed_reports": error_count,
            "comparison_file": comparison_file,
            "charts": {
                "yield_chart": yield_chart,
                "price_chart": price_chart
            },
            "reports": reports
        }
        
        logger.info(f"Generated reports for {success_count} neighborhoods, {error_count} failures")
        
        return stats


def analyze_neighborhoods(rental_data_path: str = None, 
                         sales_data_path: str = None,
                         output_dir: str = None,
                         neighborhood: str = None) -> Dict:
    """
    Analyze neighborhoods from property data files.
    
    Args:
        rental_data_path: Path to rental property data JSON file
        sales_data_path: Path to sales property data JSON file
        output_dir: Directory to save output files
        neighborhood: Specific neighborhood to analyze (optional)
        
    Returns:
        Dictionary with analysis statistics
    """
    try:
        # Initialize analyzer
        analyzer = LocationAnalyzer(rental_data_path, sales_data_path, output_dir)
        
        # Analyze specific neighborhood or all neighborhoods
        if neighborhood:
            report = analyzer.generate_neighborhood_report(neighborhood)
            return {
                "success": True,
                "neighborhood": neighborhood,
                "report_file": f"{analyzer.output_dir}/{neighborhood.lower().replace(' ', '_')}_report.json"
            }
        else:
            stats = analyzer.batch_generate_neighborhood_reports()
            return stats
            
    except Exception as e:
        logger.error(f"Error in neighborhood analysis: {e}")
        return {
            "success": False,
            "error": str(e)
        }


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/analysis/location_analyzer.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze property data by neighborhood')
    parser.add_argument('--rental_data', help='Path to rental property data JSON file')
    parser.add_argument('--sales_data', help='Path to sales property data JSON file')
    parser.add_argument('--output_dir', help='Directory to save output files')
    parser.add_argument('--neighborhood', help='Specific neighborhood to analyze (optional)')
    
    args = parser.parse_args()
    
    result = analyze_neighborhoods(args.rental_data, args.sales_data, args.output_dir, args.neighborhood)
    print(json.dumps(result, indent=2)) 