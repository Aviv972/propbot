"""
Metrics package for PropBot rental analysis.

This package contains modules for analyzing rental data, calculating yields,
and generating investment metrics for properties.
"""

from .rental_metrics import (
    load_complete_rental_data,
    standardize_location,
    extract_neighborhoods,
    extract_parish,
    extract_neighborhood,
    find_comparable_properties,
    calculate_average_rent,
    generate_income_report,
    run_improved_analysis
)

from .rental_analysis import (
    log_message,
    load_sales_data,
    extract_size,
    extract_room_type,
    save_report_to_json,
    save_report_to_csv
)

# New imports from investment_metrics
from .investment_metrics import (
    calculate_noi,
    calculate_cap_rate,
    calculate_gross_yield,
    calculate_cash_on_cash_return,
    calculate_monthly_cash_flow,
    calculate_price_per_sqm,
    calculate_all_investment_metrics,
    find_best_properties,
    generate_best_properties_report
)

# New imports from segmentation
from .segmentation import (
    calculate_location_similarity,
    calculate_price_difference,
    classify_property,
    generate_complete_property_analysis,
    load_neighborhood_data,
    calculate_neighborhood_avg_from_data
)

__all__ = [
    # Rental metrics
    'load_complete_rental_data',
    'standardize_location',
    'extract_neighborhoods',
    'extract_parish',
    'extract_neighborhood',
    'find_comparable_properties',
    'calculate_average_rent',
    'generate_income_report',
    'run_improved_analysis',
    
    # Rental analysis
    'log_message',
    'load_sales_data',
    'extract_size',
    'extract_room_type',
    'save_report_to_json',
    'save_report_to_csv',
    
    # Investment metrics
    'calculate_noi',
    'calculate_cap_rate',
    'calculate_gross_yield',
    'calculate_cash_on_cash_return',
    'calculate_monthly_cash_flow',
    'calculate_price_per_sqm',
    'calculate_all_investment_metrics',
    'find_best_properties',
    'generate_best_properties_report',
    
    # Segmentation and classification
    'calculate_location_similarity',
    'calculate_price_difference',
    'classify_property',
    'generate_complete_property_analysis',
    'load_neighborhood_data',
    'calculate_neighborhood_avg_from_data'
] 