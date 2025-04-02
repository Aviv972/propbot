"""
Metrics package for PropBot rental analysis.

This package contains modules for analyzing rental data, calculating yields,
and generating investment metrics for properties.
"""

from .db_functions import (
    get_rental_listings_from_database,
    get_sales_listings_from_database,
    get_rental_last_update,
    set_rental_last_update,
    get_sales_last_update,
    set_sales_last_update
)

from .rental_metrics import (
    load_complete_rental_data,
    filter_valid_rentals,
    calculate_rental_metrics,
    update_rental_metrics
)

from .rental_analysis import (
    analyze_rental_yields,
    analyze_size_metrics,
    save_analysis_results
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
    'get_rental_listings_from_database',
    'get_sales_listings_from_database',
    'get_rental_last_update',
    'set_rental_last_update',
    'get_sales_last_update',
    'set_sales_last_update',
    'load_complete_rental_data',
    'filter_valid_rentals',
    'calculate_rental_metrics',
    'update_rental_metrics',
    'analyze_rental_yields',
    'analyze_size_metrics',
    'save_analysis_results',
    
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