"""
PropBot data consolidation module.

Provides functions for consolidating property data from multiple sources.
"""

from .sales import consolidate_sales
from .rentals import consolidate_rentals

__all__ = ['consolidate_sales', 'consolidate_rentals']
