#!/usr/bin/env python3
"""
Database module that provides a unified interface for database operations
"""

from .database_utils import (
    get_connection,
    get_database_url,
    initialize_database,
    get_rental_last_update,
    set_rental_last_update,
    get_rental_update_frequency,
    set_rental_update_frequency
)

__all__ = [
    'get_connection',
    'get_database_url',
    'initialize_database',
    'get_rental_last_update',
    'set_rental_last_update',
    'get_rental_update_frequency',
    'set_rental_update_frequency'
] 