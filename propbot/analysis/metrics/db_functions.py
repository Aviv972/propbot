#!/usr/bin/env python3
"""
Database utility functions for analysis modules
"""

import logging
import traceback
import psycopg2
import os
from typing import List, Dict, Any, Optional
from datetime import datetime

import pandas as pd

from propbot.database_utils import get_connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_connection():
    """Get a connection to the database."""
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        return None
    
    # Add SSL mode if not present
    if "sslmode=" not in database_url:
        database_url += "?sslmode=require"
    
    try:
        conn = psycopg2.connect(database_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def get_rental_listings_from_database() -> List[Dict]:
    """Get all rental listings from the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return []
            
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    id, url, title, price, size, rooms, 
                    price_per_sqm, location, neighborhood,
                    details, snapshot_date, first_seen_date,
                    created_at, updated_at
                FROM properties_rentals
                ORDER BY snapshot_date DESC
            """)
            listings = [dict(row) for row in cur.fetchall()]
            logger.info(f"Retrieved {len(listings)} rental listings from database")
            return listings
    except Exception as e:
        logger.error(f"Error getting rental listings from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_sales_listings_from_database() -> List[Dict]:
    """Get all sales listings from the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return []
            
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    id, url, title, price, size, rooms,
                    price_per_sqm, location, neighborhood,
                    details, snapshot_date, first_seen_date,
                    created_at, updated_at
                FROM properties_sales
                ORDER BY snapshot_date DESC
            """)
            listings = [dict(row) for row in cur.fetchall()]
            logger.info(f"Retrieved {len(listings)} sales listings from database")
            return listings
    except Exception as e:
        logger.error(f"Error getting sales listings from database: {e}")
        return []
    finally:
        if conn:
            conn.close()

def get_rental_last_update() -> Optional[datetime]:
    """Get the last update timestamp for rental data."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return None
            
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(snapshot_date) as last_update
                FROM properties_rentals
            """)
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting rental last update: {e}")
        return None
    finally:
        if conn:
            conn.close()

def set_rental_last_update(timestamp: datetime) -> bool:
    """Set the last update timestamp for rental data."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE properties_rentals
                SET snapshot_date = %s
                WHERE snapshot_date IS NULL
            """, (timestamp,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error setting rental last update: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_sales_last_update() -> Optional[datetime]:
    """Get the last update timestamp for sales data."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return None
            
        with conn.cursor() as cur:
            cur.execute("""
                SELECT MAX(snapshot_date) as last_update
                FROM properties_sales
            """)
            result = cur.fetchone()
            return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting sales last update: {e}")
        return None
    finally:
        if conn:
            conn.close()

def set_sales_last_update(timestamp: datetime) -> bool:
    """Set the last update timestamp for sales data."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE properties_sales
                SET snapshot_date = %s
                WHERE snapshot_date IS NULL
            """, (timestamp,))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error setting sales last update: {e}")
        return False
    finally:
        if conn:
            conn.close()

def get_rental_update_frequency() -> int:
    """Get the rental data update frequency in days."""
    return 30  # Default to 30 days 