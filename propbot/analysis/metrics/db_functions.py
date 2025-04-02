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

def get_rental_listings_from_database(max_price_per_sqm: float = 45) -> List[Dict[str, Any]]:
    """Query rental listings from the database.
    
    Args:
        max_price_per_sqm: Maximum acceptable price per square meter
        
    Returns:
        List of rental properties in standard format
    """
    logging.info("Querying rental listings from database...")
    
    filtered_rentals = []
    outliers = []
    
    conn = get_connection()
    if not conn:
        logging.error("Could not connect to database to fetch rental listings")
        return []
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Query the database for rental properties
                cur.execute("""
                    SELECT 
                        url, price, size, rooms, location, 
                        price_per_sqm, details, title,
                        neighborhood, is_furnished
                    FROM 
                        properties_rentals 
                    WHERE 
                        price > 0 AND size > 0
                """)
                
                rows = cur.fetchall()
                logging.info(f"Retrieved {len(rows)} rental listings from database")
                
                # Process each result
                for row in rows:
                    rental_property = {
                        'url': row[0],
                        'price': float(row[1]) if row[1] is not None else 0,
                        'size': float(row[2]) if row[2] is not None else 0,
                        'rooms': row[3],
                        'location': row[4],
                        'price_per_sqm': float(row[5]) if row[5] is not None else 0,
                        'details': row[6],
                        'title': row[7],
                        'neighborhood': row[8],
                        'is_furnished': row[9]
                    }
                    
                    # Calculate price per sqm if missing
                    if rental_property['price_per_sqm'] == 0 and rental_property['size'] > 0:
                        rental_property['price_per_sqm'] = rental_property['price'] / rental_property['size']
                    
                    # Skip properties with excessively high price per sqm (likely data errors)
                    if rental_property['price_per_sqm'] > max_price_per_sqm:
                        outliers.append(rental_property)
                        continue
                    
                    filtered_rentals.append(rental_property)
                
                logging.info(f"Filtered to {len(filtered_rentals)} valid rental listings")
                if outliers:
                    logging.info(f"Excluded {len(outliers)} outliers with price_per_sqm > {max_price_per_sqm}")
        
        return filtered_rentals
        
    except Exception as e:
        logging.error(f"Error querying rental listings from database: {str(e)}")
        return []
    finally:
        conn.close()

def get_sales_listings_from_database(max_price_per_sqm: float = 8000) -> List[Dict[str, Any]]:
    """Query sales listings from the database.
    
    Args:
        max_price_per_sqm: Maximum acceptable price per square meter
        
    Returns:
        List of sales properties in standard format
    """
    logging.info("Querying sales listings from database...")
    
    filtered_sales = []
    outliers = []
    
    conn = get_connection()
    if not conn:
        logging.error("Could not connect to database to fetch sales listings")
        return []
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Query the database for sales properties
                cur.execute("""
                    SELECT 
                        url, price, size, rooms, location, 
                        price_per_sqm, details, title,
                        neighborhood, snapshot_date, first_seen_date
                    FROM 
                        properties_sales 
                    WHERE 
                        price > 0 AND size > 0
                """)
                
                rows = cur.fetchall()
                logging.info(f"Retrieved {len(rows)} sales listings from database")
                
                # Process each result
                for row in rows:
                    sales_property = {
                        'url': row[0],
                        'price': float(row[1]) if row[1] is not None else 0,
                        'size': float(row[2]) if row[2] is not None else 0,
                        'room_type': row[3],  # rooms column
                        'location': row[4],
                        'price_per_sqm': float(row[5]) if row[5] is not None else 0,
                        'details': row[6],
                        'title': row[7],
                        'neighborhood': row[8],
                        'snapshot_date': row[9],
                        'first_seen_date': row[10]
                    }
                    
                    # Calculate price per sqm if missing
                    if sales_property['price_per_sqm'] == 0 and sales_property['size'] > 0:
                        sales_property['price_per_sqm'] = sales_property['price'] / sales_property['size']
                    
                    # Skip properties with excessively high price per sqm (likely data errors)
                    if sales_property['price_per_sqm'] > max_price_per_sqm:
                        outliers.append(sales_property)
                        continue
                    
                    filtered_sales.append(sales_property)
                
                logging.info(f"Filtered to {len(filtered_sales)} valid sales listings")
                if outliers:
                    logging.info(f"Excluded {len(outliers)} outliers with price_per_sqm > {max_price_per_sqm}")
        
        return filtered_sales
        
    except Exception as e:
        logging.error(f"Error querying sales listings from database: {str(e)}")
        return []
    finally:
        conn.close()

def get_rental_last_update() -> Optional[datetime]:
    """Get the last update timestamp for rental data."""
    conn = get_connection()
    if not conn:
        return None
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MAX(snapshot_date)
                    FROM properties_rentals
                """)
                result = cur.fetchone()
                return result[0] if result and result[0] else None
    except Exception as e:
        logger.error(f"Error getting rental last update: {str(e)}")
        return None
    finally:
        conn.close()

def set_rental_last_update() -> bool:
    """Set the last update timestamp for rental data to now."""
    conn = get_connection()
    if not conn:
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE properties_rentals
                    SET snapshot_date = NOW()
                    WHERE snapshot_date IS NULL
                """)
                return True
    except Exception as e:
        logger.error(f"Error setting rental last update: {str(e)}")
        return False
    finally:
        conn.close()

def get_rental_update_frequency() -> int:
    """Get the rental data update frequency in days."""
    return 30  # Default to 30 days 