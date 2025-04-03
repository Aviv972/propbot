#!/usr/bin/env python3
"""
Database utility functions for analysis modules
"""

import logging
import traceback
import psycopg2
import psycopg2.extras
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

import pandas as pd

from propbot.database_utils import get_connection

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
            listings = []
            for row in cur.fetchall():
                listing = dict(row)
                # Convert Decimal values to float
                for key, value in listing.items():
                    if isinstance(value, Decimal):
                        listing[key] = float(value)
                listings.append(listing)
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

def get_salesdata_table_count() -> int:
    """Get the row count of properties_sales table."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return 0
            
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM properties_sales")
            count = cur.fetchone()[0]
            logger.info(f"Properties sales table contains {count} rows")
            return count
    except Exception as e:
        logger.error(f"Error getting sales table count: {e}")
        return 0
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def save_rental_estimate(url: str, neighborhood: str, size: float, rooms: int, 
                      estimated_monthly_rent: float, price_per_sqm: float, 
                      comparable_count: int, confidence: str) -> bool:
    """Save rental estimate to the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO rental_estimates
                (url, neighborhood, size, rooms, estimated_monthly_rent, price_per_sqm, 
                comparable_count, confidence, last_updated)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (url) 
                DO UPDATE SET 
                    neighborhood = EXCLUDED.neighborhood,
                    size = EXCLUDED.size,
                    rooms = EXCLUDED.rooms,
                    estimated_monthly_rent = EXCLUDED.estimated_monthly_rent,
                    price_per_sqm = EXCLUDED.price_per_sqm,
                    comparable_count = EXCLUDED.comparable_count,
                    confidence = EXCLUDED.confidence,
                    last_updated = NOW()
            """, (url, neighborhood, size, rooms, estimated_monthly_rent, 
                 price_per_sqm, comparable_count, confidence))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error saving rental estimate: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def save_multiple_rental_estimates(estimates: List[Dict]) -> bool:
    """Save multiple rental estimates to the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            for estimate in estimates:
                cur.execute("""
                    INSERT INTO rental_estimates
                    (url, neighborhood, size, rooms, estimated_monthly_rent, price_per_sqm, 
                    comparable_count, confidence, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (url) 
                    DO UPDATE SET 
                        neighborhood = EXCLUDED.neighborhood,
                        size = EXCLUDED.size,
                        rooms = EXCLUDED.rooms,
                        estimated_monthly_rent = EXCLUDED.estimated_monthly_rent,
                        price_per_sqm = EXCLUDED.price_per_sqm,
                        comparable_count = EXCLUDED.comparable_count,
                        confidence = EXCLUDED.confidence,
                        last_updated = NOW()
                """, (
                    estimate.get('url'),
                    estimate.get('neighborhood'),
                    estimate.get('size'),
                    estimate.get('rooms'),
                    estimate.get('estimated_monthly_rent'),
                    estimate.get('price_per_sqm'),
                    estimate.get('comparable_count'),
                    estimate.get('confidence')
                ))
            conn.commit()
            logger.info(f"Saved {len(estimates)} rental estimates to database")
            return True
    except Exception as e:
        logger.error(f"Error saving multiple rental estimates: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_rental_estimates() -> List[Dict]:
    """Get all rental estimates from the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return []
            
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    url, neighborhood, size, rooms, estimated_monthly_rent, 
                    price_per_sqm, comparable_count, confidence, last_updated
                FROM rental_estimates
                ORDER BY last_updated DESC
            """)
            estimates = []
            for row in cur.fetchall():
                estimate = dict(row)
                # Convert Decimal values to float
                for key, value in estimate.items():
                    if isinstance(value, Decimal):
                        estimate[key] = float(value)
                estimates.append(estimate)
            logger.info(f"Retrieved {len(estimates)} rental estimates from database")
            return estimates
    except Exception as e:
        logger.error(f"Error getting rental estimates from database: {e}")
        return []
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_rental_estimate_by_url(url: str) -> Optional[Dict]:
    """Get rental estimate for a specific property."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return None
            
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    url, neighborhood, size, rooms, estimated_monthly_rent, 
                    price_per_sqm, comparable_count, confidence, last_updated
                FROM rental_estimates
                WHERE url = %s
            """, (url,))
            row = cur.fetchone()
            if row:
                estimate = dict(row)
                # Convert Decimal values to float
                for key, value in estimate.items():
                    if isinstance(value, Decimal):
                        estimate[key] = float(value)
                return estimate
            return None
    except Exception as e:
        logger.error(f"Error getting rental estimate for {url}: {e}")
        return None
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def save_analyzed_property(property_data: Dict) -> bool:
    """Save analyzed property to the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            # First try to get the property_id from properties_sales
            property_id = None
            if 'url' in property_data:
                cur.execute("""
                    SELECT id FROM properties_sales
                    WHERE url = %s
                """, (property_data.get('url'),))
                row = cur.fetchone()
                if row:
                    property_id = row[0]
            
            # Insert or update the analyzed property
            cur.execute("""
                INSERT INTO analyzed_properties
                (property_id, url, title, price, size, rooms, neighborhood, 
                monthly_rent, price_per_sqm, rental_price_per_sqm, neighborhood_avg_rent,
                gross_yield, cap_rate, cash_on_cash, monthly_cash_flow, 
                comparable_count, analysis_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (url) 
                DO UPDATE SET 
                    property_id = EXCLUDED.property_id,
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    size = EXCLUDED.size,
                    rooms = EXCLUDED.rooms,
                    neighborhood = EXCLUDED.neighborhood,
                    monthly_rent = EXCLUDED.monthly_rent,
                    price_per_sqm = EXCLUDED.price_per_sqm,
                    rental_price_per_sqm = EXCLUDED.rental_price_per_sqm,
                    neighborhood_avg_rent = EXCLUDED.neighborhood_avg_rent,
                    gross_yield = EXCLUDED.gross_yield,
                    cap_rate = EXCLUDED.cap_rate,
                    cash_on_cash = EXCLUDED.cash_on_cash,
                    monthly_cash_flow = EXCLUDED.monthly_cash_flow,
                    comparable_count = EXCLUDED.comparable_count,
                    analysis_date = NOW()
            """, (
                property_id,
                property_data.get('url'),
                property_data.get('title'),
                property_data.get('price'),
                property_data.get('size'),
                property_data.get('rooms'),
                property_data.get('neighborhood'),
                property_data.get('monthly_rent'),
                property_data.get('price_per_sqm'),
                property_data.get('rental_price_per_sqm'),
                property_data.get('neighborhood_avg_rent'),
                property_data.get('gross_yield'),
                property_data.get('cap_rate'),
                property_data.get('cash_on_cash'),
                property_data.get('monthly_cash_flow'),
                property_data.get('comparable_count')
            ))
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Error saving analyzed property: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def save_multiple_analyzed_properties(properties: List[Dict]) -> bool:
    """Save multiple analyzed properties to the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return False
            
        with conn.cursor() as cur:
            # First get all property_ids from properties_sales for batch processing
            property_ids = {}
            urls = [p.get('url') for p in properties if 'url' in p]
            if urls:
                placeholders = ','.join(['%s'] * len(urls))
                cur.execute(f"""
                    SELECT id, url FROM properties_sales
                    WHERE url IN ({placeholders})
                """, urls)
                for row in cur.fetchall():
                    property_ids[row[1]] = row[0]
            
            # Insert or update all properties
            for prop in properties:
                property_id = property_ids.get(prop.get('url'))
                
                cur.execute("""
                    INSERT INTO analyzed_properties
                    (property_id, url, title, price, size, rooms, neighborhood, 
                    monthly_rent, price_per_sqm, rental_price_per_sqm, neighborhood_avg_rent,
                    gross_yield, cap_rate, cash_on_cash, monthly_cash_flow, 
                    comparable_count, analysis_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (url) 
                    DO UPDATE SET 
                        property_id = EXCLUDED.property_id,
                        title = EXCLUDED.title,
                        price = EXCLUDED.price,
                        size = EXCLUDED.size,
                        rooms = EXCLUDED.rooms,
                        neighborhood = EXCLUDED.neighborhood,
                        monthly_rent = EXCLUDED.monthly_rent,
                        price_per_sqm = EXCLUDED.price_per_sqm,
                        rental_price_per_sqm = EXCLUDED.rental_price_per_sqm,
                        neighborhood_avg_rent = EXCLUDED.neighborhood_avg_rent,
                        gross_yield = EXCLUDED.gross_yield,
                        cap_rate = EXCLUDED.cap_rate,
                        cash_on_cash = EXCLUDED.cash_on_cash,
                        monthly_cash_flow = EXCLUDED.monthly_cash_flow,
                        comparable_count = EXCLUDED.comparable_count,
                        analysis_date = NOW()
                """, (
                    property_id,
                    prop.get('url'),
                    prop.get('title'),
                    prop.get('price'),
                    prop.get('size'),
                    prop.get('rooms'),
                    prop.get('neighborhood'),
                    prop.get('monthly_rent'),
                    prop.get('price_per_sqm'),
                    prop.get('rental_price_per_sqm'),
                    prop.get('neighborhood_avg_rent'),
                    prop.get('gross_yield'),
                    prop.get('cap_rate'),
                    prop.get('cash_on_cash'),
                    prop.get('monthly_cash_flow'),
                    prop.get('comparable_count')
                ))
            
            conn.commit()
            logger.info(f"Saved {len(properties)} analyzed properties to database")
            return True
    except Exception as e:
        logger.error(f"Error saving multiple analyzed properties: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def get_analyzed_properties() -> List[Dict]:
    """Get all analyzed properties from the database."""
    try:
        conn = get_connection()
        if not conn:
            logger.error("Could not get connection to database")
            return []
            
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT 
                    property_id, url, title, price, size, rooms, neighborhood, 
                    monthly_rent, price_per_sqm, rental_price_per_sqm, neighborhood_avg_rent,
                    gross_yield, cap_rate, cash_on_cash, monthly_cash_flow, 
                    comparable_count, analysis_date
                FROM analyzed_properties
                ORDER BY analysis_date DESC
            """)
            properties = []
            for row in cur.fetchall():
                prop = dict(row)
                # Convert Decimal values to float
                for key, value in prop.items():
                    if isinstance(value, Decimal):
                        prop[key] = float(value)
                properties.append(prop)
            logger.info(f"Retrieved {len(properties)} analyzed properties from database")
            return properties
    except Exception as e:
        logger.error(f"Error getting analyzed properties from database: {e}")
        return []
    finally:
        if 'conn' in locals() and conn:
            conn.close() 