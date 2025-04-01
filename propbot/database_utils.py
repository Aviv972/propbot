#!/usr/bin/env python3
"""
Database utility functions for PostgreSQL integration
"""

import os
import logging
import psycopg2
from psycopg2 import extras
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_database_url():
    """Get the database URL from environment variables"""
    # Get DATABASE_URL from environment
    db_url = os.environ.get('DATABASE_URL')
    
    if not db_url:
        logger.warning("No database URL found in environment variables")
        return None
    
    # Add sslmode=require if not already present in the URL
    if 'sslmode=' not in db_url:
        db_url += ('&' if '?' in db_url else '?') + 'sslmode=require'
    
    return db_url

def get_connection():
    """Get a database connection"""
    db_url = get_database_url()
    if not db_url:
        logger.error("No database URL available")
        return None
    
    try:
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        return None

def initialize_database():
    """Initialize the database schema"""
    conn = get_connection()
    if not conn:
        logger.error("Could not initialize database - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Create metadata table if it doesn't exist
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS rental_metadata (
                        id SERIAL PRIMARY KEY,
                        metadata_key VARCHAR(255) UNIQUE NOT NULL,
                        string_value TEXT,
                        date_value TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
        logger.info("Database initialized successfully")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False
    finally:
        conn.close()

def get_rental_last_update():
    """Get the last update time for rental data"""
    conn = get_connection()
    if not conn:
        logger.error("Could not get rental last update - no connection")
        return None
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT date_value FROM rental_metadata 
                    WHERE metadata_key = 'rental_last_update'
                """)
                result = cur.fetchone()
                if result:
                    return result[0]
                return None
    except Exception as e:
        logger.error(f"Error getting rental last update: {str(e)}")
        return None
    finally:
        conn.close()

def set_rental_last_update(update_time=None):
    """Set the last update time for rental data"""
    if update_time is None:
        update_time = datetime.now()
    
    conn = get_connection()
    if not conn:
        logger.error("Could not set rental last update - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Use upsert (INSERT ... ON CONFLICT) to handle both insert and update cases
                cur.execute("""
                    INSERT INTO rental_metadata (metadata_key, date_value, updated_at)
                    VALUES ('rental_last_update', %s, NOW())
                    ON CONFLICT (metadata_key) 
                    DO UPDATE SET date_value = %s, updated_at = NOW()
                """, (update_time, update_time))
        logger.info(f"Set rental last update to {update_time}")
        return True
    except Exception as e:
        logger.error(f"Error setting rental last update: {str(e)}")
        return False
    finally:
        conn.close()

def get_rental_update_frequency():
    """Get the update frequency for rental data (in days)"""
    conn = get_connection()
    if not conn:
        logger.error("Could not get rental update frequency - no connection")
        return 30  # Default to 30 days
    
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT string_value FROM rental_metadata 
                    WHERE metadata_key = 'rental_update_frequency'
                """)
                result = cur.fetchone()
                if result and result[0]:
                    try:
                        return int(result[0])
                    except ValueError:
                        logger.warning(f"Invalid rental update frequency value: {result[0]}")
                        return 30
                return 30  # Default to 30 days
    except Exception as e:
        logger.error(f"Error getting rental update frequency: {str(e)}")
        return 30  # Default to 30 days
    finally:
        conn.close()

def set_rental_update_frequency(days=30):
    """Set the update frequency for rental data (in days)"""
    conn = get_connection()
    if not conn:
        logger.error("Could not set rental update frequency - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                # Use upsert (INSERT ... ON CONFLICT) to handle both insert and update cases
                cur.execute("""
                    INSERT INTO rental_metadata (metadata_key, string_value, updated_at)
                    VALUES ('rental_update_frequency', %s, NOW())
                    ON CONFLICT (metadata_key) 
                    DO UPDATE SET string_value = %s, updated_at = NOW()
                """, (str(days), str(days)))
        logger.info(f"Set rental update frequency to {days} days")
        return True
    except Exception as e:
        logger.error(f"Error setting rental update frequency: {str(e)}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    # If run directly, initialize the database
    initialize_database() 