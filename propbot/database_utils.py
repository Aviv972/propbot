#!/usr/bin/env python3
"""
Database utility functions for PostgreSQL integration
"""

import os
import logging
import psycopg2
from psycopg2 import extras
from datetime import datetime

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

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
    
    # Log the URL with sensitive information redacted
    redacted_url = db_url.split('@')[0] + '@[REDACTED]'
    logger.debug(f"Using database URL: {redacted_url}")
    return db_url

def get_connection():
    """Get a database connection"""
    db_url = get_database_url()
    if not db_url:
        logger.error("No database URL available")
        return None
    
    try:
        logger.debug("Attempting to connect to database...")
        conn = psycopg2.connect(db_url)
        logger.debug("Successfully connected to database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        import traceback
        logger.error(f"Connection error details: {traceback.format_exc()}")
        return None

def initialize_database():
    """Initialize the database schema"""
    logger.info("Starting database initialization")
    conn = get_connection()
    if not conn:
        logger.error("Could not initialize database - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                logger.debug("Creating/verifying metadata table...")
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
                logger.debug("Metadata table verified")
                
                logger.debug("Creating/verifying sales properties table...")
                # Create sales properties table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS properties_sales (
                        id SERIAL PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        title TEXT,
                        price NUMERIC,
                        size NUMERIC,
                        rooms INTEGER,
                        price_per_sqm NUMERIC,
                        location TEXT,
                        neighborhood TEXT,
                        details TEXT,
                        snapshot_date TIMESTAMP,
                        first_seen_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                logger.debug("Sales properties table verified")
                
                logger.debug("Creating/verifying rental properties table...")
                # Create rental properties table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS properties_rentals (
                        id SERIAL PRIMARY KEY,
                        url TEXT UNIQUE NOT NULL,
                        title TEXT,
                        price NUMERIC,
                        size NUMERIC,
                        rooms INTEGER,
                        price_per_sqm NUMERIC,
                        location TEXT,
                        neighborhood TEXT,
                        details TEXT,
                        is_furnished BOOLEAN,
                        snapshot_date TIMESTAMP,
                        first_seen_date TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                logger.debug("Rental properties table verified")
                
                logger.debug("Creating/verifying historical snapshots tables...")
                # Create historical snapshots tables
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sales_historical_snapshots (
                        id SERIAL PRIMARY KEY,
                        snapshot_date TIMESTAMP NOT NULL,
                        property_count INTEGER NOT NULL,
                        new_properties INTEGER NOT NULL,
                        updated_properties INTEGER NOT NULL,
                        snapshot_data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                logger.debug("Sales historical snapshots table verified")
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS rental_historical_snapshots (
                        id SERIAL PRIMARY KEY,
                        snapshot_date TIMESTAMP NOT NULL,
                        property_count INTEGER NOT NULL,
                        new_properties INTEGER NOT NULL,
                        updated_properties INTEGER NOT NULL,
                        snapshot_data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                logger.debug("Rental historical snapshots table verified")
                
                logger.debug("Creating/verifying analysis results table...")
                # Create analysis results historical table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS analysis_results_history (
                        id SERIAL PRIMARY KEY,
                        analysis_type VARCHAR(50) NOT NULL,
                        analysis_date TIMESTAMP NOT NULL,
                        property_count INTEGER NOT NULL,
                        result_data JSONB,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                logger.debug("Analysis results table verified")
                
                logger.debug("Creating/verifying indexes...")
                # Create indexes for better query performance
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_properties_sales_location 
                    ON properties_sales(location);
                    
                    CREATE INDEX IF NOT EXISTS idx_properties_rentals_location 
                    ON properties_rentals(location);
                    
                    CREATE INDEX IF NOT EXISTS idx_properties_sales_price 
                    ON properties_sales(price);
                    
                    CREATE INDEX IF NOT EXISTS idx_properties_rentals_price 
                    ON properties_rentals(price);
                    
                    CREATE INDEX IF NOT EXISTS idx_sales_historical_snapshots_date
                    ON sales_historical_snapshots(snapshot_date);
                    
                    CREATE INDEX IF NOT EXISTS idx_rental_historical_snapshots_date
                    ON rental_historical_snapshots(snapshot_date);
                    
                    CREATE INDEX IF NOT EXISTS idx_analysis_results_history_type_date
                    ON analysis_results_history(analysis_type, analysis_date);
                """)
                logger.debug("All indexes verified")
                
                logger.info("Database initialized successfully with all required tables")
        return True
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        import traceback
        logger.error(f"Initialization error details: {traceback.format_exc()}")
        return False
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

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

def save_historical_sales_snapshot(property_data, new_count=0, updated_count=0):
    """Save a historical snapshot of sales data to the database.
    
    Args:
        property_data: List of property dictionaries 
        new_count: Number of new properties in this snapshot
        updated_count: Number of updated properties in this snapshot
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_connection()
    if not conn:
        logger.error("Could not save historical snapshot - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                snapshot_date = datetime.now()
                property_count = len(property_data)
                
                # Convert to JSON string and then to psycopg2 Json object
                import json
                from psycopg2.extras import Json
                json_data = Json(property_data)
                
                cur.execute("""
                    INSERT INTO sales_historical_snapshots 
                    (snapshot_date, property_count, new_properties, updated_properties, snapshot_data)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (snapshot_date, property_count, new_count, updated_count, json_data))
                
                snapshot_id = cur.fetchone()[0]
                logger.info(f"Saved historical sales snapshot #{snapshot_id} with {property_count} properties")
                return True
    except Exception as e:
        logger.error(f"Error saving historical sales snapshot: {str(e)}")
        return False

def save_historical_rental_snapshot(property_data, new_count=0, updated_count=0):
    """Save a historical snapshot of rental data to the database.
    
    Args:
        property_data: List of property dictionaries
        new_count: Number of new properties in this snapshot
        updated_count: Number of updated properties in this snapshot
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_connection()
    if not conn:
        logger.error("Could not save historical snapshot - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                snapshot_date = datetime.now()
                property_count = len(property_data)
                
                # Convert to JSON string and then to psycopg2 Json object
                import json
                from psycopg2.extras import Json
                json_data = Json(property_data)
                
                cur.execute("""
                    INSERT INTO rental_historical_snapshots 
                    (snapshot_date, property_count, new_properties, updated_properties, snapshot_data)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                """, (snapshot_date, property_count, new_count, updated_count, json_data))
                
                snapshot_id = cur.fetchone()[0]
                logger.info(f"Saved historical rental snapshot #{snapshot_id} with {property_count} properties")
                return True
    except Exception as e:
        logger.error(f"Error saving historical rental snapshot: {str(e)}")
        return False

def save_analysis_results(analysis_type, result_data, property_count=0):
    """Save analysis results to the history table.
    
    Args:
        analysis_type: Type of analysis (e.g., 'investment', 'rental')
        result_data: Dictionary of analysis results
        property_count: Number of properties analyzed
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_connection()
    if not conn:
        logger.error("Could not save analysis results - no connection")
        return False
    
    try:
        with conn:
            with conn.cursor() as cur:
                analysis_date = datetime.now()
                
                # Convert to JSON string and then to psycopg2 Json object
                import json
                from psycopg2.extras import Json
                json_data = Json(result_data)
                
                cur.execute("""
                    INSERT INTO analysis_results_history 
                    (analysis_type, analysis_date, property_count, result_data)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id
                """, (analysis_type, analysis_date, property_count, json_data))
                
                result_id = cur.fetchone()[0]
                logger.info(f"Saved {analysis_type} analysis results #{result_id} for {property_count} properties")
                return True
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        return False

def get_latest_historical_snapshot(snapshot_type='sales'):
    """Get the latest historical snapshot of property data.
    
    Args:
        snapshot_type: 'sales' or 'rental'
    
    Returns:
        dict: Snapshot data or None if not found
    """
    conn = get_connection()
    if not conn:
        logger.error("Could not get historical snapshot - no connection")
        return None
    
    try:
        with conn:
            with conn.cursor() as cur:
                if snapshot_type == 'sales':
                    table = 'sales_historical_snapshots'
                else:
                    table = 'rental_historical_snapshots'
                
                cur.execute(f"""
                    SELECT id, snapshot_date, property_count, new_properties, 
                           updated_properties, snapshot_data
                    FROM {table}
                    ORDER BY snapshot_date DESC
                    LIMIT 1
                """)
                
                row = cur.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'snapshot_date': row[1],
                        'property_count': row[2],
                        'new_properties': row[3],
                        'updated_properties': row[4],
                        'snapshot_data': row[5]
                    }
                return None
    except Exception as e:
        logger.error(f"Error getting latest historical snapshot: {str(e)}")
        return None

def get_historical_snapshot_by_date(snapshot_date, snapshot_type='sales'):
    """Get a historical snapshot by date.
    
    Args:
        snapshot_date: Date to search for (datetime)
        snapshot_type: 'sales' or 'rental'
    
    Returns:
        dict: Snapshot data or None if not found
    """
    conn = get_connection()
    if not conn:
        logger.error("Could not get historical snapshot - no connection")
        return None
    
    try:
        with conn:
            with conn.cursor() as cur:
                if snapshot_type == 'sales':
                    table = 'sales_historical_snapshots'
                else:
                    table = 'rental_historical_snapshots'
                
                # Find the nearest snapshot to the requested date
                cur.execute(f"""
                    SELECT id, snapshot_date, property_count, new_properties, 
                           updated_properties, snapshot_data
                    FROM {table}
                    ORDER BY ABS(EXTRACT(EPOCH FROM (snapshot_date - %s))) ASC
                    LIMIT 1
                """, (snapshot_date,))
                
                row = cur.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'snapshot_date': row[1],
                        'property_count': row[2],
                        'new_properties': row[3],
                        'updated_properties': row[4],
                        'snapshot_data': row[5]
                    }
                return None
    except Exception as e:
        logger.error(f"Error getting historical snapshot by date: {str(e)}")
        return None

if __name__ == "__main__":
    # If run directly, initialize the database
    initialize_database() 