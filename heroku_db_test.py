#!/usr/bin/env python3
import os
import logging
import psycopg2
import sys
import traceback

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Get database URL
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        logger.error("No DATABASE_URL environment variable found")
        return
    
    logger.info(f"Found DATABASE_URL: {db_url[:20]}...")
    
    # For newer PostgreSQL versions on Heroku, we need to add sslmode=require
    if db_url.startswith('postgres://'):
        # Log original connection string structure (obscured)
        parts = db_url.split('@')
        if len(parts) > 1:
            logger.info(f"Connection string format: postgres://[credentials]@{parts[1]}")
        
        # Force sslmode=require
        if '?' in db_url:
            db_url += '&sslmode=require'
        else:
            db_url += '?sslmode=require'
    
    # Connect to database
    try:
        logger.info("Attempting connection to database...")
        conn = psycopg2.connect(db_url)
        logger.info("Successfully connected to database!")
        
        # Query rental data
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM properties_rentals")
            count = cur.fetchone()[0]
            logger.info(f"Found {count} rental properties in database")
            
            if count > 0:
                cur.execute("SELECT url, price, size, rooms, location, price_per_sqm FROM properties_rentals LIMIT 5")
                rows = cur.fetchall()
                for i, row in enumerate(rows):
                    logger.info(f"Rental {i+1}: {row}")
            
            # Check if any other tables exist
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = cur.fetchall()
            logger.info(f"Database contains {len(tables)} tables: {[table[0] for table in tables]}")
        
        conn.close()
        logger.info("Database connection closed")
        
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    logger.info("Starting database connection test")
    main()
    logger.info("Database test completed") 