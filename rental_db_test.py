#!/usr/bin/env python3
import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Get database URL
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        logger.error("No database URL available")
        return
    
    # Connect to database
    try:
        conn = psycopg2.connect(db_url)
        logger.info("Successfully connected to database")
        
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
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 