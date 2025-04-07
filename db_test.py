#!/usr/bin/env python3
import os
import sys
import psycopg2
import psycopg2.extras

# This script tests database connectivity to Heroku Postgres
# It handles Heroku's connection string format and ensures SSL is used

def main():
    print("Testing database connection...")
    
    # Get database URL
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        print("ERROR: No DATABASE_URL environment variable found")
        sys.exit(1)
    
    # Add sslmode=require if not already present
    if 'sslmode=' not in db_url:
        db_url = db_url + ('&' if '?' in db_url else '?') + 'sslmode=require'
    
    print(f"Connection string format: {db_url.split('@')[0].split(':')[0]}:[credentials]@{db_url.split('@')[1]}")
    
    try:
        # Connect with SSL required
        print("Connecting to database...")
        conn = psycopg2.connect(db_url)
        
        # Check database version
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(f"Connected to PostgreSQL: {version}")
            
            # Check tables in database
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = [table[0] for table in cur.fetchall()]
            print(f"Found tables: {tables}")
            
            # Query rental data
            print("\nQuerying rental_properties table:")
            cur.execute("SELECT COUNT(*) FROM properties_rentals")
            count = cur.fetchone()[0]
            print(f"Found {count} rental properties")
            
            # Show a few records
            if count > 0:
                cur.execute("SELECT url, price, size, rooms, location FROM properties_rentals LIMIT 3")
                rows = cur.fetchall()
                for i, row in enumerate(rows):
                    print(f"Rental {i+1}: {row}")
                    
        # Close connection
        conn.close()
        print("\nDatabase connection test successful!")
        
    except Exception as e:
        print(f"ERROR: Database connection failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 