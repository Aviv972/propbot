#!/usr/bin/env python3
"""
Upload CSV files to Heroku and import them into the database

This script uploads the CSV files to Heroku and then triggers the import process.
"""

import os
import sys
import subprocess
import argparse

def main():
    """Main function to upload files and run import script."""
    parser = argparse.ArgumentParser(description='Upload CSV files to Heroku and import them')
    parser.add_argument('--app', '-a', default='propbot-investment-analyzer', 
                        help='Heroku app name (default: propbot-investment-analyzer)')
    args = parser.parse_args()
    
    app_name = args.app
    
    # Check if the CSV files exist
    if not os.path.exists('uploads/sales_current.csv'):
        print("Error: sales_current.csv not found in uploads directory")
        return 1
    
    if not os.path.exists('uploads/rentals_current.csv'):
        print("Error: rentals_current.csv not found in uploads directory")
        return 1
    
    print("Step 1: Creating uploads directory on Heroku")
    try:
        subprocess.run(['heroku', 'run', 'mkdir -p uploads', '-a', app_name], 
                      check=True, capture_output=True)
        print("Uploads directory created successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error creating uploads directory: {e}")
        print(e.stdout.decode())
        print(e.stderr.decode())
        return 1
    
    print("Step 2: Uploading sales_current.csv to Heroku")
    try:
        with open('uploads/sales_current.csv', 'rb') as f:
            upload_process = subprocess.run(
                ['heroku', 'run', '--no-tty', 'cat > uploads/sales_current.csv', '-a', app_name],
                input=f.read(), check=True, capture_output=True
            )
        print("Sales file uploaded successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading sales file: {e}")
        print(e.stdout.decode())
        print(e.stderr.decode())
        return 1
    
    print("Step 3: Uploading rentals_current.csv to Heroku")
    try:
        with open('uploads/rentals_current.csv', 'rb') as f:
            upload_process = subprocess.run(
                ['heroku', 'run', '--no-tty', 'cat > uploads/rentals_current.csv', '-a', app_name],
                input=f.read(), check=True, capture_output=True
            )
        print("Rentals file uploaded successfully")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading rentals file: {e}")
        print(e.stdout.decode())
        print(e.stderr.decode())
        return 1
    
    print("Step 4: Running import_data.py script")
    try:
        import_process = subprocess.run(
            ['heroku', 'run', 'python', 'import_data.py', '-a', app_name],
            check=True, capture_output=False
        )
    except subprocess.CalledProcessError as e:
        print(f"Error running import script: {e}")
        return 1
    
    print("CSV files uploaded and import process triggered")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 