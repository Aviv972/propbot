#!/usr/bin/env python3
"""
Flask server to serve the PropBot investment dashboard and handle property analysis requests.
"""

import os
import logging
from pathlib import Path
from flask import Flask, send_file, jsonify, request, send_from_directory, render_template_string
from flask_cors import CORS
import subprocess
import threading
import json
import datetime
import socket
import re
import shutil

# Import the database utilities
from propbot.database_utils import (
    initialize_database, 
    get_rental_last_update, 
    set_rental_last_update,
    get_rental_update_frequency
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define paths
SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = SCRIPT_DIR.parent
UI_DIR = SCRIPT_DIR / "ui"
DASHBOARD_FILE = UI_DIR / "investment_dashboard_updated.html"

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Create necessary directories at startup
def ensure_directory_structure():
    """Ensure all required directories exist"""
    directories = [
        SCRIPT_DIR / "data",
        SCRIPT_DIR / "data" / "raw",
        SCRIPT_DIR / "data" / "raw" / "sales",
        SCRIPT_DIR / "data" / "raw" / "sales" / "history",
        SCRIPT_DIR / "data" / "raw" / "rentals",
        SCRIPT_DIR / "data" / "raw" / "rentals" / "history",
        SCRIPT_DIR / "data" / "processed",
        SCRIPT_DIR / "data" / "output",
        SCRIPT_DIR / "data" / "reports",
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {directory}")

# Ensure all directories exist on startup
ensure_directory_structure()

@app.route('/')
def dashboard():
    """Serve the dashboard HTML file"""
    if not DASHBOARD_FILE.exists():
        # Generate the dashboard if it doesn't exist
        try:
            logger.info("Dashboard file not found. Generating dashboard...")
            subprocess.run(
                ["python3", "-m", "propbot.generate_dashboard"],
                check=True
            )
            if not DASHBOARD_FILE.exists():
                return "Failed to generate dashboard file", 500
        except subprocess.SubprocessError as e:
            logger.error(f"Error generating dashboard: {str(e)}")
            return f"Error generating dashboard: {str(e)}", 500
    
    try:
        # Update localhost URLs in the dashboard file
        if DASHBOARD_FILE.exists():
            with open(DASHBOARD_FILE, 'r') as f:
                content = f.read()
            
            # Replace any hardcoded localhost URLs with relative URLs
            updated_content = re.sub(
                r"(fetch\()'http://localhost:\d+(/[^']+)'", 
                r"\1'\2'", 
                content
            )
            
            # Also replace any specific port references for run-analysis
            updated_content = re.sub(
                r"(fetch\()'http://localhost:\d+/run-analysis'", 
                r"\1'/run-analysis'", 
                updated_content
            )
            
            if content != updated_content:
                with open(DASHBOARD_FILE, 'w') as f:
                    f.write(updated_content)
                logger.info("Updated dashboard API endpoints to use relative URLs")
        
        logger.info(f"Serving dashboard file: {DASHBOARD_FILE}")
        return send_file(str(DASHBOARD_FILE))
    except Exception as e:
        logger.error(f"Error serving dashboard file: {str(e)}")
        # Fallback to a simple dashboard
        return render_template_string("""
            <!DOCTYPE html>
            <html>
            <head>
                <title>PropBot Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; }
                    .error { color: red; padding: 20px; border: 1px solid red; background-color: #ffeeee; }
                </style>
            </head>
            <body>
                <h1>PropBot Dashboard</h1>
                <div class="error">
                    <h2>Error loading dashboard</h2>
                    <p>There was an error loading the dashboard: {{ error }}</p>
                    <p>Try accessing one of these alternatives:</p>
                    <ul>
                        <li><a href="/standalone">Standalone dashboard</a></li>
                        <li><a href="/static/investment_dashboard_updated.html">Direct file access</a></li>
                    </ul>
                </div>
            </body>
            </html>
        """, error=str(e))

@app.route('/standalone')
def standalone():
    """Serve the standalone dashboard"""
    standalone_file = UI_DIR / "standalone_dashboard.html"
    if standalone_file.exists():
        return send_file(str(standalone_file))
    else:
        return "Standalone dashboard not found", 404

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files from the UI directory"""
    logger.info(f"Serving static file: {filename}")
    return send_from_directory(UI_DIR, filename)

@app.route('/neighborhood')
def neighborhood():
    """Serve the neighborhood report HTML file"""
    neighborhood_file = UI_DIR / "neighborhood_report_updated.html"
    if neighborhood_file.exists():
        logger.info(f"Serving neighborhood file: {neighborhood_file}")
        return send_file(str(neighborhood_file))
    else:
        logger.error(f"Neighborhood file not found: {neighborhood_file}")
        return "Neighborhood report not found", 404

@app.route('/stats')
def stats():
    """Return statistics about the data"""
    stats = {
        "status": "success",
        "timestamp": datetime.datetime.now().isoformat(),
        "sales": {
            "total_properties": 0,
            "last_updated": "Unknown"
        },
        "rentals": {
            "total_properties": 0,
            "last_updated": "Unknown"
        }
    }
    
    # Check sales data
    sales_data_path = SCRIPT_DIR / "data" / "raw" / "sales" / "idealista_listings.json"
    if sales_data_path.exists():
        try:
            with open(sales_data_path, 'r') as f:
                sales_data = json.load(f)
            stats["sales"]["total_properties"] = len(sales_data)
            stats["sales"]["last_updated"] = datetime.datetime.fromtimestamp(
                os.path.getmtime(sales_data_path)
            ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logger.error(f"Error reading sales data: {str(e)}")
    
    # Check rental data
    rental_metadata_path = SCRIPT_DIR / "data" / "processed" / "rental_metadata.json"
    if rental_metadata_path.exists():
        try:
            with open(rental_metadata_path, 'r') as f:
                metadata = json.load(f)
            stats["rentals"]["last_updated"] = metadata.get("last_update", "Unknown")
            
            rental_data_path = SCRIPT_DIR / "data" / "processed" / "rentals.csv"
            if rental_data_path.exists():
                # Count lines in CSV (skip header)
                with open(rental_data_path, 'r') as f:
                    stats["rentals"]["total_properties"] = sum(1 for _ in f) - 1
        except Exception as e:
            logger.error(f"Error reading rental data: {str(e)}")
    
    return jsonify(stats)

@app.route('/run-analysis', methods=['POST'])
def run_analysis():
    """Run the complete property analysis workflow in a separate thread and return immediately"""
    # Get parameters from the request JSON if available
    force_rental_update = request.json.get('force_rental_update', False) if request.is_json else False
    max_sales_pages = request.json.get('max_sales_pages', None) if request.is_json else None
    max_rental_pages = request.json.get('max_rental_pages', None) if request.is_json else None
    skip_scraping = request.json.get('skip_scraping', False) if request.is_json else False
    
    # Define paths
    sales_data_path = SCRIPT_DIR / "idealista_listings.json"
    
    # Create results object
    results = {
        "success": True,
        "message": "Complete analysis workflow started - this will take some time",
        "note": "The list of properties for sale will continuously grow as new properties are analyzed each time."
    }
    
    if skip_scraping:
        results["message"] = "Analysis workflow started with scraping skipped - using existing data"
    
    # Define task with parameters
    def task():
        run_analysis_task(force_rental_update, max_sales_pages, max_rental_pages, skip_scraping)
    
    # Start analysis in background thread
    thread = threading.Thread(target=task)
    thread.daemon = True
    thread.start()
    
    return jsonify(results)

def run_analysis_task(force_rental_update=False, max_sales_pages=None, max_rental_pages=None, skip_scraping=False):
    """Run the complete property analysis workflow"""
    try:
        results = {}
        sales_data_path = SCRIPT_DIR / "idealista_listings.json"
        
        if not skip_scraping:
            # Step 1: Run the scraper for sales properties
            # Ensure the data directory exists
            os.makedirs(os.path.dirname(os.path.join(SCRIPT_DIR, "propbot/data/raw/sales")), exist_ok=True)
            
            # Run the scraper directly as a module import instead of subprocess
            try:
                logger.info("Importing and running sales scraper directly...")
                from propbot.scrapers.idealista_scraper import run_scraper
                # Set environment variable for max pages
                if max_sales_pages:
                    logger.info(f"Using max_sales_pages={max_sales_pages}")
                    os.environ["MAX_SALES_PAGES"] = str(max_sales_pages)
                # Run the scraper with no parameters - it will use the environment variable
                new_properties_count = run_scraper()
                results["new_properties"] = new_properties_count
                
                # Immediately update the database with new sales data
                try:
                    logger.info("Updating database with new sales data...")
                    from propbot.data_processing.update_db import update_database_after_scrape
                    if update_database_after_scrape('sales'):
                        logger.info("Database successfully updated with new sales data")
                    else:
                        logger.warning("Failed to update database with new sales data")
                except Exception as e:
                    logger.error(f"Error updating database with sales data: {str(e)}")
                
                # Check file for total properties count
                if sales_data_path.exists():
                    try:
                        with open(sales_data_path, 'r') as f:
                            properties_data = json.load(f)
                            results["total_properties"] = len(properties_data)
                    except Exception as e:
                        logger.error(f"Error reading property data: {str(e)}")
            except Exception as e:
                logger.error(f"Error running sales scraper: {str(e)}")
        else:
            logger.info("Skipping scraping step as requested - using existing data")
        
        # Step 2: Check if rental data needs to be updated (once every 30 days)
        # Define the path to rental data file
        rental_data_path = SCRIPT_DIR / "data" / "processed" / "rentals.csv"
        
        # Initialize should_update_rentals based on force parameter
        if force_rental_update:
            should_update_rentals = True
            logger.info("Forced rental update requested - will update rental data regardless of age")
        else:
            should_update_rentals = True  # Default value
        
        # Check the database for last update date
        last_update = get_rental_last_update()
        
        if last_update is not None:
            # Calculate days since the last update
            days_since_update = (datetime.datetime.now() - last_update).days
            update_frequency = get_rental_update_frequency()
            
            if not force_rental_update and days_since_update < update_frequency:
                logger.info(f"Rental data was updated {days_since_update} days ago. Skipping rental data collection (limit: {update_frequency} days).")
                should_update_rentals = False
            else:
                logger.info(f"Rental data is {days_since_update} days old. Running rental data collection.")
        else:
            logger.info("No rental update history found in database. Running initial rental data collection.")
        
        if should_update_rentals:
            # Run rental scraper to collect rental data
            logger.info("Collecting rental property data...")
            
            # Ensure required directories exist before running rental scraper
            raw_rentals_dir = SCRIPT_DIR / "data" / "raw" / "rentals"
            rentals_history_dir = raw_rentals_dir / "history"
            os.makedirs(raw_rentals_dir, exist_ok=True)
            os.makedirs(rentals_history_dir, exist_ok=True)
            logger.info(f"Ensured directory structure exists at {raw_rentals_dir}")
            
            # Run the rental scraper directly as a module import instead of subprocess
            try:
                logger.info("Importing and running rental scraper directly...")
                from propbot.scrapers.rental_scraper import run_rental_scraper
                # Set environment variable for max pages
                if max_rental_pages:
                    logger.info(f"Using max_rental_pages={max_rental_pages}")
                    os.environ["MAX_RENTAL_PAGES"] = str(max_rental_pages)
                # Run the scraper with no parameters - it will use the environment variable
                new_rentals_count = run_rental_scraper()
                results["new_rentals"] = new_rentals_count
                
                # Update the database with rental data after successful run
                try:
                    logger.info("Updating database with new rental data...")
                    from propbot.data_processing.update_db import update_database_after_scrape
                    if update_database_after_scrape('rentals'):
                        logger.info("Database successfully updated with new rental data")
                    else:
                        logger.warning("Failed to update database with new rental data")
                except Exception as e:
                    logger.error(f"Error updating database with rental data: {str(e)}")
                
                # Update the database with current time after successful run
                set_rental_last_update()
                logger.info("Updated rental last update timestamp in database")
                
            except Exception as e:
                logger.error(f"Error running rental scraper: {str(e)}")
        else:
            logger.info("Using existing rental data (less than update frequency threshold).")
            
        # Step 3: Process and consolidate data
        logger.info("Processing and consolidating data...")
        
        # Ensure processed directory exists
        processed_dir = SCRIPT_DIR / "data" / "processed"
        os.makedirs(processed_dir, exist_ok=True)
        logger.info(f"Ensured processed data directory exists at {processed_dir}")
        
        # Set up paths correctly for processing
        sales_source = SCRIPT_DIR / "data" / "raw" / "sales" / "idealista_listings.json"
        sales_dest = SCRIPT_DIR / "data" / "raw" / "sales_listings.json"
        rentals_source = SCRIPT_DIR / "data" / "raw" / "rentals" / "rental_listings.json"
        rentals_dest = SCRIPT_DIR / "data" / "raw" / "rental_listings.json"
        
        # Create empty files for data processing pipeline if they don't exist
        for file_path in [sales_source, rentals_source]:
            if not os.path.exists(file_path):
                logger.info(f"Creating empty file at {file_path}")
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, 'w') as f:
                    f.write('[]')  # Empty JSON array instead of empty object
        
        # Copy the source files to the destination paths needed by the pipeline
        try:
            if os.path.exists(sales_source):
                logger.info(f"Copying {sales_source} to {sales_dest}")
                shutil.copy2(sales_source, sales_dest)
            else:
                logger.warning(f"Sales source file not found at {sales_source}")
                # Create an empty file to avoid processing errors
                with open(sales_dest, 'w') as f:
                    f.write('[]')
            
            if os.path.exists(rentals_source):
                logger.info(f"Copying {rentals_source} to {rentals_dest}")
                shutil.copy2(rentals_source, rentals_dest)
            else:
                logger.warning(f"Rentals source file not found at {rentals_source}")
                # Create an empty file to avoid processing errors
                with open(rentals_dest, 'w') as f:
                    f.write('[]')
            
            # Also ensure the processed directory has the necessary files
            sales_processed = processed_dir / "sales_listings_consolidated.json"
            rentals_processed = processed_dir / "rental_listings_consolidated.json"
            
            for file_path in [sales_processed, rentals_processed]:
                if not os.path.exists(file_path):
                    logger.info(f"Creating empty processed file at {file_path}")
                    with open(file_path, 'w') as f:
                        f.write('[]')  # Empty JSON array
        except Exception as e:
            logger.error(f"Error setting up files for data processing: {str(e)}")
        
        # Now run the data processing pipeline
        try:
            # Use absolute paths for the environment variables
            env = os.environ.copy()
            env["PROPBOT_DATA_DIR"] = str(SCRIPT_DIR / "data")
            
            subprocess.run(
                ["python3", "-m", "propbot.data_processing.pipeline.standard"],
                check=True,
                env=env
            )
            logger.info("Data processing completed successfully")
        except subprocess.SubprocessError as e:
            logger.error(f"Error in data processing pipeline: {str(e)}")
        
        # Step 4: Run rental analysis
        logger.info("Running rental analysis...")
        try:
            # Set up environment variables for the subprocess
            env = os.environ.copy()
            env["PROPBOT_DATA_DIR"] = str(SCRIPT_DIR / "data")
            
            # Ensure the right files exist for the rental analysis
            sales_csv = processed_dir / "sales.csv"
            rentals_csv = processed_dir / "rentals.csv"
            
            for file_path in [sales_csv, rentals_csv]:
                if not os.path.exists(file_path):
                    logger.warning(f"Creating empty CSV file at {file_path}")
                    with open(file_path, 'w') as f:
                        if "sales" in str(file_path):
                            f.write("url,title,price,location,size,room_type,details\n")
                        else:
                            f.write("url,title,price,location,size,room_type\n")
            
            subprocess.run(
                ["python3", "-m", "propbot.analysis.metrics.rental_analysis"],
                check=True,
                env=env
            )
            logger.info("Rental analysis completed successfully")
        except subprocess.SubprocessError as e:
            logger.error(f"Error in rental analysis: {str(e)}")
            
        # Step 5: Run investment analysis
        logger.info("Running investment analysis...")
        try:
            subprocess.run(
                ["python3", "-m", "propbot.run_investment_analysis"],
                check=True,
                env=env
            )
            logger.info("Investment analysis completed successfully")
        except subprocess.SubprocessError as e:
            logger.error(f"Error in investment analysis: {str(e)}")
            
        # Step 6: Generate the dashboard
        logger.info("Generating dashboard...")
        try:
            # Copy the output data to the UI directory for serving
            output_dir = SCRIPT_DIR / "data" / "output"
            ui_dir = SCRIPT_DIR / "ui"
            os.makedirs(output_dir / "visualizations", exist_ok=True)
            os.makedirs(ui_dir, exist_ok=True)
            
            subprocess.run(
                ["python3", "-m", "propbot.generate_dashboard"],
                check=True,
                env=env
            )
            
            # Copy latest dashboard to correct location
            dashboard_source = output_dir / "visualizations" / "investment_dashboard.html"
            dashboard_dest = ui_dir / "investment_dashboard_latest.html"
            
            if dashboard_source.exists():
                logger.info(f"Copying dashboard from {dashboard_source} to {dashboard_dest}")
                shutil.copy2(dashboard_source, dashboard_dest)
            
            logger.info("Dashboard generation completed successfully")
        except subprocess.SubprocessError as e:
            logger.error(f"Error in dashboard generation: {str(e)}")
        
        logger.info("Complete analysis workflow finished successfully")
    except Exception as e:
        logger.error(f"Error running analysis: {str(e)}")
    
    # Initialize the database on startup (this will do nothing if it's already initialized)
    try:
        initialize_database()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
    
    return jsonify({
        "success": True,
        "message": "Complete analysis workflow started - this will take some time",
        "note": "The list of properties for sale will continuously grow as new properties are analyzed each time."
    })

def main():
    """Run the dashboard server"""
    # Get the port from environment variable for Heroku compatibility
    port = int(os.environ.get("PORT", 8004))
    
    logger.info(f"Starting dashboard server on port {port}")
    
    # Update dashboard file with correct port if it exists
    try:
        if DASHBOARD_FILE.exists():
            with open(DASHBOARD_FILE, 'r') as f:
                content = f.read()
            
            # This regex will update any localhost:XXXX pattern in the fetch URL
            updated_content = re.sub(
                r"fetch\('http://localhost:\d+/run-analysis'", 
                f"fetch(window.location.origin + '/run-analysis'", 
                content
            )
            
            with open(DASHBOARD_FILE, 'w') as f:
                f.write(updated_content)
            
            logger.info(f"Updated dashboard API endpoints to use dynamic origin")
    except Exception as e:
        logger.error(f"Error updating dashboard endpoints: {str(e)}")
    
    # Start the Flask app
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    main() 