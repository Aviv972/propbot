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
DASHBOARD_FILE = UI_DIR / "investment_dashboard_latest.html"

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
                        <li><a href="/static/investment_dashboard_latest.html">Direct file access</a></li>
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
    # Get the max pages from query parameters
    max_sales_pages = request.args.get('max_sales_pages', None)
    max_rental_pages = request.args.get('max_rental_pages', None)
    debug_mode = request.args.get('debug', 'false').lower() == 'true'
    
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug mode enabled for analysis run")
    
    def run_analysis_task():
        try:
            results = {
                "new_properties": 0,
                "updated_properties": 0,
                "total_properties": 0,
                "start_time": datetime.datetime.now().isoformat()
            }
            
            # Record initial property count
            sales_data_path = SCRIPT_DIR / "data" / "raw" / "sales" / "idealista_listings.json"
            initial_count = 0
            if sales_data_path.exists():
                try:
                    with open(sales_data_path, 'r') as f:
                        initial_count = len(json.load(f))
                except Exception as e:
                    logger.error(f"Error reading initial property count: {str(e)}")
            
            # Step 1: Run the web scraper to get new listings
            logger.info("Running web scraper to collect new property listings...")
            
            # Ensure required directories exist before running scraper
            raw_sales_dir = SCRIPT_DIR / "data" / "raw" / "sales"
            history_dir = raw_sales_dir / "history"
            os.makedirs(raw_sales_dir, exist_ok=True)
            os.makedirs(history_dir, exist_ok=True)
            logger.info(f"Ensured directory structure exists at {raw_sales_dir}")
            
            # Run the scraper directly as a module import instead of subprocess
            try:
                logger.info("Importing and running sales scraper directly...")
                from propbot.scrapers.idealista_scraper import run_scraper
                # Pass max_pages if provided
                if max_sales_pages:
                    logger.info(f"Using max_sales_pages={max_sales_pages}")
                    new_properties_count = run_scraper(max_pages=int(max_sales_pages))
                else:
                    new_properties_count = run_scraper()
                results["new_properties"] = new_properties_count
                
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
            
            # Step 2: Check if rental data needs to be updated (once every 30 days)
            # Define the path to rental data file
            rental_data_path = SCRIPT_DIR / "data" / "processed" / "rentals.csv"
            rental_metadata_path = SCRIPT_DIR / "data" / "processed" / "rental_metadata.json"
            
            should_update_rentals = True
            
            # Check if the metadata file exists and when the last update was done
            if os.path.exists(rental_metadata_path):
                try:
                    with open(rental_metadata_path, 'r') as f:
                        metadata = json.load(f)
                    
                    last_update = datetime.datetime.fromisoformat(metadata.get('last_update', '2000-01-01'))
                    days_since_update = (datetime.datetime.now() - last_update).days
                    
                    if days_since_update < 30:
                        logger.info(f"Rental data was updated {days_since_update} days ago. Skipping rental data collection (limit: 30 days).")
                        should_update_rentals = False
                    else:
                        logger.info(f"Rental data is {days_since_update} days old. Running rental data collection.")
                except Exception as e:
                    logger.warning(f"Error reading rental metadata, will update rentals: {str(e)}")
            else:
                logger.info("No rental metadata found. Running initial rental data collection.")
            
            if should_update_rentals:
                # Run rental scraper to collect rental data
                logger.info("Collecting rental property data...")
                
                # Ensure required directories exist before running rental scraper
                raw_rentals_dir = SCRIPT_DIR / "data" / "raw" / "rentals"
                rentals_history_dir = raw_rentals_dir / "history"
                os.makedirs(raw_rentals_dir, exist_ok=True)
                os.makedirs(rentals_history_dir, exist_ok=True)
                os.makedirs(os.path.dirname(rental_metadata_path), exist_ok=True)
                logger.info(f"Ensured directory structure exists at {raw_rentals_dir}")
                
                # Run the rental scraper directly as a module import instead of subprocess
                try:
                    logger.info("Importing and running rental scraper directly...")
                    from propbot.scrapers.rental_scraper import run_rental_scraper
                    # Pass max_pages if provided
                    if max_rental_pages:
                        logger.info(f"Using max_rental_pages={max_rental_pages}")
                        new_rentals_count = run_rental_scraper(max_pages=int(max_rental_pages))
                    else:
                        new_rentals_count = run_rental_scraper()
                    results["new_rentals"] = new_rentals_count
                except Exception as e:
                    logger.error(f"Error running rental scraper: {str(e)}")
                
                # Update metadata after successful collection
                try:
                    with open(rental_metadata_path, 'w') as f:
                        json.dump({
                            'last_update': datetime.datetime.now().isoformat(),
                            'update_frequency': '30 days'
                        }, f)
                    logger.info("Updated rental metadata with current timestamp.")
                except Exception as e:
                    logger.error(f"Error updating rental metadata: {str(e)}")
            else:
                logger.info("Using existing rental data (less than 30 days old).")
            
            # Step 3: Process and consolidate data
            logger.info("Processing and consolidating data...")
            
            # Ensure processed directory exists
            processed_dir = SCRIPT_DIR / "data" / "processed"
            os.makedirs(processed_dir, exist_ok=True)
            logger.info(f"Ensured processed data directory exists at {processed_dir}")
            
            # Copy raw data to right locations for processing
            sales_source = SCRIPT_DIR / "data" / "raw" / "sales" / "idealista_listings.json"
            sales_dest = SCRIPT_DIR / "data" / "raw" / "sales_listings.json"
            rentals_source = SCRIPT_DIR / "data" / "raw" / "rentals" / "rental_listings.json"
            rentals_dest = SCRIPT_DIR / "data" / "raw" / "rental_listings.json"
            
            # Ensure the raw files are in the right place for processing
            if sales_source.exists() and not sales_dest.exists():
                logger.info(f"Copying {sales_source} to {sales_dest}")
                shutil.copy2(sales_source, sales_dest)
            
            if rentals_source.exists() and not rentals_dest.exists():
                logger.info(f"Copying {rentals_source} to {rentals_dest}")
                shutil.copy2(rentals_source, rentals_dest)
            
            try:
                subprocess.run(
                    ["python3", "-m", "propbot.data_processing.pipeline.standard"],
                    check=True
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
        except subprocess.SubprocessError as e:
            logger.error(f"Error running analysis: {str(e)}")
    
    # Start a background thread to run the analysis
    thread = threading.Thread(target=run_analysis_task)
    thread.daemon = True
    thread.start()
    
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