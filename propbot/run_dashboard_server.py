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
            scraper_result = subprocess.run(
                ["python3", "-m", "propbot.scrapers.idealista_scraper"],
                check=True,
                capture_output=True,
                text=True
            )
            
            # Parse scraper output to find new properties count
            for line in scraper_result.stdout.splitlines():
                if "new properties found" in line:
                    try:
                        results["new_properties"] = int(line.split()[0])
                    except (ValueError, IndexError):
                        pass
                elif "properties updated" in line:
                    try:
                        results["updated_properties"] = int(line.split()[2])
                    except (ValueError, IndexError):
                        pass
                elif "Total properties:" in line:
                    try:
                        results["total_properties"] = int(line.split()[2])
                    except (ValueError, IndexError):
                        pass
            
            # If we couldn't parse the output, check the file count
            if results["total_properties"] == 0 and sales_data_path.exists():
                try:
                    with open(sales_data_path, 'r') as f:
                        results["total_properties"] = len(json.load(f))
                        # Calculate new and updated if we have the total
                        if initial_count > 0:
                            results["new_properties"] = results["total_properties"] - initial_count
                except Exception as e:
                    logger.error(f"Error calculating property growth: {str(e)}")
            
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
                subprocess.run(
                    ["python3", "-m", "propbot.scrapers.rental_scraper"],
                    check=True
                )
                
                # Update metadata after successful collection
                try:
                    os.makedirs(os.path.dirname(rental_metadata_path), exist_ok=True)
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
            subprocess.run(
                ["python3", "-m", "propbot.data_processing.pipeline.standard"],
                check=True
            )
            
            # Step 4: Run rental analysis
            logger.info("Running rental analysis...")
            subprocess.run(
                ["python3", "-m", "propbot.analysis.metrics.rental_analysis"],
                check=True
            )
            
            # Step 5: Run investment analysis
            logger.info("Running investment analysis...")
            subprocess.run(
                ["python3", "-m", "propbot.run_investment_analysis"],
                check=True
            )
            
            # Step 6: Generate the dashboard
            logger.info("Generating dashboard...")
            subprocess.run(
                ["python3", "-m", "propbot.generate_dashboard"],
                check=True
            )
            
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