"""PropBot API endpoints for the dashboard"""

import os
import json
import logging
import subprocess
from flask import Flask, request, jsonify, send_from_directory
from .config import UI_DIR, check_rental_data_needs_update

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/')
def index():
    """Serve the main dashboard page"""
    return send_from_directory(UI_DIR, 'investment_dashboard.html')

@app.route('/<path:path>')
def serve_file(path):
    """Serve static files from the UI directory"""
    return send_from_directory(UI_DIR, path)

@app.route('/api/should_update_rental', methods=['GET'])
def should_update_rental():
    """Check if rental data should be updated based on 30-day threshold"""
    needs_update = check_rental_data_needs_update()
    return jsonify({"should_update": needs_update})

@app.route('/api/run_workflow', methods=['POST'])
def run_workflow():
    """Run the PropBot workflow"""
    try:
        data = request.json
        force = data.get('force', False)
        command = data.get('command', 'python3 -m propbot.main --scrape both --analyze --report all')
        
        # Add force flag if requested
        if force and '--force' not in command:
            command += ' --force'
        
        logger.info(f"Running command: {command}")
        
        # Run the command and capture output
        process = subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate()
        
        if process.returncode != 0:
            logger.error(f"Error running command: {stderr}")
            return jsonify({"success": False, "error": stderr})
        
        logger.info("Workflow completed successfully")
        return jsonify({"success": True, "output": stdout})
    
    except Exception as e:
        logger.exception("Error running workflow")
        return jsonify({"success": False, "error": str(e)})

@app.route('/api/workflow_status', methods=['GET'])
def workflow_status():
    """Get the status of the last workflow run and data freshness"""
    try:
        # Check if we need to update rental data
        rental_update_needed = check_rental_data_needs_update()
        
        # Get stats about property counts
        stats = {
            "rental_data_needs_update": rental_update_needed,
            "property_counts": get_property_counts()
        }
        
        return jsonify({"success": True, "stats": stats})
    except Exception as e:
        logger.exception("Error getting workflow status")
        return jsonify({"success": False, "error": str(e)})

def get_property_counts():
    """Get counts of properties in the database"""
    try:
        # Count sales properties
        sales_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "idealista_listings.json")
        sales_count = 0
        if os.path.exists(sales_file):
            with open(sales_file, 'r', encoding='utf-8') as f:
                sales_data = json.load(f)
                sales_count = len(sales_data)
        
        # Count rental properties
        rental_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "rental_listings.json")
        rental_count = 0
        if os.path.exists(rental_file):
            with open(rental_file, 'r', encoding='utf-8') as f:
                rental_data = json.load(f)
                # Count total listings in all months
                rental_count = sum(len(month.get('listings', [])) for month in rental_data)
        
        return {
            "sales": sales_count,
            "rentals": rental_count,
            "total": sales_count + rental_count
        }
    except Exception as e:
        logger.exception("Error getting property counts")
        return {"sales": 0, "rentals": 0, "total": 0}

def run_api_server(host='0.0.0.0', port=5000, debug=False):
    """Run the API server"""
    logger.info(f"Starting API server on {host}:{port}")
    app.run(host=host, port=port, debug=debug)

if __name__ == '__main__':
    run_api_server(debug=True) 