#!/usr/bin/env python3
"""
Generate a dashboard for visualizing property investment metrics
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import glob
import subprocess
import numpy as np
from typing import Dict, Any, List, Optional, Union

# Import environment loader module - this must be the first import
from propbot.env_loader import reload_env

# Make sure environment variables are loaded
reload_env()

# Import database functions
try:
    from propbot.database_utils import (
        get_connection,
        get_latest_historical_snapshot, 
        get_analyzed_properties_from_database,
        get_rental_last_update,
        get_rental_update_frequency
    )
    HAS_DB_FUNCTIONS = True
except ImportError:
    HAS_DB_FUNCTIONS = False

from propbot.analysis.metrics.db_functions import (
    get_analyzed_properties,
    get_rental_last_update,
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
DATA_DIR = SCRIPT_DIR / "data"
REPORTS_DIR = DATA_DIR / "reports"
UI_DIR = SCRIPT_DIR / "ui"

def get_latest_report():
    """Get the most recent investment summary report"""
    logger.info("Finding the latest investment summary report...")
    
    # Find all investment summary JSON files
    report_pattern = str(REPORTS_DIR / "investment_summary_*.json")
    report_files = glob.glob(report_pattern)
    
    if not report_files:
        logger.error(f"No investment summary reports found matching {report_pattern}")
        return None
    
    # Sort by modification time (most recent first)
    latest_report = max(report_files, key=os.path.getmtime)
    logger.info(f"Found latest report: {latest_report}")
    
    return latest_report

def load_investment_data():
    """Load investment data from database or files."""
    logger.info("Loading investment data")
    
    # Try database first
    if HAS_DB_FUNCTIONS:
        logger.info("Attempting to load data from database")
        analyzed_properties = get_analyzed_properties_from_database()
        if analyzed_properties:
            logger.info(f"Loaded {len(analyzed_properties)} properties from database")
            return analyzed_properties
        else:
            logger.warning("No data found in database, trying files")
    
    # Fallback to files
    sales_with_metrics = None
    
    # Try the most recent dated file first
    report_files = list(REPORTS_DIR.glob("investment_summary_*.json"))
    if report_files:
        # Sort by modification time (newest first)
        report_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        latest_file = report_files[0]
        logger.info(f"Using latest investment summary: {latest_file}")
        
        try:
            with open(latest_file, 'r', encoding='utf-8') as f:
                sales_with_metrics = json.load(f)
            logger.info(f"Loaded {len(sales_with_metrics)} properties from {latest_file}")
            return sales_with_metrics
        except Exception as e:
            logger.error(f"Error loading {latest_file}: {str(e)}")
    
    # Try standard file
    standard_file = REPORTS_DIR / "investment_summary_current.json"
    if standard_file.exists():
        try:
            with open(standard_file, 'r', encoding='utf-8') as f:
                sales_with_metrics = json.load(f)
            logger.info(f"Loaded {len(sales_with_metrics)} properties from {standard_file}")
            return sales_with_metrics
        except Exception as e:
            logger.error(f"Error loading {standard_file}: {str(e)}")
    
    logger.warning("No investment data found")
            return []
        
def get_rental_update_info():
    """Get information about when rental data was last updated."""
    if HAS_DB_FUNCTIONS:
        # Get from database
        last_update = get_rental_last_update()
        update_frequency = get_rental_update_frequency()
        
        if last_update:
            last_update_str = last_update.strftime("%Y-%m-%d")
            return {
                "last_updated": last_update_str,
                "update_frequency": update_frequency
            }
    
    # Fallback to file-based approach
    metadata_file = PROCESSED_DIR / "rental_metadata.json"
    if metadata_file.exists():
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            return {
                "last_updated": metadata.get("last_update", "Unknown"),
                "update_frequency": 30  # Default
            }
        except Exception as e:
            logger.error(f"Error loading rental metadata: {str(e)}")
    
    # Default values if nothing found
    return {
        "last_updated": "Unknown",
        "update_frequency": 30
    }

def run_property_analysis():
    """Run the property analysis script"""
    try:
        logger.info("Running property analysis...")
        result = subprocess.run(
            ["python3", "-m", "propbot.run_investment_analysis"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.info("Property analysis completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running property analysis: {str(e)}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Error: {e.stderr}")
        return False

def generate_html_dashboard(investment_data):
    """Generate HTML dashboard from investment data."""
    logger.info("Generating HTML dashboard")
    
    if not investment_data:
        logger.warning("No investment data available for dashboard generation")
        return generate_empty_dashboard()
    
    # Get rental update information
    rental_info = get_rental_update_info()
    
    # Count valid properties (those with all metrics calculated)
    valid_properties = [p for p in investment_data if p.get('monthly_rent', 0) > 0]
    logger.info(f"Found {len(valid_properties)} properties with valid rental estimates")
    
    # Current date for the dashboard
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Load the template
    template_path = TEMPLATES_DIR / "dashboard_template.html"
    with open(template_path, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    # Replace placeholders with actual data
    template_content = template_content.replace("{{GENERATION_DATE}}", current_date)
    template_content = template_content.replace("{{RENTAL_DATA_LAST_UPDATED}}", rental_info["last_updated"])
    template_content = template_content.replace("{{RENTAL_UPDATE_FREQUENCY}}", str(rental_info["update_frequency"]))
    template_content = template_content.replace("{{TOTAL_PROPERTIES}}", str(len(investment_data)))
    template_content = template_content.replace("{{VALID_PROPERTIES}}", str(len(valid_properties)))
    
    # Generate property cards HTML
    if valid_properties:
        # Sort by cash flow
        sorted_properties = sorted(valid_properties, key=lambda x: x.get('monthly_cash_flow', 0), reverse=True)
        top_properties = sorted_properties[:50]  # Top 50 properties
        
        property_cards_html = ""
        for prop in top_properties:
            # Create card for each property
            card_html = generate_property_card(prop)
            property_cards_html += card_html
        
        template_content = template_content.replace("{{PROPERTY_CARDS}}", property_cards_html)
                else:
        # No valid properties
        template_content = template_content.replace("{{PROPERTY_CARDS}}", "<p>No properties with valid rental estimates found.</p>")
    
    # Generate charts data
    charts_data = generate_charts_data(investment_data)
    template_content = template_content.replace("{{CHARTS_DATA}}", json.dumps(charts_data))
    
    # Save the dashboard HTML
    dashboard_path = VISUALIZATIONS_DIR / "investment_dashboard.html"
    with open(dashboard_path, 'w', encoding='utf-8') as f:
        f.write(template_content)
    
    # Also save to the UI directory for serving
    ui_dashboard_path = UI_DIR / "investment_dashboard_updated.html"
    with open(ui_dashboard_path, 'w', encoding='utf-8') as f:
        f.write(template_content)
    
    logger.info(f"Dashboard saved to {dashboard_path} and {ui_dashboard_path}")
    return dashboard_path

def main():
    """Main entry point for the script"""
    logger.info("Starting dashboard generation...")
    
    # Run new property analysis by default
    skip_analysis = os.environ.get('SKIP_ANALYSIS', '').lower() == 'true'
    
    if not skip_analysis:
        success = run_property_analysis()
        if not success:
            logger.warning("Property analysis failed. Attempting to use existing data.")
    
    # Get the latest investment summary report
    report_file = get_latest_report()
    if not report_file:
        logger.error("No investment summary report found. Cannot generate dashboard.")
        return False
    
    # Load investment data
    investment_data = load_investment_data()
    if not investment_data:
        logger.error("Failed to load investment data. Cannot generate dashboard.")
        return False
    
    # Generate HTML dashboard
    dashboard_html = generate_html_dashboard(investment_data)
    if not dashboard_html:
        logger.error("Failed to generate dashboard HTML.")
        return False
    
    # Write to file
    latest_dashboard_file = UI_DIR / "investment_dashboard_latest.html"
    updated_dashboard_file = UI_DIR / "investment_dashboard_updated.html"
    
    try:
        # Convert Path objects to strings when writing
        latest_dashboard_path = str(latest_dashboard_file)
        updated_dashboard_path = str(updated_dashboard_file)
        
        # Write dashboard HTML content to files
        if isinstance(dashboard_html, Path):
            # If dashboard_html is a Path, read its contents and write to output files
            with open(dashboard_html, 'r') as src:
                content = src.read()
                
            with open(latest_dashboard_path, 'w') as f:
                f.write(content)
            logger.info(f"Saved latest dashboard to {latest_dashboard_file}")
            
            with open(updated_dashboard_path, 'w') as f:
                f.write(content)
            logger.info(f"Saved updated dashboard to {updated_dashboard_file}")
        else:
            # If dashboard_html is a string, write it directly
            with open(latest_dashboard_path, 'w') as f:
                f.write(dashboard_html)
            logger.info(f"Saved latest dashboard to {latest_dashboard_file}")
            
            with open(updated_dashboard_path, 'w') as f:
                f.write(dashboard_html)
            logger.info(f"Saved updated dashboard to {updated_dashboard_file}")
        
        return True
    except Exception as e:
        logger.error(f"Error saving dashboard: {str(e)}")
        return False

if __name__ == "__main__":
    main() 