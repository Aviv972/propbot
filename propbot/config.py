#!/usr/bin/env python3
"""
Configuration file for PropBot.
Contains file paths, settings, and constants used throughout the application.
"""

import os
from pathlib import Path
from datetime import datetime

# Project root directory
ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Data directories
DATA_DIR = ROOT_DIR / "propbot" / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
PROCESSED_DIR = PROCESSED_DATA_DIR  # Alias for backward compatibility
OUTPUT_DIR = DATA_DIR / "output"
REPORTS_DIR = DATA_DIR / "reports"

# UI directory
UI_DIR = ROOT_DIR / "propbot" / "ui"

# Ensure directories exist
for directory in [RAW_DATA_DIR / "sales", RAW_DATA_DIR / "rentals", 
                 PROCESSED_DATA_DIR, OUTPUT_DIR, REPORTS_DIR, UI_DIR]:
    os.makedirs(directory, exist_ok=True)

# Raw data files
SALES_RAW_FILE = RAW_DATA_DIR / "sales" / "idealista_listings.json"
RENTAL_RAW_FILE = RAW_DATA_DIR / "rentals" / "rental_listings.json"

# Processed data files
CURRENT_MONTH = datetime.now().strftime("%Y-%m")
RENTAL_PROCESSED_FILE = PROCESSED_DATA_DIR / f"rental_data_{CURRENT_MONTH}.csv"
INVESTMENT_METRICS_FILE = PROCESSED_DATA_DIR / "investment_metrics.csv"
NEIGHBORHOOD_STATS_FILE = PROCESSED_DATA_DIR / "neighborhood_stats.json"
EXPENSE_REPORT_FILE = PROCESSED_DATA_DIR / "property_expense_report.json"

# Output report files
INVESTMENT_SUMMARY_HTML = UI_DIR / "investment_summary.html"
INVESTMENT_DASHBOARD_HTML = UI_DIR / "investment_dashboard.html"
NEIGHBORHOOD_REPORT_HTML = UI_DIR / "neighborhood_report.html"

# Investment metrics thresholds
HIGH_POTENTIAL_CAP_RATE = 6.0
MEDIUM_POTENTIAL_CAP_RATE = 4.5
LOW_POTENTIAL_CAP_RATE = 0.0

# Backup settings
def get_backup_path(filename):
    """Get a timestamped backup path for a file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{filename}.bak_{timestamp}"

# Logger settings
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = ROOT_DIR / "propbot.log"

# Function to check if a month has passed since last update
def should_update_rental_data():
    """Check if rental data should be updated based on last update time."""
    last_run_file = ROOT_DIR / "last_rental_run.txt"
    
    try:
        if not last_run_file.exists():
            return True
            
        with open(last_run_file, 'r') as f:
            last_run = float(f.read().strip())
            # Check if it's been more than 30 days
            return (datetime.now().timestamp() - last_run) >= (30 * 24 * 60 * 60)
    except Exception:
        return True  # If any error, assume update is needed

# Function to update the last rental run timestamp
def update_last_rental_run():
    """Update the timestamp for the last rental data update."""
    last_run_file = ROOT_DIR / "last_rental_run.txt"
    
    with open(last_run_file, 'w') as f:
        f.write(str(datetime.now().timestamp()))

def check_rental_data_needs_update():
    """Check if rental data needs to be updated (more than 30 days since last update).
    
    Returns:
        bool: True if rental data needs to be updated, False otherwise
    """
    import os
    import time
    from datetime import datetime, timedelta
    
    # Path to the rental data file
    rental_data_path = os.path.join(DATA_DIR, "processed", "rental_data.csv")
    
    # If file doesn't exist, definitely need to update
    if not os.path.exists(rental_data_path):
        return True
    
    # Get file modification time
    file_mod_time = os.path.getmtime(rental_data_path)
    mod_date = datetime.fromtimestamp(file_mod_time)
    
    # Check if it's been more than 30 days
    current_date = datetime.now()
    days_since_update = (current_date - mod_date).days
    
    return days_since_update >= 30 