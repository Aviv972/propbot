"""
PropBot: Real Estate Investment Analysis Tool

This package provides tools for scraping, analyzing, and visualizing 
property investment opportunities.
"""

import os
import logging
from pathlib import Path

# Ensure all required directories exist
def initialize_directories():
    """Create necessary directories for PropBot data and outputs."""
    # Project root is the parent directory of the propbot package
    root_dir = Path(__file__).parent.parent
    
    # Define required directories
    directories = [
        root_dir / "propbot" / "data" / "raw" / "sales",
        root_dir / "propbot" / "data" / "raw" / "rentals",
        root_dir / "propbot" / "data" / "processed",
        root_dir / "propbot" / "logs",
        root_dir / "propbot" / "ui",
        root_dir / "propbot" / "backups"
    ]
    
    # Create all directories
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

# Initialize directories when the package is imported
initialize_directories()

# Set up logging
# Use simple default logging configuration instead of importing from config
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = Path(__file__).parent / 'logs' / 'propbot.log'

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

__version__ = '1.0.0' 