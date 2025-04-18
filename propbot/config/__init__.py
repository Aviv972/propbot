"""
PropBot Configuration Module

This module handles loading and managing PropBot configurations.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

# Set up logging
logger = logging.getLogger(__name__)

# Default configuration paths
DEFAULT_CONFIG_DIR = os.path.expanduser("~/.propbot")
CONFIG_PATH = os.path.join(DEFAULT_CONFIG_DIR, "config.json")

# Default configuration
DEFAULT_CONFIG = {
    "data_dir": "data",
    "raw_dir": "data/raw",
    "processed_dir": "data/processed",
    "logs_dir": "data/logs",
    "scraping": {
        "idealista_api_key": "",
        "idealista_secret": "",
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "max_pages": 20,
        "rate_limit_seconds": 2
    },
    "analysis": {
        "mortgage_interest_rate": 3.0,
        "mortgage_term_years": 30,
        "down_payment_percent": 20,
        "monthly_expenses_percent": 10,
        "rental_vacancy_percent": 5,
        "property_appreciation_percent": 2,
        "income_tax_rate_percent": 24
    },
    "validation": {
        "min_price": 50000,
        "max_price": 1000000,
        "min_size": 30,
        "max_size": 500,
        "min_rooms": 1,
        "max_rooms": 10
    }
}

def ensure_config_dir():
    """Ensure the config directory exists."""
    os.makedirs(DEFAULT_CONFIG_DIR, exist_ok=True)

def create_default_config():
    """Create the default configuration file."""
    ensure_config_dir()
    
    with open(CONFIG_PATH, 'w', encoding='utf-8') as f:
        json.dump(DEFAULT_CONFIG, f, indent=2)
    
    logger.info(f"Created default configuration at {CONFIG_PATH}")
    return DEFAULT_CONFIG

def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load the PropBot configuration.
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        Configuration dictionary
    """
    # Use default path if not specified
    if not config_path:
        config_path = CONFIG_PATH
    
    # Check if configuration file exists
    if not os.path.exists(config_path):
        logger.warning(f"Configuration file not found at {config_path}")
        
        # Use the default config path and create it if it's the default
        if config_path == CONFIG_PATH:
            return create_default_config()
        else:
            logger.error(f"Cannot find configuration at {config_path}")
            return DEFAULT_CONFIG
    
    # Load the configuration
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        logger.info(f"Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return DEFAULT_CONFIG

def save_config(config: Dict[str, Any], config_path: Optional[str] = None) -> bool:
    """
    Save the PropBot configuration.
    
    Args:
        config: Configuration dictionary
        config_path: Path to save the configuration file
        
    Returns:
        True if saved successfully, False otherwise
    """
    # Use default path if not specified
    if not config_path:
        config_path = CONFIG_PATH
        ensure_config_dir()
    
    # Save the configuration
    try:
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        
        logger.info(f"Saved configuration to {config_path}")
        return True
    except Exception as e:
        logger.error(f"Error saving configuration: {e}")
        return False

def update_config(updates: Dict[str, Any], config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Update the PropBot configuration with new values.
    
    Args:
        updates: Dictionary with configuration updates
        config_path: Path to the configuration file
        
    Returns:
        Updated configuration dictionary
    """
    # Load the current configuration
    config = load_config(config_path)
    
    # Update recursively
    def update_dict(original, updates):
        for key, value in updates.items():
            if key in original and isinstance(original[key], dict) and isinstance(value, dict):
                update_dict(original[key], value)
            else:
                original[key] = value
    
    update_dict(config, updates)
    
    # Save the updated configuration
    save_config(config, config_path)
    
    return config

# Initialize configuration
if not os.path.exists(CONFIG_PATH):
    create_default_config()

# Determine the project base directory
BASE_DIR = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Define data directories
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
REPORTS_DIR = DATA_DIR / "reports"
OUTPUT_DIR = DATA_DIR / "output"

# Function to ensure directories exist, creating parents if necessary
def ensure_directories_exist(directories):
    """
    Ensure all specified directories exist, creating parent directories if needed.
    
    Args:
        directories: List of Path objects representing directories to create
    """
    for directory in directories:
        try:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured directory exists: {directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {directory}: {e}")

# Create required directories
ensure_directories_exist([
    DATA_DIR,
    RAW_DATA_DIR,
    RAW_DATA_DIR / "sales",
    RAW_DATA_DIR / "sales" / "history",
    RAW_DATA_DIR / "rentals",
    RAW_DATA_DIR / "rentals" / "history",
    PROCESSED_DATA_DIR,
    REPORTS_DIR,
    OUTPUT_DIR
])

# UI directory - handle differently since it's not in the data directory
UI_DIR = None  # Initialize as None first

# Try possible locations in order of preference
possible_ui_paths = [
    BASE_DIR / "ui",                 # Try the base/ui path first
    DATA_DIR / "ui",                 # Try data/ui as fallback
    Path("/app/propbot/ui"),         # Try absolute path as another fallback
    Path("/tmp/propbot_ui")          # Last resort - use temp directory
]

for path in possible_ui_paths:
    try:
        # Try to create the directory
        path.mkdir(parents=True, exist_ok=True)
        # If we get here, the creation succeeded
        UI_DIR = path
        logger.info(f"Successfully created UI directory at: {UI_DIR}")
        break
    except Exception as e:
        logger.warning(f"Failed to create UI directory at {path}: {e}")

if UI_DIR is None:
    # If all attempts failed, raise an error
    logger.error("CRITICAL: Failed to create UI directory at any location")
    # Use a guaranteed writable location as last resort
    UI_DIR = Path("/tmp")
    UI_DIR.mkdir(exist_ok=True)
    logger.warning(f"Using system /tmp directory as last resort: {UI_DIR}")

# Load the configuration
CONFIG = load_config()

# Neighborhood Report paths - use UI_DIR variable instead of hardcoded paths
NEIGHBORHOOD_REPORT_HTML = os.path.join(UI_DIR, "neighborhood_report_updated.html") 