#!/usr/bin/env python3
"""
Environment Variable Loader

This module loads environment variables from .env files.
It should be imported at the beginning of any script that needs environment variables.
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Print current working directory for debugging
current_dir = os.getcwd()
logger.info(f"Current working directory: {current_dir}")

# Look for .env file in multiple locations
env_paths = [
    '.env',                          # Current directory
    '../.env',                       # Parent directory
    '../../.env',                    # Grandparent directory
    f"{current_dir}/.env",           # Absolute path to current directory
    f"{os.path.dirname(current_dir)}/.env"  # Absolute path to parent directory
]

env_file = None
for path in env_paths:
    if os.path.exists(path):
        env_file = path
        logger.info(f"Found .env file at: {os.path.abspath(path)}")
        break

if env_file:
    # Load environment variables from .env file
    load_dotenv(env_file)
    logger.info(f"Loaded environment variables from {env_file}")
else:
    # Try using find_dotenv as a fallback
    dotenv_path = find_dotenv(usecwd=True)
    if dotenv_path:
        logger.info(f"Found .env file using find_dotenv at: {dotenv_path}")
        load_dotenv(dotenv_path)
    else:
        logger.warning("Could not find .env file in any of the searched locations")

# Verify DATABASE_URL is set
if not os.environ.get('DATABASE_URL'):
    logger.warning("DATABASE_URL environment variable is not set. Database connections will fail.")
else:
    logger.info("DATABASE_URL environment variable is set.")

# Export a function to reload environment variables if needed
def reload_env():
    """Reload environment variables from .env file"""
    if env_file:
        load_dotenv(env_file, override=True)
    elif find_dotenv(usecwd=True):
        load_dotenv(find_dotenv(usecwd=True), override=True)
    else:
        logger.warning("Could not find .env file to reload")
        
    if not os.environ.get('DATABASE_URL'):
        logger.warning("DATABASE_URL environment variable is not set after reload. Database connections will fail.")
    else:
        logger.info("DATABASE_URL environment variable is set after reload.") 