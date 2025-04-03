"""
Utility functions for PropBot data processing.
"""

import json
from pathlib import Path
from decimal import Decimal
from datetime import datetime
from typing import Any, Dict, List, Union, Optional

class PathJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that can handle Path objects and Decimal objects.
    
    This encoder converts:
    - Path objects to strings
    - Decimal objects to float
    - datetime objects to ISO format strings
    """
    def default(self, obj):
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def save_json(data: Any, file_path: Union[str, Path], indent: int = 2) -> bool:
    """
    Save data to a JSON file with proper error handling.
    
    Args:
        data: Data to save
        file_path: Path to save the file
        indent: Indentation level for pretty-printing
        
    Returns:
        True if successful, False if failed
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, cls=PathJSONEncoder, ensure_ascii=False)
        return True
    except Exception as e:
        logger.error(f"Error saving JSON file: {e}")
        return False

def load_json(file_path: Union[str, Path]) -> Optional[Any]:
    """
    Load data from a JSON file with proper error handling.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Loaded data or None if failed
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading JSON file: {e}")
        return None 