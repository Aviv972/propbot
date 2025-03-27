#!/usr/bin/env python3
"""
Incremental Update Module

This module handles incremental updates to property data, allowing for efficient
updates without reprocessing the entire dataset each time.
"""

import os
import json
import logging
import time
import hashlib
from typing import Dict, List, Set, Tuple, Optional, Any
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logger = logging.getLogger(__name__)

class IncrementalUpdater:
    """A class to handle incremental updates to property data."""
    
    def __init__(self, registry_file: str = None):
        """
        Initialize the incremental updater.
        
        Args:
            registry_file: Path to the property registry file. If None, uses default.
        """
        self.registry_file = registry_file or "propbot/data/processed/property_registry.json"
        self.registry = self.load_registry()
        logger.info(f"Initialized IncrementalUpdater with {len(self.registry)} registered properties")
    
    def load_registry(self) -> Dict:
        """
        Load the property registry from file.
        
        Returns:
            Dictionary of registered properties with metadata
        """
        try:
            registry_path = Path(self.registry_file)
            if registry_path.exists():
                with open(registry_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                # Create a new registry
                registry = {
                    "properties": {},
                    "last_updated": datetime.now().isoformat(),
                    "metadata": {
                        "version": "1.0",
                        "total_updates": 0
                    }
                }
                
                # Ensure directory exists
                registry_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Save empty registry
                with open(registry_path, 'w', encoding='utf-8') as f:
                    json.dump(registry, f, ensure_ascii=False, indent=2)
                
                return registry
        except Exception as e:
            logger.error(f"Error loading property registry: {e}")
            # Return an empty registry
            return {
                "properties": {},
                "last_updated": datetime.now().isoformat(),
                "metadata": {
                    "version": "1.0",
                    "total_updates": 0
                }
            }
    
    def save_registry(self):
        """Save the property registry to file."""
        try:
            # Update last_updated timestamp
            self.registry["last_updated"] = datetime.now().isoformat()
            
            # Increment total_updates
            self.registry["metadata"]["total_updates"] += 1
            
            # Save registry
            registry_path = Path(self.registry_file)
            registry_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(registry_path, 'w', encoding='utf-8') as f:
                json.dump(self.registry, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved property registry with {len(self.registry['properties'])} properties")
        except Exception as e:
            logger.error(f"Error saving property registry: {e}")
    
    def generate_property_id(self, property_data: Dict) -> str:
        """
        Generate a unique ID for a property based on its attributes.
        
        Args:
            property_data: Property data dictionary
            
        Returns:
            Unique property ID
        """
        # Extract key attributes to generate a unique ID
        # The URL is the most reliable unique identifier if available
        if 'url' in property_data and property_data['url']:
            return hashlib.md5(property_data['url'].encode('utf-8')).hexdigest()
        
        # Otherwise, combine multiple attributes to create a unique fingerprint
        key_attributes = []
        
        # Primary attributes
        if 'title' in property_data and property_data['title']:
            key_attributes.append(str(property_data['title']))
        
        if 'location' in property_data and property_data['location']:
            key_attributes.append(str(property_data['location']))
        
        if 'price' in property_data and property_data['price'] is not None:
            key_attributes.append(str(property_data['price']))
        
        if 'size' in property_data and property_data['size'] is not None:
            key_attributes.append(str(property_data['size']))
        
        # If we have enough primary attributes, use them to generate ID
        if len(key_attributes) >= 3:
            fingerprint = "|".join(key_attributes)
            return hashlib.md5(fingerprint.encode('utf-8')).hexdigest()
        
        # As a last resort, use the entire property data as a JSON string
        # (not ideal for performance, but ensures uniqueness)
        property_str = json.dumps(property_data, sort_keys=True)
        return hashlib.md5(property_str.encode('utf-8')).hexdigest()
    
    def is_property_changed(self, property_id: str, new_property: Dict) -> bool:
        """
        Check if a property has changed compared to its registered version.
        
        Args:
            property_id: Property ID in the registry
            new_property: New property data dictionary
            
        Returns:
            True if the property has changed, False otherwise
        """
        if property_id not in self.registry['properties']:
            return True  # New property
        
        registered_property = self.registry['properties'][property_id]['data']
        
        # Check key attributes for changes
        key_attributes = ['price', 'size', 'description', 'title']
        
        for attr in key_attributes:
            if attr in new_property and attr in registered_property:
                if new_property[attr] != registered_property[attr]:
                    return True
            elif attr in new_property or attr in registered_property:
                return True  # Attribute present in one but not the other
        
        return False
    
    def register_property(self, property_data: Dict, property_id: str = None) -> str:
        """
        Register a property in the registry.
        
        Args:
            property_data: Property data dictionary
            property_id: Optional property ID. If None, generates a new ID.
            
        Returns:
            Property ID
        """
        # Generate ID if not provided
        if property_id is None:
            property_id = self.generate_property_id(property_data)
        
        # Initialize registry entry if it doesn't exist
        if property_id not in self.registry['properties']:
            self.registry['properties'][property_id] = {
                "first_seen": datetime.now().isoformat(),
                "update_history": [],
                "data": {}
            }
        
        # Check if property has changed
        is_changed = self.is_property_changed(property_id, property_data)
        
        if is_changed:
            # Record update in history
            previous_data = self.registry['properties'][property_id]['data']
            
            # Record only the changes
            changes = {}
            for key, value in property_data.items():
                if key not in previous_data or previous_data[key] != value:
                    changes[key] = value
            
            # Add update to history
            self.registry['properties'][property_id]['update_history'].append({
                "timestamp": datetime.now().isoformat(),
                "changes": changes
            })
            
            # Limit history length to avoid registry bloat
            if len(self.registry['properties'][property_id]['update_history']) > 10:
                self.registry['properties'][property_id]['update_history'] = \
                    self.registry['properties'][property_id]['update_history'][-10:]
            
            # Update the data
            self.registry['properties'][property_id]['data'] = property_data
            self.registry['properties'][property_id]['last_updated'] = datetime.now().isoformat()
        
        return property_id
    
    def register_properties(self, properties: List[Dict]) -> Dict:
        """
        Register multiple properties in the registry.
        
        Args:
            properties: List of property data dictionaries
            
        Returns:
            Statistics about the registration process
        """
        start_time = time.time()
        registered_count = 0
        updated_count = 0
        unchanged_count = 0
        
        for prop in properties:
            property_id = self.generate_property_id(prop)
            
            # Check if property already exists
            is_new = property_id not in self.registry['properties']
            
            # Register property
            self.register_property(prop, property_id)
            
            # Update counts
            if is_new:
                registered_count += 1
            elif self.is_property_changed(property_id, prop):
                updated_count += 1
            else:
                unchanged_count += 1
        
        # Save registry
        self.save_registry()
        
        # Generate statistics
        stats = {
            "total_properties": len(properties),
            "registered_properties": registered_count,
            "updated_properties": updated_count,
            "unchanged_properties": unchanged_count,
            "processing_time": time.time() - start_time
        }
        
        logger.info(f"Registered {registered_count} new properties, updated {updated_count}, "
                   f"unchanged: {unchanged_count}")
        
        return stats
    
    def get_new_and_updated_properties(self, properties: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Separate new and updated properties from a list.
        
        Args:
            properties: List of property data dictionaries
            
        Returns:
            Tuple (new_properties, updated_properties)
        """
        new_properties = []
        updated_properties = []
        
        for prop in properties:
            property_id = self.generate_property_id(prop)
            
            if property_id not in self.registry['properties']:
                new_properties.append(prop)
            elif self.is_property_changed(property_id, prop):
                updated_properties.append(prop)
        
        return new_properties, updated_properties
    
    def process_incremental_update(self, input_file: str, output_dir: str = None) -> Dict:
        """
        Process an incremental update from a file.
        
        Args:
            input_file: Path to input JSON file containing new property data
            output_dir: Directory to save output files. If None, uses default.
            
        Returns:
            Statistics about the update process
        """
        try:
            # Load new properties
            with open(input_file, 'r', encoding='utf-8') as f:
                new_data = json.load(f)
            
            # Ensure properties is a list
            if isinstance(new_data, dict) and 'properties' in new_data:
                properties = new_data['properties']
            elif isinstance(new_data, list):
                properties = new_data
            else:
                properties = [new_data]  # Single property
            
            # Split into new and updated properties
            new_properties, updated_properties = self.get_new_and_updated_properties(properties)
            
            # Register all properties
            registration_stats = self.register_properties(properties)
            
            # Setup output directory
            if output_dir is None:
                output_dir = Path("propbot/data/processed/incremental")
            else:
                output_dir = Path(output_dir)
            
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Save new and updated properties to separate files
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if new_properties:
                new_file = output_dir / f"new_properties_{timestamp}.json"
                with open(new_file, 'w', encoding='utf-8') as f:
                    json.dump(new_properties, f, ensure_ascii=False, indent=2)
            
            if updated_properties:
                updated_file = output_dir / f"updated_properties_{timestamp}.json"
                with open(updated_file, 'w', encoding='utf-8') as f:
                    json.dump(updated_properties, f, ensure_ascii=False, indent=2)
            
            # Generate statistics
            stats = {
                "input_file": input_file,
                "timestamp": timestamp,
                "total_properties": len(properties),
                "new_properties": len(new_properties),
                "updated_properties": len(updated_properties),
                "unchanged_properties": len(properties) - len(new_properties) - len(updated_properties),
                "new_file": str(new_file) if new_properties else None,
                "updated_file": str(updated_file) if updated_properties else None
            }
            
            logger.info(f"Processed incremental update from {input_file}: "
                       f"{stats['new_properties']} new, {stats['updated_properties']} updated")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error processing incremental update from {input_file}: {e}")
            return {
                "input_file": input_file,
                "error": str(e),
                "success": False
            }
    
    def get_recently_changed_properties(self, days: int = 7) -> List[Dict]:
        """
        Get properties that changed recently.
        
        Args:
            days: Number of days to look back for changes
            
        Returns:
            List of recently changed property data dictionaries
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()
        
        recent_properties = []
        
        for property_id, prop_info in self.registry['properties'].items():
            if 'last_updated' in prop_info and prop_info['last_updated'] > cutoff_str:
                recent_properties.append(prop_info['data'])
        
        logger.info(f"Found {len(recent_properties)} properties changed in the last {days} days")
        return recent_properties


def update_property_data(input_file: str, output_dir: str = None, registry_file: str = None) -> Dict:
    """
    Process an incremental update to property data.
    
    Args:
        input_file: Path to input JSON file containing new property data
        output_dir: Directory to save output files. If None, uses default.
        registry_file: Path to the property registry file. If None, uses default.
        
    Returns:
        Statistics about the update process
    """
    updater = IncrementalUpdater(registry_file)
    return updater.process_incremental_update(input_file, output_dir)


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/data_processing/incremental_updater.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Process incremental property data updates')
    parser.add_argument('input_file', help='Path to input JSON file containing new property data')
    parser.add_argument('--output_dir', help='Directory to save output files')
    parser.add_argument('--registry', help='Path to the property registry file')
    
    args = parser.parse_args()
    
    result = update_property_data(args.input_file, args.output_dir, args.registry)
    print(json.dumps(result, indent=2)) 