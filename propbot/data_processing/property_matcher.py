#!/usr/bin/env python3
"""
Property Matcher Module

This module provides functionality to find comparable properties based on 
various attributes like location, size, and property type.
It's used to match sales properties with rental properties to calculate yields.
"""

import os
import json
import logging
import math
from typing import Dict, List, Optional, Tuple, Union, Any
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class PropertyMatcher:
    """A class to find comparable properties based on attributes."""
    
    def __init__(self, default_size_range_percent: float = 20, 
                 default_location_weight: float = 0.5,
                 default_size_weight: float = 0.3,
                 default_type_weight: float = 0.2):
        """
        Initialize the property matcher with default parameters.
        
        Args:
            default_size_range_percent: Default percentage range for size matching (e.g., 20 means ±20%)
            default_location_weight: Default weight for location in similarity calculation
            default_size_weight: Default weight for size in similarity calculation
            default_type_weight: Default weight for property type in similarity calculation
        """
        self.default_size_range_percent = default_size_range_percent
        self.default_location_weight = default_location_weight
        self.default_size_weight = default_size_weight
        self.default_type_weight = default_type_weight
        
        logger.info(f"Initialized PropertyMatcher with size range: ±{default_size_range_percent}%, "
                   f"weights: location={default_location_weight}, size={default_size_weight}, type={default_type_weight}")
    
    def filter_by_neighborhood(self, target_property: Dict, candidate_properties: List[Dict], 
                              neighborhood_field: str = 'neighborhood',
                              min_similarity: float = 100) -> List[Dict]:
        """
        Filter properties by neighborhood match.
        
        Args:
            target_property: The target property to find comparables for
            candidate_properties: List of candidate comparable properties
            neighborhood_field: Field name containing neighborhood information
            min_similarity: Minimum neighborhood similarity required (exact match = 100)
            
        Returns:
            List of properties with matching neighborhoods
        """
        # If target doesn't have neighborhood, return all candidates
        if neighborhood_field not in target_property or not target_property[neighborhood_field]:
            logger.debug(f"Target property has no {neighborhood_field}, skipping filter")
            return candidate_properties
        
        target_neighborhood = target_property[neighborhood_field]
        
        # Filter properties by exact neighborhood match
        if min_similarity >= 100:
            filtered = [prop for prop in candidate_properties 
                       if neighborhood_field in prop 
                       and prop[neighborhood_field] == target_neighborhood]
        else:
            # This would be the place to implement partial matching with min_similarity threshold
            # For now, we only implement exact matching
            filtered = [prop for prop in candidate_properties 
                       if neighborhood_field in prop 
                       and prop[neighborhood_field] == target_neighborhood]
        
        logger.debug(f"Filtered {len(candidate_properties)} to {len(filtered)} properties by {neighborhood_field}")
        return filtered
    
    def filter_by_size(self, target_property: Dict, candidate_properties: List[Dict],
                      size_field: str = 'size', range_percent: float = None) -> List[Dict]:
        """
        Filter properties by size within a percentage range of the target.
        
        Args:
            target_property: The target property to find comparables for
            candidate_properties: List of candidate comparable properties
            size_field: Field name containing size information
            range_percent: Percentage range for size matching (e.g., 20 means ±20%)
            
        Returns:
            List of properties with size within range
        """
        # If target doesn't have size, return all candidates
        if size_field not in target_property or target_property[size_field] is None:
            logger.debug(f"Target property has no {size_field}, skipping filter")
            return candidate_properties
        
        # Use default range if not specified
        range_percent = range_percent or self.default_size_range_percent
        
        target_size = target_property[size_field]
        min_size = target_size * (1 - range_percent / 100)
        max_size = target_size * (1 + range_percent / 100)
        
        filtered = [prop for prop in candidate_properties 
                   if size_field in prop 
                   and prop[size_field] is not None
                   and min_size <= prop[size_field] <= max_size]
        
        logger.debug(f"Filtered {len(candidate_properties)} to {len(filtered)} properties by {size_field} "
                    f"(target: {target_size}, range: {min_size}-{max_size})")
        return filtered
    
    def filter_by_property_type(self, target_property: Dict, candidate_properties: List[Dict],
                               type_field: str = 'property_type') -> List[Dict]:
        """
        Filter properties by property type match.
        
        Args:
            target_property: The target property to find comparables for
            candidate_properties: List of candidate comparable properties
            type_field: Field name containing property type information
            
        Returns:
            List of properties with matching property type
        """
        # If target doesn't have property type, return all candidates
        if type_field not in target_property or not target_property[type_field]:
            logger.debug(f"Target property has no {type_field}, skipping filter")
            return candidate_properties
        
        target_type = target_property[type_field]
        
        filtered = [prop for prop in candidate_properties 
                   if type_field in prop 
                   and prop[type_field] == target_type]
        
        logger.debug(f"Filtered {len(candidate_properties)} to {len(filtered)} properties by {type_field}")
        return filtered
    
    def filter_by_rooms(self, target_property: Dict, candidate_properties: List[Dict],
                       rooms_field: str = 'rooms', exact_match: bool = False) -> List[Dict]:
        """
        Filter properties by number of rooms.
        
        Args:
            target_property: The target property to find comparables for
            candidate_properties: List of candidate comparable properties
            rooms_field: Field name containing room information
            exact_match: If True, requires exact room count match, otherwise allows ±1 room
            
        Returns:
            List of properties with matching room count
        """
        # If target doesn't have rooms, return all candidates
        if rooms_field not in target_property or target_property[rooms_field] is None:
            logger.debug(f"Target property has no {rooms_field}, skipping filter")
            return candidate_properties
        
        target_rooms = target_property[rooms_field]
        
        if exact_match:
            filtered = [prop for prop in candidate_properties 
                       if rooms_field in prop 
                       and prop[rooms_field] is not None
                       and prop[rooms_field] == target_rooms]
        else:
            # Allow ±1 room
            min_rooms = target_rooms - 1
            max_rooms = target_rooms + 1
            filtered = [prop for prop in candidate_properties 
                       if rooms_field in prop 
                       and prop[rooms_field] is not None
                       and min_rooms <= prop[rooms_field] <= max_rooms]
        
        logger.debug(f"Filtered {len(candidate_properties)} to {len(filtered)} properties by {rooms_field}")
        return filtered
    
    def calculate_property_similarity(self, target_property: Dict, candidate_property: Dict,
                                    location_weight: float = None,
                                    size_weight: float = None,
                                    type_weight: float = None,
                                    neighborhood_field: str = 'neighborhood',
                                    size_field: str = 'size',
                                    type_field: str = 'property_type') -> float:
        """
        Calculate similarity score between two properties.
        
        Args:
            target_property: The target property
            candidate_property: The candidate property to compare
            location_weight: Weight for location in similarity calculation
            size_weight: Weight for size in similarity calculation
            type_weight: Weight for property type in similarity calculation
            neighborhood_field: Field name containing neighborhood information
            size_field: Field name containing size information
            type_field: Field name containing property type information
            
        Returns:
            Similarity score (0-100)
        """
        # Use default weights if not specified
        location_weight = location_weight or self.default_location_weight
        size_weight = size_weight or self.default_size_weight
        type_weight = type_weight or self.default_type_weight
        
        # Initialize scores for each attribute
        location_score = 0
        size_score = 0
        type_score = 0
        
        # Calculate location similarity
        if (neighborhood_field in target_property and target_property[neighborhood_field] and
            neighborhood_field in candidate_property and candidate_property[neighborhood_field]):
            if target_property[neighborhood_field] == candidate_property[neighborhood_field]:
                location_score = 100
        
        # Calculate size similarity
        if (size_field in target_property and target_property[size_field] is not None and
            size_field in candidate_property and candidate_property[size_field] is not None):
            target_size = target_property[size_field]
            candidate_size = candidate_property[size_field]
            
            # Calculate percentage difference
            size_diff_percent = abs(target_size - candidate_size) / target_size * 100
            
            # Convert to similarity score (100 = identical, 0 = 50% or more different)
            size_score = max(0, 100 - 2 * size_diff_percent)
        
        # Calculate property type similarity
        if (type_field in target_property and target_property[type_field] and
            type_field in candidate_property and candidate_property[type_field]):
            if target_property[type_field] == candidate_property[type_field]:
                type_score = 100
        
        # Calculate weighted similarity score
        total_weight = location_weight + size_weight + type_weight
        similarity = (location_score * location_weight + 
                     size_score * size_weight + 
                     type_score * type_weight) / total_weight
        
        return similarity
    
    def rank_comparables(self, target_property: Dict, filtered_properties: List[Dict],
                        location_weight: float = None,
                        size_weight: float = None,
                        type_weight: float = None) -> List[Tuple[Dict, float]]:
        """
        Rank properties by similarity to the target property.
        
        Args:
            target_property: The target property to find comparables for
            filtered_properties: List of pre-filtered candidate properties
            location_weight: Weight for location in similarity calculation
            size_weight: Weight for size in similarity calculation
            type_weight: Weight for property type in similarity calculation
            
        Returns:
            List of tuples (property, similarity_score) sorted by descending similarity
        """
        # Calculate similarity for each property
        properties_with_scores = []
        
        for prop in filtered_properties:
            similarity = self.calculate_property_similarity(
                target_property, prop,
                location_weight, size_weight, type_weight
            )
            properties_with_scores.append((prop, similarity))
        
        # Sort by descending similarity
        ranked_properties = sorted(properties_with_scores, key=lambda x: x[1], reverse=True)
        
        logger.debug(f"Ranked {len(filtered_properties)} properties by similarity")
        return ranked_properties
    
    def find_comparable_properties(self, target_property: Dict, candidate_properties: List[Dict],
                                  min_similarity: float = 70,
                                  max_results: int = 10,
                                  filter_by_params: Dict = None) -> List[Dict]:
        """
        Find comparable properties to the target property.
        
        Args:
            target_property: The target property to find comparables for
            candidate_properties: List of candidate comparable properties
            min_similarity: Minimum similarity score required (0-100)
            max_results: Maximum number of results to return
            filter_by_params: Dictionary of filter parameters
            
        Returns:
            List of comparable properties sorted by descending similarity
        """
        # Initialize filter parameters
        filter_params = filter_by_params or {}
        
        # Apply filters
        filtered_properties = candidate_properties
        
        # Filter by neighborhood
        if filter_params.get('apply_neighborhood_filter', True):
            filtered_properties = self.filter_by_neighborhood(
                target_property, filtered_properties,
                neighborhood_field=filter_params.get('neighborhood_field', 'neighborhood'),
                min_similarity=filter_params.get('min_neighborhood_similarity', 100)
            )
        
        # Filter by size
        if filter_params.get('apply_size_filter', True):
            filtered_properties = self.filter_by_size(
                target_property, filtered_properties,
                size_field=filter_params.get('size_field', 'size'),
                range_percent=filter_params.get('size_range_percent', self.default_size_range_percent)
            )
        
        # Filter by property type
        if filter_params.get('apply_type_filter', True):
            filtered_properties = self.filter_by_property_type(
                target_property, filtered_properties,
                type_field=filter_params.get('type_field', 'property_type')
            )
        
        # Filter by rooms
        if filter_params.get('apply_rooms_filter', False):
            filtered_properties = self.filter_by_rooms(
                target_property, filtered_properties,
                rooms_field=filter_params.get('rooms_field', 'rooms'),
                exact_match=filter_params.get('exact_room_match', False)
            )
        
        # If no properties remain after filtering, relax filters and try again
        if not filtered_properties and filter_params.get('relax_filters_if_empty', True):
            logger.info("No properties after filtering, relaxing filters")
            
            # Try with only neighborhood filter
            if filter_params.get('apply_neighborhood_filter', True):
                filtered_properties = self.filter_by_neighborhood(
                    target_property, candidate_properties,
                    neighborhood_field=filter_params.get('neighborhood_field', 'neighborhood'),
                    min_similarity=filter_params.get('min_neighborhood_similarity', 100)
                )
            
            # If still no results, try with only size filter
            if not filtered_properties and filter_params.get('apply_size_filter', True):
                filtered_properties = self.filter_by_size(
                    target_property, candidate_properties,
                    size_field=filter_params.get('size_field', 'size'),
                    range_percent=filter_params.get('size_range_percent', self.default_size_range_percent) * 1.5  # Increase range
                )
            
            # If still no results, return a subset of all candidates
            if not filtered_properties:
                logger.warning("No properties after relaxed filtering, using subset of all candidates")
                filtered_properties = candidate_properties[:min(100, len(candidate_properties))]
        
        # Rank properties by similarity
        ranked_properties = self.rank_comparables(
            target_property, filtered_properties,
            location_weight=filter_params.get('location_weight', self.default_location_weight),
            size_weight=filter_params.get('size_weight', self.default_size_weight),
            type_weight=filter_params.get('type_weight', self.default_type_weight)
        )
        
        # Filter by minimum similarity and limit results
        comparable_properties = [
            prop for prop, score in ranked_properties 
            if score >= min_similarity
        ][:max_results]
        
        logger.info(f"Found {len(comparable_properties)} comparable properties "
                   f"out of {len(candidate_properties)} candidates")
        
        return comparable_properties
    
    def find_rental_comparables(self, sales_property: Dict, rental_properties: List[Dict],
                              min_similarity: float = 70,
                              max_results: int = 5,
                              size_range_percent: float = None) -> List[Dict]:
        """
        Find comparable rental properties for a sales property.
        
        Args:
            sales_property: The sales property to find rental comparables for
            rental_properties: List of rental properties
            min_similarity: Minimum similarity score required (0-100)
            max_results: Maximum number of results to return
            size_range_percent: Percentage range for size matching
            
        Returns:
            List of comparable rental properties sorted by descending similarity
        """
        filter_params = {
            'apply_neighborhood_filter': True,
            'apply_size_filter': True,
            'apply_type_filter': True,
            'apply_rooms_filter': True,
            'size_range_percent': size_range_percent or self.default_size_range_percent,
            'relax_filters_if_empty': True
        }
        
        return self.find_comparable_properties(
            sales_property, rental_properties,
            min_similarity=min_similarity,
            max_results=max_results,
            filter_by_params=filter_params
        )


def find_comparable_properties(target_property: Dict, candidate_properties: List[Dict],
                             **kwargs) -> List[Dict]:
    """
    Find comparable properties to the target property.
    
    Args:
        target_property: The target property to find comparables for
        candidate_properties: List of candidate comparable properties
        **kwargs: Additional arguments for PropertyMatcher.find_comparable_properties
        
    Returns:
        List of comparable properties
    """
    matcher = PropertyMatcher()
    return matcher.find_comparable_properties(target_property, candidate_properties, **kwargs)


def find_rental_comparables(sales_property: Dict, rental_properties: List[Dict],
                          **kwargs) -> List[Dict]:
    """
    Find comparable rental properties for a sales property.
    
    Args:
        sales_property: The sales property to find rental comparables for
        rental_properties: List of rental properties
        **kwargs: Additional arguments for PropertyMatcher.find_rental_comparables
        
    Returns:
        List of comparable rental properties
    """
    matcher = PropertyMatcher()
    return matcher.find_rental_comparables(sales_property, rental_properties, **kwargs)


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/data_processing/property_matcher.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Find comparable properties')
    parser.add_argument('target_file', help='Path to target property JSON file')
    parser.add_argument('candidates_file', help='Path to candidate properties JSON file')
    parser.add_argument('--output', help='Path to output JSON file')
    parser.add_argument('--id', help='ID of the target property in the target file')
    
    args = parser.parse_args()
    
    # Load target property
    with open(args.target_file, 'r', encoding='utf-8') as f:
        target_data = json.load(f)
    
    if isinstance(target_data, list):
        if args.id:
            target_property = next((p for p in target_data if p.get('id') == args.id), target_data[0])
        else:
            target_property = target_data[0]
    else:
        target_property = target_data
    
    # Load candidate properties
    with open(args.candidates_file, 'r', encoding='utf-8') as f:
        candidate_properties = json.load(f)
    
    # Find comparable properties
    comparable_properties = find_comparable_properties(target_property, candidate_properties)
    
    # Save results
    if args.output:
        output_data = {
            'target_property': target_property,
            'comparable_properties': comparable_properties
        }
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(comparable_properties)} comparable properties to {args.output}")
    else:
        # Print results
        print(f"Found {len(comparable_properties)} comparable properties")
        for i, prop in enumerate(comparable_properties, 1):
            print(f"{i}. {prop.get('title', 'Unnamed')} - {prop.get('neighborhood', 'Unknown')}, "
                 f"{prop.get('size', 'Unknown')} m², {prop.get('price', 'Unknown')} €") 