#!/usr/bin/env python3
"""
Location Matching Module

This module provides functionality to standardize location names from property listings.
It uses a mapping dictionary and fuzzy matching to normalize location data.
"""

import os
import json
import logging
import re
from typing import Dict, List, Optional, Set, Tuple
from pathlib import Path
from fuzzywuzzy import fuzz, process

# Configure logging
logger = logging.getLogger(__name__)

# Default list of Lisbon neighborhoods
DEFAULT_NEIGHBORHOODS = [
    "Ajuda", "Alcântara", "Alvalade", "Areeiro", "Arroios", "Avenidas Novas", "Beato", 
    "Belém", "Benfica", "Campo de Ourique", "Campolide", "Carnide", "Estrela", 
    "Lumiar", "Marvila", "Misericórdia", "Olivais", "Parque das Nações", "Penha de França", 
    "Santa Clara", "Santa Maria Maior", "Santo António", "São Domingos de Benfica", 
    "São Vicente", "Ajuda", "Alcântara", "Alfama", "Alto do Pina", "Alvalade", "Anjos", 
    "Avenida", "Avenida da Liberdade", "Avenida Almirante Reis", "Baixa", "Bairro Alto", 
    "Belém", "Benfica", "Cais do Sodré", "Campo de Ourique", "Castelo", "Chiado", "Graça", "Intendente",
    "Lapa", "Marquês de Pombal", "Martim Moniz", "Mouraria", "Príncipe Real", "Restelo", "Rossio",
    "Santos", "Saldanha", "São Bento", "Telheiras", "Bica", "Centro", "Madragoa", "Prazeres"
]

# Mapping of common misspellings or alternate names to standardized names
DEFAULT_LOCATION_MAPPING = {
    # Basic normalizations
    "belem": "Belém",
    "graca": "Graça",
    "principe real": "Príncipe Real",
    "parque nacoes": "Parque das Nações",
    "parque das nacoes": "Parque das Nações",
    "sao vicente": "São Vicente",
    "sao bento": "São Bento",
    "sao domingos de benfica": "São Domingos de Benfica",
    "avenidas novas": "Avenidas Novas",
    "santa maria maior": "Santa Maria Maior",
    "penha de franca": "Penha de França",
    "cais sodre": "Cais do Sodré",
    "marques pombal": "Marquês de Pombal",
    "marques de pombal": "Marquês de Pombal",
    "campo ourique": "Campo de Ourique",
    "avenida liberdade": "Avenida da Liberdade",
    "av liberdade": "Avenida da Liberdade",
    
    # Enhanced mapping from location improvements
    # Municipality areas
    'centro': 'Centro',
    'centro histórico': 'Centro',
    'histórico': 'Centro',
    
    # Specific neighborhoods
    'anjos': 'Anjos',
    'arroios': 'Arroios',
    'anjos arroios': 'Arroios',
    'pena arroios': 'Arroios',
    'pena': 'Arroios',
    
    'alfama': 'Alfama',
    'alfama santa maria maior': 'Alfama',
    'santa maria maior': 'Alfama',
    
    'ajuda': 'Ajuda',
    'centro ajuda': 'Ajuda',
    'boa hora ajuda': 'Ajuda',
    'alto da ajuda ajuda': 'Ajuda',
    'alto da ajuda': 'Ajuda',
    'calçada da ajuda': 'Ajuda',
    'belém ajuda': 'Ajuda',
    
    'alcântara': 'Alcântara',
    'largo do calvário': 'Alcântara',
    'largo do calvário lx factory alcântara': 'Alcântara',
    'lx factory': 'Alcântara',
    'lx factory alcântara': 'Alcântara',
    'alto de alcântara': 'Alcântara', 
    'alto de alcântara alcântara': 'Alcântara',
    'alvito': 'Alcântara',
    'alvito quinta do jacinto alcântara': 'Alcântara',
    'quinta do jacinto': 'Alcântara',
    
    'alvalade': 'Alvalade',
    'são joão de brito': 'Alvalade',
    'são joão de brito alvalade': 'Alvalade',
    
    'areeiro': 'Areeiro',
    'bairro dos actores': 'Areeiro',
    'bairro dos actores areeiro': 'Areeiro',
    'barão sabrosa': 'Areeiro',
    
    'avenidas novas': 'Avenidas Novas',
    'entrecampos': 'Avenidas Novas',
    'entrecampos avenidas novas': 'Avenidas Novas',
    
    'baixa': 'Baixa',
    'baixa chiado': 'Baixa',
    'chiado': 'Baixa',
    'rossio': 'Baixa',
    'rossio martim moniz': 'Baixa',
    'martim moniz': 'Baixa',
    
    'beato': 'Beato',
    
    'belém': 'Belém',
    'centro belém': 'Belém',
    
    'benfica': 'Benfica',
    'portas de benfica': 'Benfica',
    'mercado de benfica': 'Benfica',
    'portas de benfica mercado de benfica benfica': 'Benfica',
    'estrada de benfica': 'Benfica',
    'arneiros': 'Benfica',
    'arneiros benfica': 'Benfica',
    'bairro de santa cruz': 'Benfica',
    'bairro de santa cruz benfica': 'Benfica',
    
    'bica': 'Bica',
    'bica misericórdia': 'Bica',
    'misericórdia': 'Bica',
    'santa catarina': 'Bica',
    'santa catarina misericórdia': 'Bica',
    
    'campo de ourique': 'Campo de Ourique',
    'centro campo de ourique': 'Campo de Ourique',
    'santa isabel': 'Campo de Ourique',
    'santa isabel campo de ourique': 'Campo de Ourique',
    'prazeres': 'Campo de Ourique',
    'prazeres estrela': 'Campo de Ourique',
    'prazeres maria pia': 'Campo de Ourique',
    'prazeres maria pia campo de ourique': 'Campo de Ourique',
    'maria pia': 'Campo de Ourique',
    
    'campolide': 'Campolide',
    'centro campolide': 'Campolide',
    'bairro da serafina': 'Campolide',
    'bairro da serafina campolide': 'Campolide',
    'praça de espanha': 'Campolide',
    'praça de espanha sete rios campolide': 'Campolide',
    'sete rios': 'Campolide',
    'bairro calçada dos mestres': 'Campolide',
    
    'carnide': 'Carnide',
    'bairro novo': 'Carnide',
    'bairro novo carnide': 'Carnide',
    
    'estrela': 'Estrela',
    'lapa': 'Estrela',
    'lapa estrela': 'Estrela',
    'santos o velho': 'Estrela',
    'madragoa': 'Estrela',
    'santos o velho madragoa estrela': 'Estrela',
    
    'graça': 'Graça',
    'são vicente': 'Graça',
    'graça são vicente': 'Graça',
    
    'marvila': 'Marvila',
    
    'olivais': 'Olivais',
    
    'parque das nações': 'Parque das Nações',
    
    'penha de frança': 'Penha de França',
    'centro penha de frança': 'Penha de França',
    'barão sabrosa penha de frança': 'Penha de França',
    
    'santa clara': 'Santa Clara',
    
    'são domingos de benfica': 'São Domingos de Benfica'
}

class LocationMatcher:
    """A class to standardize location names using mapping and fuzzy matching."""
    
    def __init__(self, neighborhoods: List[str] = None, location_mapping: Dict[str, str] = None, 
                 config_file: str = None, fuzzy_threshold: int = 85):
        """
        Initialize the location matcher with neighborhoods and mapping.
        
        Args:
            neighborhoods: List of standardized neighborhood names. If None, uses default list.
            location_mapping: Mapping of alternate names to standardized names. If None, uses default mapping.
            config_file: Path to a JSON config file with neighborhoods and mapping. If provided, overrides other arguments.
            fuzzy_threshold: Threshold for fuzzy matching (0-100). Higher values require closer matches.
        """
        # Try to load from config file first
        if config_file and os.path.exists(config_file):
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.neighborhoods = config.get('neighborhoods', DEFAULT_NEIGHBORHOODS)
                self.location_mapping = config.get('location_mapping', DEFAULT_LOCATION_MAPPING)
                logger.info(f"Loaded location configuration from {config_file}")
            except Exception as e:
                logger.error(f"Error loading config file {config_file}: {e}")
                self.neighborhoods = neighborhoods or DEFAULT_NEIGHBORHOODS
                self.location_mapping = location_mapping or DEFAULT_LOCATION_MAPPING
        else:
            self.neighborhoods = neighborhoods or DEFAULT_NEIGHBORHOODS
            self.location_mapping = location_mapping or DEFAULT_LOCATION_MAPPING
        
        self.fuzzy_threshold = fuzzy_threshold
        logger.info(f"Initialized LocationMatcher with {len(self.neighborhoods)} neighborhoods and "
                   f"{len(self.location_mapping)} mappings")
    
    def normalize_text(self, text: str) -> str:
        """
        Normalize text by removing accents, converting to lowercase, and removing special characters.
        
        Args:
            text: Text to normalize
            
        Returns:
            Normalized text
        """
        if not text:
            return ""
            
        # Convert to lowercase
        text = text.lower()
        
        # Remove accents (simplistic approach, could be improved)
        replacements = {
            'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a',
            'é': 'e', 'è': 'e', 'ê': 'e',
            'í': 'i', 'ì': 'i', 'î': 'i',
            'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o',
            'ú': 'u', 'ù': 'u', 'û': 'u',
            'ç': 'c'
        }
        for accent, replacement in replacements.items():
            text = text.replace(accent, replacement)
        
        # Remove special characters and extra spaces
        text = re.sub(r'[^\w\s]', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def extract_neighborhoods(self, location_text: str) -> Set[str]:
        """
        Extract potential neighborhood terms from a location string.
        
        Args:
            location_text: Location string to extract neighborhoods from
            
        Returns:
            Set of potential neighborhood terms
        """
        if not location_text:
            return set()
            
        # Normalize text
        normalized_text = self.normalize_text(location_text)
        
        # Split into words and filter out short words
        words = normalized_text.split()
        filtered_words = [word for word in words if len(word) > 2]
        
        # Create potential phrases (1-3 words)
        phrases = set()
        for i in range(len(filtered_words)):
            phrases.add(filtered_words[i])
            if i < len(filtered_words) - 1:
                phrases.add(f"{filtered_words[i]} {filtered_words[i+1]}")
            if i < len(filtered_words) - 2:
                phrases.add(f"{filtered_words[i]} {filtered_words[i+1]} {filtered_words[i+2]}")
        
        return phrases
    
    def match_location(self, location_text: str) -> Optional[str]:
        """
        Match a location string to a standardized neighborhood name.
        
        Args:
            location_text: Location string to match
            
        Returns:
            Standardized neighborhood name, or None if no match found
        """
        if not location_text:
            return None
            
        # First attempt direct matching using the mapping
        normalized_text = self.normalize_text(location_text)
        for key, value in self.location_mapping.items():
            if key in normalized_text:
                logger.debug(f"Direct mapping match: '{location_text}' -> '{value}'")
                return value
        
        # Extract potential neighborhood terms
        neighborhood_terms = self.extract_neighborhoods(location_text)
        
        # Check if any term is in our neighborhoods list (case-insensitive)
        normalized_neighborhoods = [self.normalize_text(n) for n in self.neighborhoods]
        for term in neighborhood_terms:
            if term in normalized_neighborhoods:
                index = normalized_neighborhoods.index(term)
                logger.debug(f"Direct term match: '{location_text}' -> '{self.neighborhoods[index]}'")
                return self.neighborhoods[index]
        
        # Try fuzzy matching
        best_match = None
        best_score = 0
        
        for term in neighborhood_terms:
            # Only consider terms of a minimum length for fuzzy matching
            if len(term) >= 4:  
                match, score = process.extractOne(term, normalized_neighborhoods)
                if score > best_score and score >= self.fuzzy_threshold:
                    best_score = score
                    index = normalized_neighborhoods.index(match)
                    best_match = self.neighborhoods[index]
        
        if best_match:
            logger.debug(f"Fuzzy match: '{location_text}' -> '{best_match}' (score: {best_score})")
            return best_match
        
        # Look for specific location patterns
        # For example: "Apartamento em Lisboa, Bairro Alto" -> "Bairro Alto"
        patterns = [
            r'(?:em|in|at|no|na|nos|nas)\s+lisboa,\s+([^,\.]+)',
            r'lisboa\s*[-/]\s*([^,\.]+)',
            r'([^,\.]+)\s*[-/]\s*lisboa'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, normalized_text, re.IGNORECASE)
            if match:
                potential_neighborhood = match.group(1).strip()
                # Check if this matches a neighborhood
                for neighborhood in normalized_neighborhoods:
                    if neighborhood in potential_neighborhood or fuzz.ratio(neighborhood, potential_neighborhood) >= self.fuzzy_threshold:
                        index = normalized_neighborhoods.index(neighborhood)
                        logger.debug(f"Pattern match: '{location_text}' -> '{self.neighborhoods[index]}'")
                        return self.neighborhoods[index]
        
        logger.debug(f"No match found for location: '{location_text}'")
        return None
    
    def batch_match_locations(self, location_texts: List[str]) -> Dict[str, str]:
        """
        Match a batch of location strings to standardized neighborhood names.
        
        Args:
            location_texts: List of location strings to match
            
        Returns:
            Dictionary mapping original strings to standardized names (None for no match)
        """
        results = {}
        for text in location_texts:
            if text:
                results[text] = self.match_location(text)
        
        match_count = sum(1 for v in results.values() if v is not None)
        logger.info(f"Batch matched {match_count} out of {len(results)} locations")
        return results
    
    def save_mapping_report(self, mapping_results: Dict[str, str], output_file: str = "location_mapping_report.json"):
        """
        Save the mapping results to a JSON file.
        
        Args:
            mapping_results: Dictionary of mapping results
            output_file: Path to the output file
        """
        # Create directory if it doesn't exist
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Group by matched neighborhood
        grouped_results = {}
        for original, matched in mapping_results.items():
            if matched not in grouped_results:
                grouped_results[matched] = []
            grouped_results[matched].append(original)
        
        # Summary statistics
        report = {
            "summary": {
                "total_locations": len(mapping_results),
                "matched_locations": sum(1 for v in mapping_results.values() if v is not None),
                "unmatched_locations": sum(1 for v in mapping_results.values() if v is None),
                "unique_neighborhoods": len([v for v in mapping_results.values() if v is not None])
            },
            "neighborhoods": {},
            "unmatched": []
        }
        
        # Add neighborhood matches
        for neighborhood, locations in grouped_results.items():
            if neighborhood is not None:
                report["neighborhoods"][neighborhood] = locations
            else:
                report["unmatched"] = locations
        
        # Save the report
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Saved location mapping report to {output_file}")
        return report["summary"]

    def standardize_location(self, location: str) -> str:
        """
        Enhanced standardize_location method that incorporates logic from improve_location_matching.py
        """
        if not location:
            return None
            
        # Convert to lowercase and remove extra spaces
        location = location.lower().strip()
        
        # Remove common prefixes and numbers
        location = re.sub(r'rua|avenida|travessa|largo|praça|estrada|beco|calçada|bairro', '', location)
        location = re.sub(r'\d+', '', location)
        
        # Remove common symbols and extra whitespace
        location = re.sub(r'[,\.;:\-–—_/\\]', ' ', location)
        location = re.sub(r'\s+', ' ', location).strip()
        
        # Use the existing normalization logic
        normalized = self.normalize_text(location)
        
        # First check direct matches in mapping
        if normalized in self.location_mapping:
            return self.location_mapping[normalized]
            
        # Try fuzzy matching for locations not found in mapping
        best_match = process.extractOne(normalized, self.location_mapping.keys(), 
                                      scorer=fuzz.token_sort_ratio)
        if best_match and best_match[1] >= self.fuzzy_threshold:
            return self.location_mapping[best_match[0]]
        
        # If we have a neighborhood name in the original text, use that
        for neighborhood in self.neighborhoods:
            if neighborhood.lower() in location:
                return neighborhood
                
        # No match found, return None or original
        return None


def standardize_locations(data: List[Dict], location_field: str = "location", 
                         output_field: str = "neighborhood", matcher: LocationMatcher = None) -> List[Dict]:
    """
    Standardize location fields in a list of property dictionaries.
    
    Args:
        data: List of property dictionaries
        location_field: Name of the field containing location strings
        output_field: Name of the field to store standardized neighborhood names
        matcher: LocationMatcher instance. If None, a new one is created with default settings.
        
    Returns:
        List of property dictionaries with standardized neighborhood fields added
    """
    # Create a matcher if not provided
    if matcher is None:
        matcher = LocationMatcher()
    
    # Extract location texts
    location_texts = [item.get(location_field, "") for item in data if location_field in item]
    
    # Match locations
    matched_locations = matcher.batch_match_locations(location_texts)
    
    # Update data with matched neighborhoods
    for item in data:
        if location_field in item:
            location = item[location_field]
            if location in matched_locations:
                item[output_field] = matched_locations[location]
    
    match_count = sum(1 for item in data if output_field in item and item[output_field] is not None)
    logger.info(f"Standardized {match_count} out of {len(data)} locations")
    
    return data


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/data_processing/location_matching.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    sample_locations = [
        "Apartamento T2 em Lisboa, Bairro Alto",
        "Lisboa / Campo de Ourique, Perto do Metro",
        "Excelente apartamento na zona da Avenida da Liberdade",
        "Lux Terrace Príncipe Real",
        "Apartamento T1 em Alcântara",
        "Apartamento em Benfica",
        "Moradia em Belem com Vista Rio",
        "Estúdio no centro de Lisboa"
    ]
    
    # Create a matcher and test sample locations
    matcher = LocationMatcher()
    for location in sample_locations:
        neighborhood = matcher.match_location(location)
        print(f"'{location}' -> {neighborhood}")
    
    # Test batch matching
    results = matcher.batch_match_locations(sample_locations)
    
    # Save a report
    matcher.save_mapping_report(results, "propbot/data/processed/sample_location_mapping.json") 