#!/usr/bin/env python3
"""
PropBot Analysis Runner

This script runs the PropBot rental analysis and generates investment reports.
"""

import sys
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f"propbot/logs/propbot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    ]
)

def main():
    """Run the PropBot analysis pipeline."""
    logging.info("Starting PropBot analysis pipeline")
    
    # Import here to avoid circular imports
    from propbot.analysis.metrics.rental_metrics import run_improved_analysis
    
    # Run the rental analysis
    success = run_improved_analysis(
        similarity_threshold=40,  # 40% location similarity threshold
        min_comparable_properties=2  # Require at least 2 comparable properties
    )
    
    if success:
        logging.info("PropBot analysis completed successfully")
        return 0
    else:
        logging.error("PropBot analysis failed")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 