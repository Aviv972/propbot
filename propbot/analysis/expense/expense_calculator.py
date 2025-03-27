#!/usr/bin/env python3
"""
PropBot Expense Calculator

This module calculates recurring and one-time expenses for property investments.
"""

import os
import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default expense parameters
DEFAULT_EXPENSE_RATES = {
    # Recurring expenses (annual percentage of property value)
    "property_management": 0.05,  # 5% of monthly rent
    "maintenance": 0.01,  # 1% of property value annually
    "vacancy": 0.05,  # 5% of annual rent
    "insurance": 0.005,  # 0.5% of property value annually
    "property_tax": 0.003,  # 0.3% of property value annually (IMI)
    
    # Fixed monthly expenses
    "utilities": 50,  # €50/month for common area utilities
    
    # One-time expenses (percentage of property value)
    "closing_costs": 0.01,  # 1% for closing costs
    "renovation": 0.02,  # 2% for minor renovations
    "furnishing": 0.03,  # 3% for furnishing
}

def load_expense_parameters(config_file=None):
    """
    Load expense parameters from config file or use defaults.
    
    Args:
        config_file: Path to config file (optional)
        
    Returns:
        Dictionary of expense parameters
    """
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                params = json.load(f)
            logger.info(f"Loaded expense parameters from {config_file}")
            return params
        except Exception as e:
            logger.error(f"Error loading expense parameters: {e}")
    
    # Attempt to load from standard locations
    standard_locations = [
        "propbot/config/expense_defaults.json",
        "config/expense_defaults.json",
        "../../../config/expense_defaults.json"
    ]
    
    for location in standard_locations:
        if os.path.exists(location):
            try:
                with open(location, 'r', encoding='utf-8') as f:
                    params = json.load(f)
                logger.info(f"Loaded expense parameters from {location}")
                return params
            except Exception as e:
                logger.error(f"Error loading expense parameters from {location}: {e}")
    
    # Use defaults if no file found or error occurred
    logger.info("Using default expense parameters")
    return DEFAULT_EXPENSE_RATES

def calculate_recurring_expenses(property_value, monthly_rent, params=None):
    """
    Calculate recurring expenses for a property.
    
    Args:
        property_value: Property value in euros
        monthly_rent: Monthly rent in euros
        params: Custom expense parameters (optional)
        
    Returns:
        Dictionary of recurring expenses with total
    """
    # Load parameters (use defaults if not provided)
    if params is None:
        params = load_expense_parameters()
    
    annual_rent = monthly_rent * 12
    
    # Calculate each expense
    property_management = annual_rent * params.get("property_management", DEFAULT_EXPENSE_RATES["property_management"])
    maintenance = property_value * params.get("maintenance", DEFAULT_EXPENSE_RATES["maintenance"])
    vacancy = annual_rent * params.get("vacancy", DEFAULT_EXPENSE_RATES["vacancy"])
    insurance = property_value * params.get("insurance", DEFAULT_EXPENSE_RATES["insurance"])
    property_tax = property_value * params.get("property_tax", DEFAULT_EXPENSE_RATES["property_tax"])
    utilities = params.get("utilities", DEFAULT_EXPENSE_RATES["utilities"]) * 12  # Annual utilities cost
    
    # Create expenses dictionary
    expenses = {
        "property_management": property_management,
        "maintenance": maintenance,
        "vacancy": vacancy,
        "insurance": insurance,
        "property_tax": property_tax,
        "utilities": utilities,
        "total": property_management + maintenance + vacancy + insurance + property_tax + utilities
    }
    
    return expenses

def calculate_one_time_expenses(property_value, params=None):
    """
    Calculate one-time expenses for a property.
    
    Args:
        property_value: Property value in euros
        params: Custom expense parameters (optional)
        
    Returns:
        Dictionary of one-time expenses with total
    """
    # Load parameters (use defaults if not provided)
    if params is None:
        params = load_expense_parameters()
    
    # Calculate each expense
    closing_costs = property_value * params.get("closing_costs", DEFAULT_EXPENSE_RATES["closing_costs"])
    renovation = property_value * params.get("renovation", DEFAULT_EXPENSE_RATES["renovation"])
    furnishing = property_value * params.get("furnishing", DEFAULT_EXPENSE_RATES["furnishing"])
    
    # Create expenses dictionary
    expenses = {
        "closing_costs": closing_costs,
        "renovation": renovation,
        "furnishing": furnishing,
        "total": closing_costs + renovation + furnishing
    }
    
    return expenses

if __name__ == "__main__":
    # Example usage
    property_value = 300000
    monthly_rent = 1200
    
    recurring = calculate_recurring_expenses(property_value, monthly_rent)
    one_time = calculate_one_time_expenses(property_value)
    
    print(f"Recurring Expenses (Annual): €{recurring['total']:.2f}")
    print(f"One-Time Expenses: €{one_time['total']:.2f}") 