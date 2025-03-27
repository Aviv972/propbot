#!/usr/bin/env python3
"""
PropBot Tax Calculator

This module calculates various property taxes for investments in Portugal.
"""

import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Tax rates for Portugal
TAX_RATES = {
    "IMT": {  # Property Transfer Tax (Imposto Municipal sobre Transmissões)
        "residential": [
            (0, 92407, 0),         # Up to €92,407: 0%
            (92407, 126403, 0.02),  # €92,407 to €126,403: 2%
            (126403, 172348, 0.05),  # €126,403 to €172,348: 5%
            (172348, 287213, 0.07),  # €172,348 to €287,213: 7%
            (287213, 574323, 0.08),  # €287,213 to €574,323: 8%
            (574323, float('inf'), 0.06)  # Over €574,323: 6%
        ],
        "non_residential": 0.065,  # 6.5% flat rate
        "urban_for_resale": 0.065,  # 6.5% flat rate
    },
    "IMI": {  # Municipal Property Tax (Imposto Municipal sobre Imóveis)
        "urban": (0.003, 0.008),  # Between 0.3% and 0.8% (depends on municipality)
        "rural": 0.008,  # 0.8% flat rate
        "default": 0.005,  # Default rate (0.5%) if specific municipality not known
    },
    "IRS": {  # Income Tax (Imposto sobre o Rendimento de Singulares)
        "rental_income": 0.28,  # 28% flat rate
    },
    "stamp_duty": 0.008,  # 0.8% of property value
}

def calculate_imt(property_value, property_type="residential"):
    """
    Calculate IMT (Property Transfer Tax) for a property in Portugal.
    
    Args:
        property_value: Property value in euros
        property_type: Type of property ("residential", "non_residential", "urban_for_resale")
        
    Returns:
        IMT tax amount in euros
    """
    if property_type != "residential":
        # For non-residential or urban for resale, apply flat rate
        rate = TAX_RATES["IMT"].get(property_type, TAX_RATES["IMT"]["non_residential"])
        return property_value * rate
    
    # For residential properties, use the progressive brackets
    brackets = TAX_RATES["IMT"]["residential"]
    
    # Find the applicable bracket
    applicable_bracket = None
    for min_val, max_val, rate in brackets:
        if min_val <= property_value < max_val:
            applicable_bracket = (min_val, max_val, rate)
            break
    
    if applicable_bracket:
        _, _, rate = applicable_bracket
        return property_value * rate
    else:
        # If no bracket found, use the highest bracket
        _, _, rate = brackets[-1]
        return property_value * rate

def calculate_imi(property_value, property_type="urban", municipality=None):
    """
    Calculate IMI (Municipal Property Tax) for a property in Portugal.
    
    Args:
        property_value: Property value in euros
        property_type: Type of property ("urban", "rural")
        municipality: Municipality name (optional)
        
    Returns:
        Annual IMI tax amount in euros
    """
    if property_type == "rural":
        rate = TAX_RATES["IMI"]["rural"]
    else:
        # If municipality is provided, could look up specific rate in a database
        # For now, using default rate
        rate = TAX_RATES["IMI"]["default"]
    
    return property_value * rate

def calculate_income_tax(annual_rental_income, expenses=0):
    """
    Calculate income tax on rental income in Portugal.
    
    Args:
        annual_rental_income: Annual rental income in euros
        expenses: Deductible expenses in euros
        
    Returns:
        Annual income tax amount in euros
    """
    # In Portugal, landlords can deduct some expenses from rental income
    # Simplified calculation for now
    taxable_income = max(0, annual_rental_income - expenses)
    tax_rate = TAX_RATES["IRS"]["rental_income"]
    
    return taxable_income * tax_rate

def calculate_stamp_duty(property_value):
    """
    Calculate stamp duty for a property purchase in Portugal.
    
    Args:
        property_value: Property value in euros
        
    Returns:
        Stamp duty amount in euros
    """
    return property_value * TAX_RATES["stamp_duty"]

def calculate_total_taxes(property_value, annual_rental_income, property_type="residential", expenses=0):
    """
    Calculate total taxes for a property investment in Portugal.
    
    Args:
        property_value: Property value in euros
        annual_rental_income: Annual rental income in euros
        property_type: Type of property ("residential", "non_residential", "urban_for_resale")
        expenses: Deductible expenses in euros
        
    Returns:
        Dictionary of tax amounts with total
    """
    imt = calculate_imt(property_value, property_type)
    imi = calculate_imi(property_value, property_type)
    income_tax = calculate_income_tax(annual_rental_income, expenses)
    stamp_duty = calculate_stamp_duty(property_value)
    
    # Create taxes dictionary
    taxes = {
        "imt": imt,  # One-time tax on purchase
        "imi": imi,  # Annual property tax
        "income_tax": income_tax,  # Annual tax on rental income
        "stamp_duty": stamp_duty,  # One-time tax on purchase
        "one_time_total": imt + stamp_duty,  # Total one-time taxes
        "annual_total": imi + income_tax,  # Total annual taxes
    }
    
    return taxes

if __name__ == "__main__":
    # Example usage
    property_value = 300000
    annual_rental_income = 14400  # €1200 per month
    
    taxes = calculate_total_taxes(property_value, annual_rental_income)
    
    print(f"One-Time Taxes: €{taxes['one_time_total']:.2f}")
    print(f"Annual Taxes: €{taxes['annual_total']:.2f}")
    print(f"IMT: €{taxes['imt']:.2f}")
    print(f"IMI (Annual): €{taxes['imi']:.2f}")
    print(f"Income Tax (Annual): €{taxes['income_tax']:.2f}")
    print(f"Stamp Duty: €{taxes['stamp_duty']:.2f}") 