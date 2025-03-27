#!/usr/bin/env python3
"""
PropBot Investment Metrics Calculator

This module calculates advanced investment metrics for real estate properties.
"""

import logging
import numpy as np
import math
import os
import json
from datetime import datetime
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default investment parameters
DEFAULT_INVESTMENT_PARAMS = {
    "down_payment_rate": 0.20,  # 20% down payment
    "interest_rate": 0.035,  # 3.5% interest rate
    "loan_term_years": 30,  # 30-year mortgage
    "mortgage_type": "fixed",  # fixed rate mortgage
    "appreciation_rate": 0.03,  # 3% annual appreciation
    "rental_growth_rate": 0.02,  # 2% annual rent growth
    "expense_growth_rate": 0.025,  # 2.5% annual expense growth
    "vacancy_rate": 0.05,  # 5% vacancy
    "income_tax_rate": 0.28,  # 28% income tax
}

DEFAULT_EXPENSE_PARAMS = {
    "property_tax_rate": 0.005,  # 0.5% property tax
    "insurance_rate": 0.004,  # 0.4% insurance
    "maintenance_rate": 0.01,  # 1% maintenance
    "management_rate": 0.08,  # 8% property management
    "utilities": 0,  # No utilities
    "vacancy_rate": 0.08,  # 8% vacancy
    "closing_cost_rate": 0.03,  # 3% closing costs
    "renovation_cost_rate": 0.05,  # 5% renovation costs
}

def calculate_mortgage_payment(loan_amount, interest_rate, term_years):
    """
    Calculate monthly mortgage payment.
    
    Args:
        loan_amount: Loan amount in euros
        interest_rate: Annual interest rate (decimal)
        term_years: Loan term in years
        
    Returns:
        Monthly mortgage payment in euros
    """
    # Convert annual interest rate to monthly
    monthly_interest_rate = interest_rate / 12
    
    # Calculate number of payments
    num_payments = term_years * 12
    
    # Calculate monthly payment
    if monthly_interest_rate == 0:
        return loan_amount / num_payments
    
    payment = loan_amount * (monthly_interest_rate * (1 + monthly_interest_rate) ** num_payments) / \
              ((1 + monthly_interest_rate) ** num_payments - 1)
    
    return payment

def calculate_noi(property_data, expense_params=None):
    """
    Calculate Net Operating Income (NOI).
    
    Args:
        property_data: Dictionary containing property information
        expense_params: Custom expense parameters (optional)
        
    Returns:
        Annual NOI in euros
    """
    # Extract property data
    property_value = property_data.get("price", 0)
    monthly_rent = property_data.get("monthly_rent", 0)
    annual_rent = monthly_rent * 12
    
    # Calculate operating expenses (excluding mortgage)
    recurring_expenses = calculate_recurring_expenses(
        property_value, 
        monthly_rent, 
        expense_params
    )
    
    # NOI = Annual Rental Income - Operating Expenses
    noi = annual_rent - recurring_expenses["total"]
    
    return noi

def calculate_cap_rate(property_data, expense_params=None):
    """
    Calculate Capitalization Rate (Cap Rate).
    
    Args:
        property_data: Dictionary containing property information
        expense_params: Custom expense parameters (optional)
        
    Returns:
        Cap Rate as a decimal
    """
    noi = calculate_noi(property_data, expense_params)
    property_value = property_data.get("price", 0)
    
    if property_value == 0:
        return 0
    
    cap_rate = noi / property_value
    
    return cap_rate

def calculate_gross_yield(property_data):
    """
    Calculate Gross Rental Yield.
    
    Args:
        property_data: Dictionary containing property information
        
    Returns:
        Gross Yield as a decimal
    """
    monthly_rent = property_data.get("monthly_rent", 0)
    annual_rent = monthly_rent * 12
    property_value = property_data.get("price", 0)
    
    if property_value == 0:
        return 0
    
    gross_yield = annual_rent / property_value
    
    return gross_yield

def calculate_cash_on_cash_return(property_data, investment_params=None, expense_params=None):
    """
    Calculate Cash on Cash Return.
    
    Args:
        property_data: Dictionary containing property information
        investment_params: Custom investment parameters (optional)
        expense_params: Custom expense parameters (optional)
        
    Returns:
        Cash on Cash Return as a decimal
    """
    # Use default investment parameters if none provided
    if investment_params is None:
        investment_params = DEFAULT_INVESTMENT_PARAMS
    
    # Extract property data
    property_value = property_data.get("price", 0)
    monthly_rent = property_data.get("monthly_rent", 0)
    annual_rent = monthly_rent * 12
    
    # Calculate down payment
    down_payment_rate = investment_params.get("down_payment_rate", DEFAULT_INVESTMENT_PARAMS["down_payment_rate"])
    down_payment = property_value * down_payment_rate
    
    # Calculate loan amount
    loan_amount = property_value - down_payment
    
    # Calculate mortgage payment
    interest_rate = investment_params.get("interest_rate", DEFAULT_INVESTMENT_PARAMS["interest_rate"])
    term_years = investment_params.get("loan_term_years", DEFAULT_INVESTMENT_PARAMS["loan_term_years"])
    monthly_mortgage_payment = calculate_mortgage_payment(loan_amount, interest_rate, term_years)
    annual_mortgage_payment = monthly_mortgage_payment * 12
    
    # Calculate operating expenses
    recurring_expenses = calculate_recurring_expenses(property_value, monthly_rent, expense_params)
    
    # Calculate annual taxes (excluding income tax)
    taxes = calculate_total_taxes(property_value, annual_rent)
    annual_property_tax = taxes["imi"]  # Only include property tax, not income tax for cash flow
    
    # Calculate one-time expenses (closing costs, renovation, etc.)
    one_time_expenses = calculate_one_time_expenses(property_value, expense_params)
    
    # Calculate annual cash flow
    annual_cash_flow = annual_rent - recurring_expenses["total"] - annual_mortgage_payment - annual_property_tax
    
    # Calculate total cash invested
    total_cash_invested = down_payment + one_time_expenses["total"]
    
    if total_cash_invested == 0:
        return 0
    
    # Calculate cash on cash return
    cash_on_cash_return = annual_cash_flow / total_cash_invested
    
    return cash_on_cash_return

def calculate_monthly_cash_flow(property_data, investment_params=None, expense_params=None):
    """
    Calculate Monthly Cash Flow.
    
    Args:
        property_data: Dictionary containing property information
        investment_params: Custom investment parameters (optional)
        expense_params: Custom expense parameters (optional)
        
    Returns:
        Monthly cash flow in euros
    """
    # Use default investment parameters if none provided
    if investment_params is None:
        investment_params = DEFAULT_INVESTMENT_PARAMS
    
    # Extract property data
    property_value = property_data.get("price", 0)
    monthly_rent = property_data.get("monthly_rent", 0)
    annual_rent = monthly_rent * 12
    
    # Calculate loan amount
    down_payment_rate = investment_params.get("down_payment_rate", DEFAULT_INVESTMENT_PARAMS["down_payment_rate"])
    loan_amount = property_value * (1 - down_payment_rate)
    
    # Calculate mortgage payment
    interest_rate = investment_params.get("interest_rate", DEFAULT_INVESTMENT_PARAMS["interest_rate"])
    term_years = investment_params.get("loan_term_years", DEFAULT_INVESTMENT_PARAMS["loan_term_years"])
    monthly_mortgage_payment = calculate_mortgage_payment(loan_amount, interest_rate, term_years)
    
    # Calculate monthly operating expenses
    recurring_expenses = calculate_recurring_expenses(property_value, monthly_rent, expense_params)
    monthly_operating_expenses = recurring_expenses["total"] / 12
    
    # Calculate monthly property tax
    taxes = calculate_total_taxes(property_value, annual_rent)
    monthly_property_tax = taxes["imi"] / 12
    
    # Calculate monthly cash flow
    monthly_cash_flow = monthly_rent - monthly_operating_expenses - monthly_mortgage_payment - monthly_property_tax
    
    return monthly_cash_flow

def calculate_price_per_sqm(property_data):
    """
    Calculate Price per Square Meter.
    
    Args:
        property_data: Dictionary containing property information
        
    Returns:
        Price per square meter in euros
    """
    price = property_data.get("price", 0)
    size = property_data.get("size", 0)
    
    if size == 0:
        return 0
    
    price_per_sqm = price / size
    
    return price_per_sqm

def calculate_all_investment_metrics(property_data, investment_params=None, expense_params=None):
    """
    Calculate all investment metrics for a property.
    
    Args:
        property_data (dict): Property data including price, monthly_rent, and size
        investment_params (dict, optional): Investment parameters
        expense_params (dict, optional): Expense parameters
    
    Returns:
        dict: Dictionary containing all calculated investment metrics
    """
    # Initialize with default values
    metrics = {
        'price': 0,
        'monthly_rent': 0,
        'size': 0,
        'price_per_sqm': 0,
        'noi_monthly': 0,
        'cap_rate': 0,
        'gross_yield': 0,
        'coc_return': 0,
        'monthly_cash_flow': 0,
        'annual_cash_flow': 0,
        'break_even_ratio': 0,
        'expected_appreciation': 0,
        'total_return': 0,
        'url': '',
        'location': '',
        'is_valid': False
    }
    
    # Extract property data
    price = float(property_data.get('price', 0) or 0)
    monthly_rent = float(property_data.get('monthly_rent', 0) or 0)
    size = float(property_data.get('size', 0) or 0)
    url = property_data.get('url', '')
    location = property_data.get('location', '')
    
    # Skip calculations if essential data is missing
    if price <= 0 or size <= 0:
        logging.debug(f"Skipping metrics calculation due to invalid data: price={price}, size={size}")
        metrics.update({
            'price': price,
            'size': size,
            'url': url,
            'location': location,
            'is_valid': False
        })
        return metrics
    
    # Set default investment parameters if none provided
    if not investment_params:
        investment_params = DEFAULT_INVESTMENT_PARAMS
    
    # Set default expense parameters if none provided
    if not expense_params:
        expense_params = DEFAULT_EXPENSE_PARAMS
    
    # Calculate price per square meter
    price_per_sqm = price / size if size > 0 else 0
    
    # Calculate monthly rent if not provided
    if monthly_rent <= 0:
        logging.debug("Monthly rent not provided, cannot calculate accurate metrics")
        metrics.update({
            'price': price,
            'size': size,
            'price_per_sqm': price_per_sqm,
            'url': url,
            'location': location,
            'is_valid': False
        })
        return metrics
    
    # Calculate recurring expenses
    # Property tax moved to one-time expenses
    insurance = expense_params.get('insurance_rate', 0.004) * price / 12
    maintenance = expense_params.get('maintenance_rate', 0.01) * price / 12
    management = expense_params.get('management_rate', 0.08) * monthly_rent
    utilities = expense_params.get('utilities', 0)
    vacancy_rate = expense_params.get('vacancy_rate', 0.08)
    vacancy_cost = monthly_rent * vacancy_rate
    
    # Total monthly expenses (property tax removed)
    monthly_expenses = insurance + maintenance + management + utilities + vacancy_cost
    
    # Calculate one-time expenses, including property tax as one-time
    closing_costs = expense_params.get('closing_cost_rate', 0.03) * price
    renovation_costs = expense_params.get('renovation_cost_rate', 0.05) * price
    property_tax_onetime = expense_params.get('property_tax_rate', 0.005) * price  # Property tax as one-time expense
    
    # Calculate investment amount (full price, no mortgage)
    total_investment = price + closing_costs + renovation_costs + property_tax_onetime
    
    # Calculate NOI (Net Operating Income)
    noi_monthly = monthly_rent - monthly_expenses
    noi_annual = noi_monthly * 12
    
    # Calculate Cap Rate
    cap_rate = (noi_annual / price) * 100 if price > 0 else 0
    
    # Calculate Gross Yield
    gross_yield = ((monthly_rent * 12) / price) * 100 if price > 0 else 0
    
    # Calculate Cash on Cash Return (no mortgage)
    monthly_cash_flow = noi_monthly  # No mortgage payment
    annual_cash_flow = monthly_cash_flow * 12
    coc_return = (annual_cash_flow / total_investment) * 100 if total_investment > 0 else 0
    
    # Calculate Break-Even Ratio (without mortgage)
    break_even_ratio = monthly_expenses / monthly_rent if monthly_rent > 0 else 0
    
    # Calculate Expected Appreciation
    appreciation_rate = investment_params.get('appreciation_rate', 0.03)
    expected_appreciation = appreciation_rate * 100
    
    # Calculate Total Return (CoC + Appreciation)
    total_return = coc_return + expected_appreciation
    
    # Update metrics dictionary
    metrics.update({
        'price': price,
        'monthly_rent': monthly_rent,
        'size': size,
        'price_per_sqm': price_per_sqm,
        'noi_monthly': noi_monthly,
        'noi_annual': noi_annual,
        'cap_rate': cap_rate,
        'gross_yield': gross_yield,
        'coc_return': coc_return,
        'monthly_cash_flow': monthly_cash_flow,
        'annual_cash_flow': annual_cash_flow,
        'break_even_ratio': break_even_ratio,
        'expected_appreciation': expected_appreciation,
        'total_return': total_return,
        'url': url,
        'location': location,
        'is_valid': True
    })
    
    return metrics

def find_best_properties(properties_data: List[Dict[str, Any]], top_n: int = 10) -> Dict[str, List[Dict[str, Any]]]:
    """
    Find and rank the best investment properties based on different metrics.
    
    Args:
        properties_data: List of property dictionaries with investment metrics
        top_n: Number of top properties to return for each category
        
    Returns:
        Dictionary with lists of top properties by different metrics
    """
    logger.info(f"Finding best properties among {len(properties_data)} properties")
    
    # Filter out properties with missing data
    valid_properties = []
    for prop in properties_data:
        if (prop.get('price') and prop.get('monthly_rent') and 
            prop.get('noi') is not None and 
            prop.get('cap_rate') is not None):
            valid_properties.append(prop)
            
    if not valid_properties:
        logger.warning("No valid properties with complete metrics found")
        return {
            "by_cap_rate": [],
            "by_cash_on_cash": [],
            "by_cash_flow": []
        }
    
    logger.info(f"Found {len(valid_properties)} properties with valid metrics")
    
    # Sort properties by different metrics
    by_cap_rate = sorted(valid_properties, key=lambda x: x.get('cap_rate', 0) or 0, reverse=True)
    by_cash_on_cash = sorted(valid_properties, key=lambda x: x.get('cash_on_cash_return', 0) or 0, reverse=True)
    by_cash_flow = sorted(valid_properties, key=lambda x: x.get('monthly_cash_flow', 0) or 0, reverse=True)
    
    # Get top N for each category
    top_by_cap_rate = by_cap_rate[:top_n]
    top_by_cash_on_cash = by_cash_on_cash[:top_n]
    top_by_cash_flow = by_cash_flow[:top_n]
    
    # Calculate additional statistics
    positive_cash_flow_count = sum(1 for p in valid_properties if p.get('monthly_cash_flow', 0) > 0)
    positive_cash_flow_percent = (positive_cash_flow_count / len(valid_properties)) * 100 if valid_properties else 0
    
    logger.info(f"Properties with positive cash flow: {positive_cash_flow_count} ({positive_cash_flow_percent:.1f}%)")
    
    # Prepare result
    result = {
        "by_cap_rate": top_by_cap_rate,
        "by_cash_on_cash": top_by_cash_on_cash,
        "by_cash_flow": top_by_cash_flow,
        "stats": {
            "total_properties": len(valid_properties),
            "positive_cash_flow_count": positive_cash_flow_count,
            "positive_cash_flow_percent": positive_cash_flow_percent
        }
    }
    
    return result

def generate_best_properties_report(properties_data: List[Dict[str, Any]], output_file: str = None) -> bool:
    """
    Generate a report of the best investment properties.
    
    Args:
        properties_data: List of property dictionaries with investment metrics
        output_file: Path to save the report
        
    Returns:
        Boolean indicating success
    """
    try:
        logger.info("Generating best properties report")
        
        # Find best properties
        best_properties = find_best_properties(properties_data)
        
        # Add timestamp
        report = {
            "timestamp": datetime.now().isoformat(),
            "properties": best_properties
        }
        
        # Save to file if specified
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2)
            logger.info(f"Best properties report saved to {output_file}")
        
        # Return success
        return True
    
    except Exception as e:
        logger.error(f"Error generating best properties report: {str(e)}")
        return False

if __name__ == "__main__":
    # Example usage
    property_data = {
        "price": 300000,
        "size": 75,
        "monthly_rent": 1200,
        "neighborhood": "Alfama"
    }
    
    metrics = calculate_all_investment_metrics(property_data)
    
    print(f"Price: €{metrics['price']:,.2f}")
    print(f"Size: {metrics['size']} sqm")
    print(f"Monthly Rent: €{metrics['monthly_rent']:,.2f}")
    print(f"Annual Rent: €{metrics['annual_rent']:,.2f}")
    print(f"Recurring Expenses: €{metrics['recurring_expenses']:,.2f}")
    print(f"One-Time Expenses: €{metrics['one_time_expenses']:,.2f}")
    print(f"Taxes (Annual): €{metrics['taxes']:,.2f}")
    print(f"NOI: €{metrics['noi']:,.2f}")
    print(f"Cap Rate: {metrics['cap_rate']*100:.2f}%")
    print(f"Gross Yield: {metrics['gross_yield']*100:.2f}%")
    print(f"Cash on Cash Return: {metrics['cash_on_cash_return']*100:.2f}%")
    print(f"Monthly Cash Flow: €{metrics['monthly_cash_flow']:,.2f}")
    print(f"Price per sqm: €{metrics['price_per_sqm']:,.2f}") 