#!/usr/bin/env python3
"""
Yield Calculator Module

This module calculates rental yields and other investment metrics for properties,
taking into account various expenses, taxes, and financing options.
"""

import os
import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Union, Any
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

class YieldCalculator:
    """A class to calculate rental yields and investment metrics."""
    
    def __init__(self, tax_rates_path: str = None):
        """
        Initialize the yield calculator.
        
        Args:
            tax_rates_path: Path to the tax rates JSON file
        """
        self.tax_rates = self._load_tax_rates(tax_rates_path)
        logger.info("Initialized YieldCalculator")
    
    def _load_tax_rates(self, tax_rates_path: str = None) -> Dict:
        """
        Load tax rates from a JSON file.
        
        Args:
            tax_rates_path: Path to the tax rates JSON file
            
        Returns:
            Dictionary of tax rates
        """
        # Default tax rates for Portugal
        default_rates = {
            "imi": {
                "urban": 0.003,        # IMI rate for urban properties (0.3%)
                "rural": 0.008         # IMI rate for rural properties (0.8%)
            },
            "aimi": {
                "threshold": 600000,    # AIMI threshold (€600,000)
                "rate": 0.007,          # AIMI rate (0.7%)
                "higher_rate": 0.01,    # Higher AIMI rate for properties over €1M (1%)
                "higher_threshold": 1000000
            },
            "irs": {
                "rates": [0.145, 0.21, 0.26, 0.35, 0.37, 0.45, 0.48],  # IRS rates
                "thresholds": [7479, 11284, 15992, 20700, 26355, 38632, 75009]  # IRS thresholds
            },
            "capital_gains": {
                "rate": 0.28,           # Capital gains tax rate (28%)
                "inflation_adjustment": True,
                "inflation_coefficients": {
                    "2023": 1.00,
                    "2022": 1.02,
                    "2021": 1.05,
                    "2020": 1.06,
                    "2019": 1.07,
                    "2018": 1.08,
                    "2017": 1.09,
                    "2016": 1.10,
                    "2015": 1.10,
                    "2014": 1.10,
                    "2013": 1.11,
                    "2012": 1.12,
                    "2011": 1.14,
                    "2010": 1.16,
                    "2009": 1.16,
                    "2008": 1.16,
                    "2007": 1.19,
                    "2006": 1.22,
                    "2005": 1.25,
                    "2004": 1.28,
                    "2003": 1.31,
                    "2002": 1.34,
                    "2001": 1.37,
                    "2000": 1.41
                }
            }
        }
        
        if tax_rates_path:
            try:
                with open(tax_rates_path, 'r', encoding='utf-8') as f:
                    custom_rates = json.load(f)
                
                # Merge custom rates with default rates
                for category, rates in custom_rates.items():
                    if category in default_rates:
                        if isinstance(default_rates[category], dict) and isinstance(rates, dict):
                            default_rates[category].update(rates)
                        else:
                            default_rates[category] = rates
                    else:
                        default_rates[category] = rates
                
                logger.info(f"Loaded custom tax rates from {tax_rates_path}")
            except Exception as e:
                logger.error(f"Error loading tax rates from {tax_rates_path}: {e}")
        
        return default_rates
    
    def calculate_imi(self, property_value: float, is_urban: bool = True) -> float:
        """
        Calculate IMI (Imposto Municipal sobre Imóveis) for a property.
        
        Args:
            property_value: Property value in euros
            is_urban: Whether the property is urban (vs. rural)
            
        Returns:
            Annual IMI amount in euros
        """
        rate = self.tax_rates['imi']['urban'] if is_urban else self.tax_rates['imi']['rural']
        return property_value * rate
    
    def calculate_aimi(self, total_property_value: float) -> float:
        """
        Calculate AIMI (Adicional ao IMI) for a property portfolio.
        
        Args:
            total_property_value: Total value of all properties owned
            
        Returns:
            Annual AIMI amount in euros
        """
        threshold = self.tax_rates['aimi']['threshold']
        rate = self.tax_rates['aimi']['rate']
        higher_rate = self.tax_rates['aimi']['higher_rate']
        higher_threshold = self.tax_rates['aimi']['higher_threshold']
        
        if total_property_value <= threshold:
            return 0
        
        # Calculate AIMI for the portion between threshold and higher_threshold
        tier1_value = min(higher_threshold, total_property_value) - threshold
        aimi = tier1_value * rate
        
        # Add AIMI for the portion above higher_threshold
        if total_property_value > higher_threshold:
            tier2_value = total_property_value - higher_threshold
            aimi += tier2_value * higher_rate
        
        return aimi
    
    def calculate_income_tax(self, annual_rental_income: float, income_category: str = "default") -> float:
        """
        Calculate income tax on rental income.
        
        Args:
            annual_rental_income: Annual rental income in euros
            income_category: Income tax category
            
        Returns:
            Annual income tax amount in euros
        """
        # For rental income in Portugal, typically taxed at a flat 28% rate
        # unless the taxpayer opts to include it in their global income
        flat_rate = 0.28
        
        # Apply the flat rate by default
        if income_category == "default":
            return annual_rental_income * flat_rate
        
        # If user wants progressive taxation (global income)
        elif income_category == "progressive":
            # This is a simplified calculation and should be adjusted based on actual income
            thresholds = self.tax_rates['irs']['thresholds']
            rates = self.tax_rates['irs']['rates']
            
            tax = 0
            remaining_income = annual_rental_income
            
            for i, threshold in enumerate(thresholds):
                rate = rates[i]
                
                if i == 0:
                    income_in_bracket = min(remaining_income, threshold)
                else:
                    income_in_bracket = min(remaining_income, threshold - thresholds[i-1])
                
                tax += income_in_bracket * rate
                remaining_income -= income_in_bracket
                
                if remaining_income <= 0:
                    break
            
            # If there's still income above the highest threshold
            if remaining_income > 0:
                tax += remaining_income * rates[-1]
            
            return tax
        
        else:
            logger.warning(f"Unknown income category: {income_category}. Using flat rate.")
            return annual_rental_income * flat_rate
    
    def calculate_capital_gains_tax(self, 
                                  purchase_price: float, 
                                  sale_price: float, 
                                  purchase_year: int,
                                  sale_year: int,
                                  is_primary_residence: bool = False,
                                  reinvestment_amount: float = 0) -> float:
        """
        Calculate capital gains tax on a property sale.
        
        Args:
            purchase_price: Purchase price in euros
            sale_price: Sale price in euros
            purchase_year: Year of purchase
            sale_year: Year of sale
            is_primary_residence: Whether the property is primary residence
            reinvestment_amount: Amount reinvested in another primary residence
            
        Returns:
            Capital gains tax amount in euros
        """
        # Adjust purchase price for inflation if applicable
        adjusted_purchase_price = purchase_price
        
        if self.tax_rates['capital_gains']['inflation_adjustment']:
            coefficient = self.tax_rates['capital_gains']['inflation_coefficients'].get(str(purchase_year), 1.0)
            adjusted_purchase_price = purchase_price * coefficient
        
        # Calculate nominal gain
        nominal_gain = sale_price - adjusted_purchase_price
        
        # If there's a loss, no tax is due
        if nominal_gain <= 0:
            return 0
        
        # For primary residence with reinvestment
        if is_primary_residence and reinvestment_amount > 0:
            # Proportional exemption based on reinvestment
            exemption_ratio = min(reinvestment_amount / sale_price, 1.0)
            taxable_gain = nominal_gain * (1 - exemption_ratio)
        else:
            # For non-primary residences, only 50% of the gain is taxable in Portugal
            taxable_gain = nominal_gain * 0.5
        
        # Apply capital gains tax rate
        tax_rate = self.tax_rates['capital_gains']['rate']
        return taxable_gain * tax_rate
    
    def calculate_expenses(self, 
                         property_data: Dict,
                         include_mortgage: bool = True) -> Dict:
        """
        Calculate all expenses for a property.
        
        Args:
            property_data: Property data dictionary
            include_mortgage: Whether to include mortgage payments in expenses
            
        Returns:
            Dictionary of expenses
        """
        # Extract property data
        property_value = property_data.get('price', 0)
        annual_rent = property_data.get('annual_rent', 0)
        
        # If annual_rent is not available but monthly_rent is
        if not annual_rent and property_data.get('monthly_rent'):
            annual_rent = property_data.get('monthly_rent', 0) * 12
        
        # Get mortgage details if applicable
        mortgage_amount = property_data.get('mortgage_amount', 0)
        mortgage_rate = property_data.get('mortgage_rate', 0.03)  # 3% default
        mortgage_term = property_data.get('mortgage_term', 30)  # 30 years default
        
        # Calculate mortgage payment if applicable
        mortgage_payment = 0
        if include_mortgage and mortgage_amount > 0:
            monthly_rate = mortgage_rate / 12
            num_payments = mortgage_term * 12
            
            # Calculate monthly payment using the mortgage formula
            if monthly_rate > 0:
                mortgage_payment = mortgage_amount * (monthly_rate * (1 + monthly_rate) ** num_payments) / ((1 + monthly_rate) ** num_payments - 1)
            
            # Annual mortgage payment
            annual_mortgage = mortgage_payment * 12
        else:
            annual_mortgage = 0
        
        # Calculate property taxes
        imi = self.calculate_imi(property_value)
        
        # Calculate other expenses
        # Typical percentages of annual rent
        management_fee_pct = property_data.get('management_fee_pct', 0.08)  # 8% default
        maintenance_pct = property_data.get('maintenance_pct', 0.05)  # 5% default
        insurance_pct = property_data.get('insurance_pct', 0.005)  # 0.5% default
        vacancy_rate = property_data.get('vacancy_rate', 0.08)  # 8% default
        
        # Calculate absolute values
        management_fee = annual_rent * management_fee_pct
        maintenance = annual_rent * maintenance_pct
        insurance = property_value * insurance_pct
        vacancy_cost = annual_rent * vacancy_rate
        
        # Calculate total annual expenses
        total_expenses = imi + management_fee + maintenance + insurance + vacancy_cost
        
        # Include mortgage if applicable
        if include_mortgage:
            total_expenses += annual_mortgage
        
        # Return expenses breakdown
        expenses = {
            "property_tax": round(imi, 2),
            "management_fee": round(management_fee, 2),
            "maintenance": round(maintenance, 2),
            "insurance": round(insurance, 2),
            "vacancy_cost": round(vacancy_cost, 2),
            "annual_mortgage": round(annual_mortgage, 2) if include_mortgage else 0,
            "total_annual_expenses": round(total_expenses, 2),
            "total_monthly_expenses": round(total_expenses / 12, 2)
        }
        
        logger.info(f"Calculated expenses: {expenses['total_annual_expenses']} EUR annually")
        return expenses
    
    def calculate_rental_yield(self, 
                             property_data: Dict,
                             include_expenses: bool = True,
                             include_mortgage: bool = False,
                             include_taxes: bool = True) -> Dict:
        """
        Calculate rental yield for a property.
        
        Args:
            property_data: Property data dictionary
            include_expenses: Whether to include expenses in calculation
            include_mortgage: Whether to include mortgage in expenses
            include_taxes: Whether to include income tax in calculation
            
        Returns:
            Dictionary with yield calculations
        """
        # Extract property data
        property_price = property_data.get('price', 0)
        monthly_rent = property_data.get('monthly_rent', 0)
        annual_rent = monthly_rent * 12
        
        # Handle case where no rent is provided
        if not monthly_rent and property_data.get('annual_rent'):
            annual_rent = property_data.get('annual_rent')
            monthly_rent = annual_rent / 12
        
        # Calculate gross yield
        gross_yield = 0
        if property_price > 0 and annual_rent > 0:
            gross_yield = (annual_rent / property_price) * 100
        
        # Calculate expenses if required
        expenses = {}
        if include_expenses:
            property_with_rent = property_data.copy()
            if 'annual_rent' not in property_with_rent:
                property_with_rent['annual_rent'] = annual_rent
                
            expenses = self.calculate_expenses(property_with_rent, include_mortgage)
            total_expenses = expenses.get('total_annual_expenses', 0)
        else:
            total_expenses = 0
        
        # Calculate net income before tax
        net_income_before_tax = annual_rent - total_expenses
        
        # Calculate income tax if required
        income_tax = 0
        if include_taxes and net_income_before_tax > 0:
            income_tax = self.calculate_income_tax(net_income_before_tax)
        
        # Calculate net income after tax
        net_income_after_tax = net_income_before_tax - income_tax
        
        # Calculate net yield
        net_yield_before_tax = 0
        net_yield_after_tax = 0
        
        if property_price > 0:
            if net_income_before_tax > 0:
                net_yield_before_tax = (net_income_before_tax / property_price) * 100
                
            if net_income_after_tax > 0:
                net_yield_after_tax = (net_income_after_tax / property_price) * 100
        
        # Calculate cash-on-cash return if mortgage is involved
        cash_on_cash_return = None
        if include_mortgage and property_data.get('mortgage_amount', 0) > 0:
            down_payment = property_price - property_data.get('mortgage_amount', 0)
            closing_costs = property_data.get('closing_costs', 0)
            initial_investment = down_payment + closing_costs
            
            if initial_investment > 0 and net_income_after_tax > 0:
                cash_on_cash_return = (net_income_after_tax / initial_investment) * 100
        
        # Prepare result
        result = {
            "property_price": property_price,
            "monthly_rent": monthly_rent,
            "annual_rent": annual_rent,
            "gross_yield_percent": round(gross_yield, 2),
            "net_income_before_tax": round(net_income_before_tax, 2),
            "income_tax": round(income_tax, 2) if include_taxes else 0,
            "net_income_after_tax": round(net_income_after_tax, 2),
            "net_yield_before_tax_percent": round(net_yield_before_tax, 2),
            "net_yield_after_tax_percent": round(net_yield_after_tax, 2),
            "cash_on_cash_return_percent": round(cash_on_cash_return, 2) if cash_on_cash_return is not None else None,
            "expenses": expenses if include_expenses else {}
        }
        
        logger.info(f"Calculated rental yield: {result['gross_yield_percent']}% gross, "
                   f"{result['net_yield_after_tax_percent']}% net (after tax)")
        
        return result
    
    def calculate_roi(self, 
                    property_data: Dict,
                    holding_period: int = 5,
                    annual_appreciation_rate: float = 0.03,
                    include_rental_income: bool = True,
                    include_taxes: bool = True) -> Dict:
        """
        Calculate return on investment (ROI) for a property over a holding period.
        
        Args:
            property_data: Property data dictionary
            holding_period: Holding period in years
            annual_appreciation_rate: Annual property value appreciation rate
            include_rental_income: Whether to include rental income in ROI
            include_taxes: Whether to include taxes in calculation
            
        Returns:
            Dictionary with ROI calculations
        """
        # Extract property data
        initial_price = property_data.get('price', 0)
        closing_costs_buy = property_data.get('closing_costs_buy', initial_price * 0.07)  # Default 7% closing costs
        closing_costs_sell = property_data.get('closing_costs_sell', initial_price * 0.03)  # Default 3% selling costs
        
        # Calculate future value of the property
        final_price = initial_price * ((1 + annual_appreciation_rate) ** holding_period)
        
        # Calculate capital gain
        capital_gain = final_price - initial_price
        
        # Calculate capital gains tax if applicable
        capital_gains_tax = 0
        if include_taxes and capital_gain > 0:
            current_year = pd.Timestamp.now().year
            capital_gains_tax = self.calculate_capital_gains_tax(
                initial_price,
                final_price,
                current_year,
                current_year + holding_period,
                property_data.get('is_primary_residence', False)
            )
        
        # Calculate net profit from property appreciation
        net_appreciation_profit = capital_gain - capital_gains_tax - closing_costs_sell
        
        # Calculate rental income over the holding period if applicable
        rental_profit = 0
        if include_rental_income:
            # Calculate annual net rental income
            rental_yield_data = self.calculate_rental_yield(
                property_data,
                include_expenses=True,
                include_mortgage=True,
                include_taxes=include_taxes
            )
            
            annual_net_rental = rental_yield_data.get('net_income_after_tax', 0)
            rental_profit = annual_net_rental * holding_period
        
        # Calculate total profit
        total_profit = net_appreciation_profit + rental_profit
        
        # Calculate total investment
        total_investment = initial_price + closing_costs_buy
        
        # Calculate ROI metrics
        roi_percent = 0
        annualized_roi_percent = 0
        
        if total_investment > 0:
            roi_percent = (total_profit / total_investment) * 100
            annualized_roi_percent = ((1 + roi_percent / 100) ** (1 / holding_period) - 1) * 100
        
        # Prepare result
        result = {
            "initial_price": initial_price,
            "closing_costs_buy": round(closing_costs_buy, 2),
            "closing_costs_sell": round(closing_costs_sell, 2),
            "total_investment": round(total_investment, 2),
            "holding_period_years": holding_period,
            "annual_appreciation_rate_percent": round(annual_appreciation_rate * 100, 2),
            "final_price": round(final_price, 2),
            "capital_gain": round(capital_gain, 2),
            "capital_gains_tax": round(capital_gains_tax, 2),
            "net_appreciation_profit": round(net_appreciation_profit, 2),
            "rental_profit": round(rental_profit, 2),
            "total_profit": round(total_profit, 2),
            "roi_percent": round(roi_percent, 2),
            "annualized_roi_percent": round(annualized_roi_percent, 2)
        }
        
        logger.info(f"Calculated ROI: {result['roi_percent']}% over {holding_period} years "
                   f"({result['annualized_roi_percent']}% annualized)")
        
        return result
    
    def analyze_property_investment(self, property_data: Dict, analysis_params: Dict = None) -> Dict:
        """
        Perform a comprehensive investment analysis for a property.
        
        Args:
            property_data: Property data dictionary
            analysis_params: Additional parameters for analysis
            
        Returns:
            Dictionary with comprehensive investment analysis
        """
        # Set default analysis parameters if not provided
        if analysis_params is None:
            analysis_params = {}
        
        default_params = {
            "holding_period": 5,
            "annual_appreciation_rate": 0.03,
            "include_rental_income": True,
            "include_taxes": True,
            "include_mortgage": True
        }
        
        # Merge provided parameters with defaults
        for key, value in default_params.items():
            if key not in analysis_params:
                analysis_params[key] = value
        
        # Calculate rental yield
        rental_yield_data = self.calculate_rental_yield(
            property_data,
            include_expenses=True,
            include_mortgage=analysis_params["include_mortgage"],
            include_taxes=analysis_params["include_taxes"]
        )
        
        # Calculate ROI
        roi_data = self.calculate_roi(
            property_data,
            holding_period=analysis_params["holding_period"],
            annual_appreciation_rate=analysis_params["annual_appreciation_rate"],
            include_rental_income=analysis_params["include_rental_income"],
            include_taxes=analysis_params["include_taxes"]
        )
        
        # Combine results into a comprehensive analysis
        analysis = {
            "property_data": property_data,
            "rental_yield_analysis": rental_yield_data,
            "roi_analysis": roi_data,
            "analysis_parameters": analysis_params
        }
        
        return analysis
    
    def batch_analyze_properties(self, properties: List[Dict], analysis_params: Dict = None, output_file: str = None) -> Dict:
        """
        Perform investment analysis for multiple properties.
        
        Args:
            properties: List of property dictionaries
            analysis_params: Additional parameters for analysis
            output_file: Path to save output JSON file
            
        Returns:
            Dictionary with batch analysis results
        """
        start_time = pd.Timestamp.now()
        
        results = []
        
        for i, prop in enumerate(properties):
            try:
                logger.info(f"Analyzing property {i+1}/{len(properties)}")
                analysis = self.analyze_property_investment(prop, analysis_params)
                results.append(analysis)
            except Exception as e:
                logger.error(f"Error analyzing property {i+1}: {e}")
                results.append({
                    "property_data": prop,
                    "error": str(e)
                })
        
        # Calculate processing time
        processing_time = (pd.Timestamp.now() - start_time).total_seconds()
        
        # Save results if output file is provided
        if output_file:
            try:
                output_path = Path(output_file)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Saved batch analysis results to {output_file}")
                
            except Exception as e:
                logger.error(f"Error saving batch analysis results: {e}")
        
        # Generate summary statistics
        stats = {
            "total_properties": len(properties),
            "analysis_completed": len(results),
            "processing_time_seconds": processing_time,
            "output_file": output_file
        }
        
        # Calculate average metrics
        gross_yields = [r['rental_yield_analysis']['gross_yield_percent'] for r in results 
                        if 'rental_yield_analysis' in r and 'gross_yield_percent' in r['rental_yield_analysis']]
        
        net_yields = [r['rental_yield_analysis']['net_yield_after_tax_percent'] for r in results 
                     if 'rental_yield_analysis' in r and 'net_yield_after_tax_percent' in r['rental_yield_analysis']]
        
        rois = [r['roi_analysis']['roi_percent'] for r in results 
               if 'roi_analysis' in r and 'roi_percent' in r['roi_analysis']]
        
        if gross_yields:
            stats["avg_gross_yield_percent"] = round(sum(gross_yields) / len(gross_yields), 2)
        
        if net_yields:
            stats["avg_net_yield_percent"] = round(sum(net_yields) / len(net_yields), 2)
        
        if rois:
            stats["avg_roi_percent"] = round(sum(rois) / len(rois), 2)
        
        return {
            "stats": stats,
            "results": results if not output_file else None  # Return results in memory only if not saved to file
        }


def analyze_property_investments(input_file: str, output_file: str = None, tax_rates_file: str = None) -> Dict:
    """
    Analyze property investments from an input file.
    
    Args:
        input_file: Path to input JSON file with properties
        output_file: Path to save output. If None, derives from input file name.
        tax_rates_file: Path to tax rates JSON file
        
    Returns:
        Statistics about the analysis process
    """
    # Derive output file name if not provided
    if output_file is None:
        input_path = Path(input_file)
        output_file = str(input_path.parent / f"{input_path.stem}_analyzed{input_path.suffix}")
    
    try:
        # Load properties
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Ensure properties is a list
        if isinstance(data, dict) and 'properties' in data:
            properties = data['properties']
        elif isinstance(data, list):
            properties = data
        else:
            properties = [data]  # Single property
        
        # Initialize yield calculator
        calculator = YieldCalculator(tax_rates_file)
        
        # Batch analyze properties
        result = calculator.batch_analyze_properties(properties, output_file=output_file)
        
        return result['stats']
        
    except Exception as e:
        logger.error(f"Error in property investment analysis: {e}")
        return {
            "error": str(e),
            "success": False
        }


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/analysis/yield_calculator.log"),
            logging.StreamHandler()
        ]
    )
    
    # Example usage
    import argparse
    
    parser = argparse.ArgumentParser(description='Analyze property investments')
    parser.add_argument('input_file', help='Path to input JSON file with properties')
    parser.add_argument('--output_file', help='Path to save output file with analysis')
    parser.add_argument('--tax_rates', help='Path to tax rates JSON file')
    
    args = parser.parse_args()
    
    result = analyze_property_investments(args.input_file, args.output_file, args.tax_rates)
    print(json.dumps(result, indent=2))