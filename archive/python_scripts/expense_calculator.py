import json
import os
import csv
from datetime import datetime

def log_message(message):
    """Log a message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def get_expense_parameters():
    """
    Define and return the default expense parameters for property investments.
    
    Returns:
        dict: Default expense parameters with recurring and one-time costs
    """
    return {
        # Recurring Annual Expenses
        "recurring": {
            "property_management": {
                "value": 0.10,  # 10% of annual rent
                "description": "Property Management (10% of annual rent)"
            },
            "maintenance": {
                "value": 0.01,  # 1% of home value
                "description": "Maintenance and Repairs (1% of home value)"
            },
            "vacancy": {
                "value": 2/12,  # 2 months of lost rent annually
                "description": "Vacancy (2 months of lost rent annually)"
            },
            "insurance": {
                "value": 360,  # €360 per year
                "description": "Insurance (€360 per year)"
            },
            "utilities": {
                "value": 1200,  # €1,200 per year
                "description": "Utilities (€1,200 per year)"
            }
        },
        # One-Time Costs
        "one_time": {
            "closing_costs": {
                "value": 0.04,  # 4% of purchase price
                "description": "Closing Costs (4% of purchase price)"
            },
            "taxes": {
                "description": "Property Transfer Tax (IMT) and Stamp Duty"
                # Calculated separately using calculate_taxes function
            }
        }
    }

def confirm_expenses(user_input=None):
    """
    Present the expense list for user confirmation and allow adjustments if necessary.
    
    Args:
        user_input (dict, optional): User-provided expense parameters to override defaults
            
    Returns:
        dict: Confirmed expense parameters
    """
    # Get default expense parameters
    expense_params = get_expense_parameters()
    
    # If user provided custom parameters, update the defaults
    if user_input:
        # Update recurring expenses
        if "recurring" in user_input:
            for expense_key, expense_data in user_input["recurring"].items():
                if expense_key in expense_params["recurring"]:
                    expense_params["recurring"][expense_key]["value"] = expense_data
        
        # Update one-time expenses
        if "one_time" in user_input:
            for expense_key, expense_data in user_input["one_time"].items():
                if expense_key in expense_params["one_time"] and expense_key != "taxes":
                    expense_params["one_time"][expense_key]["value"] = expense_data
    
    # Display the confirmed expenses
    log_message("Confirmed Expense Parameters:")
    log_message("Recurring Annual Expenses:")
    for name, data in expense_params["recurring"].items():
        log_message(f"  - {data['description']}")
    
    log_message("One-Time Costs:")
    for name, data in expense_params["one_time"].items():
        log_message(f"  - {data['description']}")
    
    return expense_params

def calculate_taxes(property_value):
    """
    Calculate the IMT (Property Transfer Tax) and Stamp Duty for a property.
    
    Args:
        property_value (float): The purchase price of the property in euros
        
    Returns:
        dict: Dictionary containing IMT, Stamp Duty, and total tax amounts
    """
    # IMT tax brackets
    imt_brackets = [
        {"max_value": 104261.00, "rate": 0.01, "deductible": 0.00},
        {"max_value": 142618.00, "rate": 0.02, "deductible": 1042.61},
        {"max_value": 194458.00, "rate": 0.05, "deductible": 5321.15},
        {"max_value": 324058.00, "rate": 0.07, "deductible": 9210.31},
        {"max_value": 621501.00, "rate": 0.08, "deductible": 12450.89},
        {"max_value": 1128287.00, "rate": 0.06, "deductible": 0.00},
        {"max_value": float('inf'), "rate": 0.075, "deductible": 0.00},
    ]
    
    # Find the appropriate tax bracket
    bracket = None
    for b in imt_brackets:
        if property_value <= b["max_value"]:
            bracket = b
            break
    
    if not bracket:
        # Use the highest bracket if no match found
        bracket = imt_brackets[-1]
    
    # Calculate IMT
    imt = (property_value * bracket["rate"]) - bracket["deductible"]
    imt = max(0, imt)  # Ensure IMT is not negative
    
    # Calculate Stamp Duty (0.8% of property value)
    stamp_duty = property_value * 0.008
    
    # Calculate total taxes
    total_taxes = imt + stamp_duty
    
    return {
        "imt": round(imt, 2),
        "stamp_duty": round(stamp_duty, 2),
        "total": round(total_taxes, 2),
        "bracket_info": {
            "rate": bracket["rate"],
            "deductible": bracket["deductible"]
        }
    }

def calculate_expenses(property_data, expense_params=None, rental_estimate=None):
    """
    Calculate all expenses for a property based on its purchase price and estimated rental income.
    
    Args:
        property_data (dict): Dictionary containing property information
        expense_params (dict, optional): Expense parameters to use, defaults to system defaults
        rental_estimate (float or dict, optional): Estimated monthly rental income or rental info dictionary
        
    Returns:
        dict: Dictionary containing all calculated expenses
    """
    if not expense_params:
        expense_params = get_expense_parameters()
    
    property_price = property_data.get('price', 0)
    
    # Handle the case where rental_estimate might be a dictionary
    monthly_rent = 0
    if isinstance(rental_estimate, dict):
        monthly_rent = rental_estimate.get('estimated_monthly_rent', 0)
    elif isinstance(rental_estimate, (int, float)):
        monthly_rent = rental_estimate
    
    # Determine if rental income is available
    has_rental_data = monthly_rent > 0
    
    # Calculate annual rental income
    annual_rent = monthly_rent * 12 if has_rental_data else 0
    
    # Calculate recurring expenses
    recurring_expenses = {}
    
    # Calculate expenses that depend on rental income
    if has_rental_data:
        recurring_expenses["property_management"] = annual_rent * expense_params["recurring"]["property_management"]["value"]
        recurring_expenses["vacancy"] = annual_rent * expense_params["recurring"]["vacancy"]["value"]
    else:
        # Mark rent-dependent expenses as N/A
        recurring_expenses["property_management"] = "N/A"
        recurring_expenses["vacancy"] = "N/A"
    
    # Calculate expenses that don't depend on rental income
    recurring_expenses["maintenance"] = property_price * expense_params["recurring"]["maintenance"]["value"]
    recurring_expenses["insurance"] = expense_params["recurring"]["insurance"]["value"]
    recurring_expenses["utilities"] = expense_params["recurring"]["utilities"]["value"]
    
    # Calculate total recurring expenses (only numeric values)
    numeric_recurring = {k: v for k, v in recurring_expenses.items() if isinstance(v, (int, float))}
    total_recurring = sum(numeric_recurring.values())
    
    # Calculate one-time expenses
    one_time_expenses = {
        "closing_costs": property_price * expense_params["one_time"]["closing_costs"]["value"]
    }
    
    # Calculate taxes
    taxes = calculate_taxes(property_price)
    one_time_expenses["taxes"] = taxes["total"]
    
    # Calculate total one-time expenses
    total_one_time = sum(one_time_expenses.values())
    
    # Prepare the expense summary
    expense_summary = {
        "property_url": property_data.get('url', ''),
        "property_price": property_price,
        "monthly_rent_estimate": monthly_rent if has_rental_data else "N/A",
        "annual_rent_estimate": annual_rent if has_rental_data else "N/A",
        "recurring_expenses": recurring_expenses,
        "total_recurring_expenses": total_recurring,
        "recurring_expenses_percent_of_rent": round(total_recurring / annual_rent * 100, 2) if has_rental_data and annual_rent > 0 else "N/A",
        "one_time_expenses": one_time_expenses,
        "total_one_time_expenses": total_one_time,
        "one_time_expenses_percent_of_price": round(total_one_time / property_price * 100, 2) if property_price > 0 else 0,
        "tax_details": {
            "imt": taxes["imt"],
            "stamp_duty": taxes["stamp_duty"],
            "tax_bracket_rate": taxes["bracket_info"]["rate"],
            "tax_bracket_deductible": taxes["bracket_info"]["deductible"]
        }
    }
    
    return expense_summary

def generate_expense_report(properties, rental_estimates=None):
    """
    Generate expense reports for a list of properties.
    
    Args:
        properties (list): List of property dictionaries
        rental_estimates (dict, optional): Dictionary mapping property URLs to rental estimates
        
    Returns:
        list: List of property expense summaries
    """
    log_message("Generating expense report for properties...")
    
    expense_reports = []
    expense_params = get_expense_parameters()  # Get default expense parameters
    
    for property_data in properties:
        property_url = property_data.get('url', '')
        
        # Get rental estimate for this property if available
        rental_estimate = None
        if rental_estimates and property_url in rental_estimates:
            rental_estimate = rental_estimates[property_url]
        
        # Create a new expense summary with the property URL
        expense_summary = {'url': property_url}
        
        # Add basic property information
        expense_summary['title'] = property_data.get('title', 'Unknown Property')
        expense_summary['price'] = property_data.get('price', 0)
        expense_summary['location'] = property_data.get('location', 'Unknown Location')
        expense_summary['size'] = property_data.get('size', 0)
        
        # Add rental information if available
        if rental_estimate:
            if isinstance(rental_estimate, dict):
                expense_summary['rental_info'] = {
                    'monthly_rent': rental_estimate.get('estimated_monthly_rent', 0),
                    'annual_rent': rental_estimate.get('estimated_annual_rent', 0),
                    'gross_rental_yield': rental_estimate.get('gross_rental_yield', 0),
                    'comparable_count': rental_estimate.get('comparable_count', 0)
                }
            else:
                expense_summary['rental_info'] = {
                    'monthly_rent': rental_estimate,
                    'annual_rent': rental_estimate * 12,
                    'gross_rental_yield': (rental_estimate * 12 / expense_summary['price']) * 100 if expense_summary['price'] > 0 else 0,
                    'comparable_count': 0
                }
        else:
            expense_summary['rental_info'] = {
                'monthly_rent': 0,
                'annual_rent': 0,
                'gross_rental_yield': 0,
                'comparable_count': 0
            }
        
        # Calculate expenses for this property
        expenses = calculate_expenses(
            property_data, 
            expense_params=expense_params,
            rental_estimate=rental_estimate
        )
        
        # Add expense calculations to the summary
        expense_summary['expenses'] = expenses
        
        expense_reports.append(expense_summary)
    
    log_message(f"Generated expense reports for {len(expense_reports)} properties")
    return expense_reports

def save_expense_report_to_json(expense_reports, filename="property_expense_report.json"):
    """
    Save the expense report to a JSON file.
    
    Args:
        expense_reports (list): List of property expense summaries
        filename (str): Output filename
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(expense_reports, f, indent=2, ensure_ascii=False)
        log_message(f"Expense report saved to {filename}")
        return True
    except Exception as e:
        log_message(f"Error saving expense report to JSON: {str(e)}")
        return False

def save_expense_report_to_csv(expense_reports, filename="property_expense_report.csv"):
    """
    Save the expense report to a CSV file.
    
    Args:
        expense_reports (list): List of property expense summaries
        filename (str): Output filename
        
    Returns:
        bool: True if saved successfully, False otherwise
    """
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            # Define CSV headers
            fieldnames = [
                'property_url', 'property_price', 'monthly_rent_estimate', 'annual_rent_estimate',
                'total_recurring_expenses', 'recurring_expenses_percent_of_rent',
                'total_one_time_expenses', 'one_time_expenses_percent_of_price',
                'property_management', 'maintenance', 'vacancy', 'insurance', 'utilities',
                'closing_costs', 'taxes', 'imt', 'stamp_duty'
            ]
            
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for report in expense_reports:
                # Flatten the nested dictionary structure for CSV
                row = {
                    'property_url': report['property_url'],
                    'property_price': report['property_price'],
                    'monthly_rent_estimate': report['monthly_rent_estimate'],
                    'annual_rent_estimate': report['annual_rent_estimate'],
                    'total_recurring_expenses': report['total_recurring_expenses'],
                    'recurring_expenses_percent_of_rent': report['recurring_expenses_percent_of_rent'],
                    'total_one_time_expenses': report['total_one_time_expenses'],
                    'one_time_expenses_percent_of_price': report['one_time_expenses_percent_of_price'],
                    'property_management': report['recurring_expenses']['property_management'],
                    'maintenance': report['recurring_expenses']['maintenance'],
                    'vacancy': report['recurring_expenses']['vacancy'],
                    'insurance': report['recurring_expenses']['insurance'],
                    'utilities': report['recurring_expenses']['utilities'],
                    'closing_costs': report['one_time_expenses']['closing_costs'],
                    'taxes': report['one_time_expenses']['taxes'],
                    'imt': report['tax_details']['imt'],
                    'stamp_duty': report['tax_details']['stamp_duty']
                }
                writer.writerow(row)
                
        log_message(f"Expense report saved to {filename}")
        return True
    except Exception as e:
        log_message(f"Error saving expense report to CSV: {str(e)}")
        return False

def run_expense_analysis(properties_for_sale, rental_estimates=None):
    """
    Run a complete expense analysis on properties for sale.
    
    Args:
        properties_for_sale (list): List of property dictionaries for sale
        rental_estimates (dict, optional): Dictionary mapping property URLs to rental estimates
    
    Returns:
        list: List of property expense summaries
    """
    log_message("Starting expense analysis for properties...")
    
    # Confirm expense parameters (using defaults)
    expense_params = confirm_expenses()
    
    # Generate expense reports
    expense_reports = generate_expense_report(properties_for_sale, rental_estimates)
    
    # Save the reports
    json_filename = "property_expense_report.json"
    csv_filename = "property_expense_report.csv"
    
    json_saved = save_expense_report_to_json(expense_reports, json_filename)
    csv_saved = save_expense_report_to_csv(expense_reports, csv_filename)
    
    if json_saved and csv_saved:
        log_message(f"Expense reports saved to {json_filename} and {csv_filename}")
    
    # Calculate and display summary statistics
    property_count = len(expense_reports)
    total_one_time_expenses = sum(report["total_one_time_expenses"] for report in expense_reports)
    total_recurring_expenses = sum(report["total_recurring_expenses"] for report in expense_reports)
    
    log_message(f"Expense Analysis Summary:")
    log_message(f"  Properties analyzed: {property_count}")
    if property_count > 0:
        log_message(f"  Average one-time expenses: €{total_one_time_expenses / property_count:.2f}")
        log_message(f"  Average annual recurring expenses: €{total_recurring_expenses / property_count:.2f}")
    
    return expense_reports

# If running as a standalone script
if __name__ == "__main__":
    # Import necessary functions when running standalone
    from rental_analysis import load_sales_data
    
    # Load sales data
    properties_for_sale = load_sales_data()
    
    if properties_for_sale:
        # Create a simple mapping of rental estimates for testing
        # In a real scenario, this would come from rental_income_report
        rental_estimates = {}
        
        # Run the expense analysis
        expense_reports = run_expense_analysis(properties_for_sale, rental_estimates)
        
        log_message(f"Analyzed expenses for {len(expense_reports)} properties")
    else:
        log_message("Error: Failed to load properties for sale") 