#!/usr/bin/env python3

def test_metrics():
    # Sample property with well-defined values
    property_data = {
        'Property URL': 'https://example.com/sample-property',
        'Price (€)': 300000,             # Price: 300,000€
        'Size (sqm)': 75,                # Size: 75 sqm
        'Monthly Rent (€)': 1200,        # Monthly Rent: 1,200€
        'Total Recurring Expenses (€)': 3600  # Annual recurring expenses: 3,600€
    }
    
    # Calculate metrics
    print("===== SAMPLE PROPERTY METRICS =====")
    print(f"Property: {property_data['Property URL']}")
    print(f"Price: {property_data['Price (€)']:,.2f}€")
    print(f"Size: {property_data['Size (sqm)']:,.2f} sqm")
    print(f"Monthly Rent: {property_data['Monthly Rent (€)']:,.2f}€")
    print(f"Total Recurring Expenses (annual): {property_data['Total Recurring Expenses (€)']:,.2f}€")
    print("\n--- Calculated Metrics ---")
    
    # 1. Annual Rent
    annual_rent = property_data['Monthly Rent (€)'] * 12
    print(f"Annual Rent = Monthly Rent × 12")
    print(f"Annual Rent = {property_data['Monthly Rent (€)']:,.2f}€ × 12 = {annual_rent:,.2f}€")
    
    # 2. Price per sqm
    price_per_sqm = property_data['Price (€)'] / property_data['Size (sqm)']
    print(f"\nPrice per sqm = Price ÷ Size")
    print(f"Price per sqm = {property_data['Price (€)']:,.2f}€ ÷ {property_data['Size (sqm)']:,.2f} sqm = {price_per_sqm:,.2f}€/sqm")
    
    # 3. Gross Yield
    gross_yield = (annual_rent / property_data['Price (€)']) * 100
    print(f"\nGross Yield = (Annual Rent ÷ Price) × 100")
    print(f"Gross Yield = ({annual_rent:,.2f}€ ÷ {property_data['Price (€)']:,.2f}€) × 100 = {gross_yield:.2f}%")
    
    # 4. NOI (Net Operating Income)
    noi = annual_rent - property_data['Total Recurring Expenses (€)']
    print(f"\nNOI = Annual Rent - Total Recurring Expenses")
    print(f"NOI = {annual_rent:,.2f}€ - {property_data['Total Recurring Expenses (€)']:,.2f}€ = {noi:,.2f}€")
    
    # 5. Cap Rate
    cap_rate = (noi / property_data['Price (€)']) * 100
    print(f"\nCap Rate = (NOI ÷ Price) × 100")
    print(f"Cap Rate = ({noi:,.2f}€ ÷ {property_data['Price (€)']:,.2f}€) × 100 = {cap_rate:.2f}%")
    
    # Summary
    print("\n===== SUMMARY OF CALCULATED METRICS =====")
    print(f"Annual Rent: {annual_rent:,.2f}€")
    print(f"Price per sqm: {price_per_sqm:,.2f}€/sqm")
    print(f"Gross Yield: {gross_yield:.2f}%")
    print(f"NOI (Net Operating Income): {noi:,.2f}€")
    print(f"Cap Rate: {cap_rate:.2f}%")

if __name__ == "__main__":
    test_metrics() 