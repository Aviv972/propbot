#!/usr/bin/env python3
import pandas as pd

def check_sample_property():
    try:
        df = pd.read_csv('investment_summary_with_neighborhoods_updated.csv')
        if df is not None and len(df) > 0:
            # Select first property with discrepancies
            sample = df.iloc[0]
            print('=== SAMPLE PROPERTY FROM CSV ===')
            print(f'URL: {sample["Property URL"]}')
            print(f'Price: {sample["Price (€)"]:.2f}€')
            print(f'Monthly Rent: {sample["Monthly Rent (€)"]:.2f}€')
            print(f'Annual Rent: {sample["Annual Rent (€)"]:.2f}€')
            print(f'Total Recurring Expenses: {sample["Total Recurring Expenses (€)"]:.2f}€')
            print(f'NOI (stored): {sample["NOI (€)"]:.2f}€')
            
            # Calculate NOI
            calculated_noi = sample["Annual Rent (€)"] - sample["Total Recurring Expenses (€)"]
            print(f'NOI (calculated): {calculated_noi:.2f}€')
            print(f'Difference: {(sample["NOI (€)"] - calculated_noi):.2f}€')
            
            # Calculate Cap Rate
            stored_cap_rate = sample["Cap Rate (%)"]
            calculated_cap_rate = (calculated_noi / sample["Price (€)"]) * 100
            print(f'Cap Rate (stored): {stored_cap_rate:.2f}%')
            print(f'Cap Rate (calculated): {calculated_cap_rate:.2f}%')
            print(f'Difference: {(stored_cap_rate - calculated_cap_rate):.2f}%')
            
            # Find properties with largest discrepancies
            print("\n=== PROPERTIES WITH LARGEST NOI DISCREPANCIES ===")
            df['Calculated NOI'] = df['Annual Rent (€)'] - df['Total Recurring Expenses (€)']
            df['NOI Difference'] = df['NOI (€)'] - df['Calculated NOI']
            top_noi_diff = df.nlargest(3, 'NOI Difference')
            
            for i, row in top_noi_diff.iterrows():
                print(f"\nProperty: {row['Property URL']}")
                print(f"NOI (stored): {row['NOI (€)']:.2f}€")
                print(f"NOI (calculated): {row['Calculated NOI']:.2f}€")
                print(f"Difference: {row['NOI Difference']:.2f}€")
                print(f"Annual Rent: {row['Annual Rent (€)']:.2f}€")
                print(f"Total Recurring Expenses: {row['Total Recurring Expenses (€)']:.2f}€")
        else:
            print('No data found in CSV file')
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    check_sample_property() 