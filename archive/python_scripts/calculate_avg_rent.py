#!/usr/bin/env python3
import pandas as pd
import re

# Load the CSV file
df = pd.read_csv('rental_data_2025-03_fixed.csv', header=None)

# Extract the rent values
prices = []
for rent in df[2]:
    if isinstance(rent, str) and '/month' in rent:
        match = re.search(r'(\d+)€/month', rent)
        if match:
            prices.append(int(match.group(1)))

# Calculate the average
if prices:
    avg_rent = sum(prices) / len(prices)
    print(f'Total properties with rent data: {len(prices)}')
    print(f'Average Monthly Rent: €{avg_rent:.2f}') 