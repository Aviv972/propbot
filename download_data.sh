#!/bin/bash

echo "Downloading data from Heroku..."

# Create local directories if they don't exist
mkdir -p propbot/data/raw/sales/history
mkdir -p propbot/data/raw/rentals/history
mkdir -p propbot/data/processed
mkdir -p propbot/data/output/reports

# Download the sales listings from both temp directory and persistent storage
echo "Downloading sales listings..."
heroku run "cat /tmp/propbot_sales/idealista_listings.json 2>/dev/null || cat /app/propbot/data/raw/sales/idealista_listings.json 2>/dev/null || echo '{}'" > propbot/data/raw/sales/idealista_listings.json

# Download the rental listings from both temp directory and persistent storage
echo "Downloading rental listings..."
heroku run "cat /tmp/propbot_rentals/rental_listings.json 2>/dev/null || cat /app/propbot/data/raw/rentals/rental_listings.json 2>/dev/null || echo '{}'" > propbot/data/raw/rentals/rental_listings.json

# Download the processed reports
echo "Downloading processed reports..."
heroku run "cat /app/propbot/data/output/reports/investment_metrics_current.csv 2>/dev/null || echo ''" > propbot/data/output/reports/investment_metrics_current.csv
heroku run "cat /app/propbot/data/output/reports/rental_income_report_current.csv 2>/dev/null || echo ''" > propbot/data/output/reports/rental_income_report_current.csv

# Download the consolidated data
echo "Downloading consolidated data..."
heroku run "cat /app/propbot/data/processed/sales_listings_consolidated.json 2>/dev/null || echo '{}'" > propbot/data/processed/sales_listings_consolidated.json
heroku run "cat /app/propbot/data/processed/rental_listings_consolidated.json 2>/dev/null || echo '{}'" > propbot/data/processed/rental_listings_consolidated.json

# Get today's date for historical files
TODAY=$(date +%Y%m%d)

# Copy files to history folders
cp propbot/data/raw/sales/idealista_listings.json propbot/data/raw/sales/history/idealista_listings_${TODAY}.json
cp propbot/data/raw/rentals/rental_listings.json propbot/data/raw/rentals/history/rental_listings_${TODAY}.json

echo "Data download complete!"
echo "Files are saved in propbot/data/ directory structure." 