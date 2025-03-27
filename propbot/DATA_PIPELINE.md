# PropBot Data Pipeline

This document outlines the data flow and processing pipeline for the PropBot real estate analysis system, which is a component of the larger `investmentbot` project.

## Data Structure

The PropBot system uses two main types of data:

1. **Sales Data**: Property listings for sale
2. **Rental Data**: Property listings for rent

Each data type follows a standardized pipeline from raw data to processed analytics:

```
Raw Data → Consolidation → Standardized CSV → Analysis → Reports
```

## Data Locations

### Sales Data

- **Primary source**: `idealista_listings.json` in the project root
- **Consolidated source**: `investmentbot/propbot/data/processed/sales_listings_consolidated.json`
- **Standardized CSV**: `investmentbot/propbot/data/processed/sales.csv`

### Rental Data

- **Primary sources**: 
  - `investmentbot/propbot/data/raw/rentals/rental_listings.json` (from scraping)
  - `rental_complete.csv` in the project root (legacy dataset)
- **Consolidated source**: `investmentbot/propbot/data/processed/rental_listings_consolidated.json`
- **Standardized CSV**: `investmentbot/propbot/data/processed/rentals.csv`

## Data Processing Workflow

### 1. Data Consolidation

The consolidation scripts combine data from multiple sources, removing duplicates based on URL:

- **For Sales**: `python3 -m investmentbot.propbot.consolidate_sales`
  - Combines data from `idealista_listings.json` and any additional sales files
  - Creates `sales_listings_consolidated.json`
  
- **For Rentals**: `python3 -m investmentbot.propbot.consolidate_rentals`
  - Combines data from `rental_listings.json` and `rental_complete.csv`
  - Creates `rental_listings_consolidated.json`

### 2. Standardization

The conversion scripts transform the consolidated JSON data into a standardized CSV format:

- **For Sales**: `python3 -m investmentbot.propbot.convert_sales_from_consolidated`
  - Converts `sales_listings_consolidated.json` to `sales.csv`
  - Extracts and standardizes fields like price, size, and room type
  
- **For Rentals**: `python3 -m investmentbot.propbot.convert_from_consolidated`
  - Converts `rental_listings_consolidated.json` to `rentals.csv`
  - Standardizes rental data fields

### 3. Analysis

The analysis scripts process the standardized data to generate insights:

- **Rental Income Analysis**: `python3 -m investmentbot.propbot.run_rental_analysis`
  - Uses both `sales.csv` and `rentals.csv`
  - Generates rental income reports in both JSON and CSV formats

## Key Features

- **Single Source of Truth**: Each data type has a single consolidated file that grows over time
- **Deduplication**: Properties are uniquely identified by URL to prevent duplicates
- **Metadata Tracking**: Each consolidation operation records history in a metadata file
- **Automatic Field Extraction**: Missing fields like size and room type are extracted from titles where possible
- **Standardized Format**: All data is transformed into a consistent CSV format for analysis

## Typical Usage After Scraping

After new data is scraped, run the following commands in sequence:

1. **Update consolidated files**:
   ```
   python3 -m investmentbot.propbot.consolidate_sales
   python3 -m investmentbot.propbot.consolidate_rentals
   ```

2. **Convert to standardized format**:
   ```
   python3 -m investmentbot.propbot.convert_sales_from_consolidated
   python3 -m investmentbot.propbot.convert_from_consolidated
   ```

3. **Run analysis**:
   ```
   python3 -m investmentbot.propbot.run_rental_analysis
   ```

This workflow ensures that new data is properly integrated with existing data before analysis. 