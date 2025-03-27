# PropBot - Property Data Analysis Tool

PropBot is a comprehensive data processing and analysis tool for real estate property data. It processes both sales and rental listings through a flexible, modular pipeline.

## Features

- **Modular Data Processing Pipeline**: Validation, consolidation, and conversion of property data
- **Support for Multiple Formats**: Processes JSON and CSV data 
- **Property Type Handling**: Specialized processing for both sales and rental properties
- **Robust Error Handling**: Graceful recovery from errors and detailed logging
- **Command-line Interface**: Easy to use CLI for all operations

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/propbot.git

# Install requirements
pip install -r requirements.txt
```

## Project Structure

```
propbot/
├── __init__.py
├── main.py                        # Main entry point
├── config.py                      # Configuration handling
├── data_processing/               # Data processing modules
│   ├── __init__.py
│   ├── consolidation/             # Data consolidation modules
│   │   ├── __init__.py
│   │   ├── rentals.py             # Rental consolidation
│   │   └── sales.py               # Sales consolidation
│   ├── conversion/                # Data conversion modules
│   │   ├── __init__.py
│   │   ├── rentals.py             # Rental conversion
│   │   └── sales.py               # Sales conversion
│   ├── validation/                # Data validation modules
│   │   ├── __init__.py
│   │   ├── precheck.py            # Basic validation
│   │   └── schemas.py             # Schema validation
│   ├── pipeline/                  # Pipeline modules
│   │   ├── __init__.py
│   │   └── standard.py            # Standard pipeline
│   └── utils/                     # Utility functions
│       └── __init__.py
├── ui/                            # User interface components
│   └── __init__.py
└── data/                          # Data directory
    ├── raw/                       # Raw data files
    │   ├── sales/                 # Sales data
    │   └── rentals/               # Rental data
    ├── processed/                 # Processed data files
    └── logs/                      # Log files
```

## Usage

### Basic Usage

```bash
# Run the full pipeline for both sales and rentals
python -m propbot.main

# Run only sales pipeline
python -m propbot.main --type sales

# Run only rentals pipeline
python -m propbot.main --type rentals

# Skip validation step
python -m propbot.main --skip-validation

# Continue pipeline even if errors occur
python -m propbot.main --force-continue

# Specify a different data directory
python -m propbot.main --data-dir /path/to/data

# Run in test mode with sample data
python -m propbot.main --test
```

### Pipeline Steps

The PropBot pipeline consists of three main steps:

1. **Validation**: Checks the input data for correctness and completeness
2. **Consolidation**: Merges data from multiple sources, handling duplicates
3. **Conversion**: Converts consolidated data to a standardized CSV format

Each step can be run independently or skipped as needed.

## Configuration

PropBot looks for a configuration file at `~/.propbot/config.json`. You can specify a different configuration file with the `--config` parameter.

Example configuration:

```json
{
    "data_dir": "/path/to/data",
    "log_level": "INFO",
    "default_currency": "EUR"
}
```

## Development

### Running Tests

```bash
# Run end-to-end test
./propbot/test_pipeline.sh
```

### Adding New Data Sources

To add a new data source:

1. Place the raw data files in the appropriate directory (`data/raw/sales/` or `data/raw/rentals/`)
2. Ensure file formats conform to the expected schema
3. Run the pipeline with `python -m propbot.main`

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

# PropBot Data Management

## Overview

PropBot collects, processes, and analyzes real estate property data from Lisbon's property market to enable better investment decisions. This README explains how data is managed within the system.

## Data Structure

PropBot maintains two primary consolidated datasets:

1. **Sales Data** (`investmentbot/propbot/data/processed/sales.csv`): Contains all property listings for sale
2. **Rental Data** (`investmentbot/propbot/data/processed/rentals.csv`): Contains all rental property listings

These two datasets serve as the single source of truth for all analysis operations.

## Data Flow

The data flows through PropBot in three distinct stages:

### 1. Collection (Scraping)

Property data is collected from real estate websites and stored in raw JSON format:

- **Sales Listings**: `investmentbot/propbot/data/raw/sales/idealista_listings.json`
- **Rental Listings**: `investmentbot/propbot/data/raw/rentals/rental_listings.json`

### 2. Processing

Raw data is processed to:

- Clean and normalize the data
- Extract key fields (price, size, room type)
- Fix any parsing issues
- Consolidate into standardized CSV files

The processing is handled by:
- `propbot/data_processor.py`: Main processing script that transforms raw JSON to standardized CSV
- `propbot/convert_rental_data.py`: Script for fixing and converting rental data
- `propbot/convert_sales_data.py`: Script for fixing and converting sales data

### 3. Analysis

Analyses are performed using the consolidated datasets:

- **Rental Income Estimation**: Calculates estimated rental income for sales properties
- **Investment Metrics**: Calculates ROI, cap rates, and other investment metrics
- **Expense Analysis**: Calculates property expenses and taxes

## Key Files and Their Purpose

- `investmentbot/propbot/data/processed/sales.csv`: Consolidated sales data
- `investmentbot/propbot/data/processed/rentals.csv`: Consolidated rental data
- `investmentbot/propbot/data/processed/metadata.json`: Metadata about the datasets, including processing history
- `propbot/data_processor.py`: Main script for processing raw data into consolidated datasets
- `propbot/convert_rental_data.py`: Script to convert rental data with fixed parsing
- `propbot/convert_sales_data.py`: Script to convert sales data with fixed parsing

## Standardized Data Format

Both datasets share a common structure with these fields:

| Field | Description |
|-------|-------------|
| url | Unique identifier for the property |
| price | Price in euros |
| size | Size in square meters |
| room_type | Number of rooms (T0, T1, T2, etc.) |
| location | Property location |
| is_rental | Boolean indicating if it's a rental (true) or sale (false) |
| details | Additional property details |
| snapshot_date | Date the data was captured |

## Data Validation

The data processing pipeline includes validation checks to ensure:

1. **Size Extraction**: Properly extracts size values, including fixing the issue with prefixed digits (e.g., "270 m²" → "70 m²")
2. **Room Type Extraction**: Correctly identifies T0, T1, T2, etc.
3. **Price Cleaning**: Properly extracts numeric price values from formatted strings:
   - For rentals: Converts prices like "1,40€/month" to 1400.0
   - For sales: Converts prices like "275,000€" to 275000.0
4. **Data Completeness**: Ensures required fields are present

## Running the Data Processor

To process raw data and generate the consolidated datasets:

```bash
python -m investmentbot.propbot.data_processor
```

To fix issues in the existing rental data:

```bash
python -m investmentbot.propbot.convert_rental_data
```

To fix issues in the existing sales data:

```bash
python -m investmentbot.propbot.convert_sales_data
```

## Best Practices

1. **Always use the consolidated datasets** for analysis, not the raw data
2. **Update the consolidated datasets** when new raw data is available
3. **Check the metadata** to ensure you're using the latest data
4. **Fix parsing issues** in the processor, not downstream in the analysis

