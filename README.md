# PropertyBot Investment Analysis System

A comprehensive data pipeline and analysis system for evaluating real estate investment opportunities.

## Overview

PropertyBot scrapes property listings, processes the data, analyzes rental yields and investment metrics, and generates visualizations to help identify the best investment opportunities.

## Project Structure

```
investmentbot/
├── propbot/                   # Main project code
│   ├── data/                  # Data directories
│   │   ├── raw/               # Original data files
│   │   │   ├── scrape_history/ # Raw scrape data organized by date
│   │   │   └── consolidated/  # Consolidated data files
│   │   ├── processed/         # Processed, standardized data
│   │   │   └── archive/       # Older versions of processed files
│   │   ├── output/            # Analysis results and visualization data
│   │   │   ├── reports/       # Generated reports and metrics
│   │   │   └── visualizations/ # Dashboard and visualization files
│   │   └── metadata/          # Metadata about the datasets
│   ├── analysis/              # Analysis modules
│   │   └── metrics/           # Rental and investment metrics calculation
│   ├── utils/                 # Utility functions
│   │   └── data_cleaning.py   # Data cleaning utilities
│   └── ui/                    # User interface assets
│       └── investment_dashboard.html # Dashboard HTML file
├── archive/                   # Archived old scripts and data
├── update_yields.py           # Script to run the yield analysis
├── check_yields.py            # Script to check yield distribution
└── propbot/data_manifest.json # Documentation of data files
```

## Data Organization

Data is organized into a structured pipeline:

1. **Raw Data** (`propbot/data/raw/`)
   - Scraped data from various sources
   - Data organized by date in `scrape_history/`
   - Consolidated listings in `consolidated/`

2. **Processed Data** (`propbot/data/processed/`)
   - Cleaned, standardized datasets ready for analysis
   - Current versions use the suffix `_current` (e.g., `sales_current.csv`)
   - Previous versions archived in `processed/archive/`

3. **Output** (`propbot/data/output/`)
   - Analysis results in `reports/`
   - Visualizations and dashboards in `visualizations/`

4. **Metadata** (`propbot/data/metadata/`)
   - Information about datasets and processing history

## Key Files

- `propbot/data/processed/rentals_current.csv` - Current rental property listings
- `propbot/data/processed/sales_current.csv` - Current property sales listings
- `propbot/data/output/reports/rental_income_report_current.csv` - Rental income analysis
- `propbot/data/output/reports/investment_metrics_current.csv` - Investment metrics
- `propbot/data/output/visualizations/investment_dashboard.html` - Interactive dashboard

## Running the System

### Updating Rental Data

To scrape new rental listings and update the consolidated data:

```bash
python propbot/scrape_additional_rentals.py
python propbot/consolidate_rentals.py
```

### Processing Data

To convert consolidated data to standardized format:

```bash
python propbot/convert_from_consolidated.py
python propbot/convert_sales_from_consolidated.py
```

### Generating Yields and Investment Metrics

To calculate rental yields and generate investment metrics:

```bash
python update_yields.py
```

### Viewing Analytics

To check the distribution of yields:

```bash
python check_yields.py
```

To generate the investment dashboard:

```bash
python propbot/dashboard_generator.py
```

Then open `propbot/data/output/visualizations/investment_dashboard.html` in a web browser.

## Data Management Guidelines

1. Always use the `_current` suffix for the latest version of a file
2. When updating a file, move the old version to `archive/` with a timestamp
3. Keep raw data organized by date in `scrape_history/`
4. Reference data file paths from the manifest

For a complete listing of all data files and their purpose, refer to `propbot/data_manifest.json`.