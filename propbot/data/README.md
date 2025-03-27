# PropBot Data Organization

This directory contains all data files used by PropBot for property investment analysis.

## Directory Structure

```
propbot/data/
├── raw/                   # Original, unmodified data
│   ├── scrape_history/    # Historical scrape data with dates
│   └── consolidated/      # Consolidated JSON files
├── processed/             # Clean, processed datasets ready for analysis
│   └── archive/           # Archived versions of old processed files
├── output/                # Analysis results and reports
│   ├── reports/           # All generated reports (rental income, investment metrics)
│   └── visualizations/    # Dashboard data and visualization outputs
└── metadata/              # Centralized location for all metadata files
```

## Data Flow

1. **Data Acquisition**
   - Web scraping produces raw data in `raw/scrape_history/YYYY-MM-DD/`
   - Consolidated data from multiple sources saved in `raw/consolidated/`

2. **Data Processing**
   - Raw data is cleaned and standardized into `processed/`
   - Current versions use the `_current` suffix (e.g., `rentals_current.csv`)
   - Previous versions are archived in `processed/archive/`

3. **Analysis & Reporting**
   - Analysis results saved to `output/reports/`
   - Visualizations and dashboards in `output/visualizations/`

4. **Metadata Tracking**
   - All metadata JSON files stored in `metadata/`

## Main Data Files

### Current Working Files

- `processed/rentals_current.csv` - Current rental listings dataset
- `processed/sales_current.csv` - Current property sales dataset
- `output/reports/rental_income_report_current.csv` - Latest rental income analysis
- `output/reports/investment_metrics_current.csv` - Latest investment metrics

### Visualization Files

- `output/visualizations/investment_dashboard.html` - Investment dashboard
- `output/visualizations/neighborhood_stats.json` - Neighborhood statistics

## Data Management Guidelines

1. Always use the `_current` suffix for the latest version of a file
2. When updating a file, move the old version to `archive/` with a timestamp
3. Keep raw data organized by date in `scrape_history/`
4. Reference data file paths from the manifest

For a complete listing of all data files and their purpose, refer to `propbot/data_manifest.json`. 