# PropBot Integration Plan

PropBot Recommended Directory Structure

```
investmentbot/
│
├── propbot/                       # PropBot application
│   ├── scrapers/                  # Data collection module
│   │   ├── __init__.py
│   │   ├── idealista_scraper.py   # Sales property scraper
│   │   ├── rental_scraper.py      # Rental property scraper
│   │   ├── scrape_additional_rentals.py
│   │   └── scheduler.py           # Scheduling automation
│   │
│   ├── data_processing/           # Data normalization and location matching
│   │   ├── __init__.py
│   │   ├── location_matching.py   # Location standardization functions
│   │   ├── data_cleaner.py        # Data cleaning and validation
│   │   ├── property_matcher.py    # Find comparable properties
│   │   └── incremental_updater.py # Incremental update strategies
│   │
│   ├── analysis/                  # Analysis modules
│   │   ├── __init__.py
│   │   ├── expense/               # Expense and tax calculations
│   │   │   ├── __init__.py
│   │   │   ├── expense_calculator.py  # Property expenses calculator
│   │   │   └── tax_calculator.py  # Tax calculation functions
│   │   │
│   │   └── metrics/               # Investment metrics calculations
│   │       ├── __init__.py
│   │       ├── rental_metrics.py  # Rental income estimations
│   │       ├── investment_metrics.py  # ROI, cap rate calculations
│   │       └── segmentation.py    # Neighborhood segmentation
│   │
│   ├── reporting/                 # Summary and reporting tools
│   │   ├── __init__.py
│   │   ├── investment_summary.py  # Generate investment summaries
│   │   ├── neighborhood_report.py # Neighborhood-specific reports
│   │   ├── report_formatter.py    # Format reports (HTML/CSV/JSON)
│   │   └── report_exporter.py     # Export reports to different formats
│   │
│   ├── ui/                        # User interface components
│   │   ├── __init__.py
│   │   ├── static/                # Static assets
│   │   │   ├── css/
│   │   │   ├── js/
│   │   │   │   └── investment_filter.js  # Frontend filtering
│   │   │   └── images/
│   │   ├── templates/             # HTML templates
│   │   │   ├── base.html
│   │   │   ├── dashboard.html
│   │   │   └── reports.html
│   │   └── app.py                 # Web application entry point
│   │
│   ├── utils/                     # Utility modules
│   │   ├── __init__.py
│   │   ├── error_logger.py        # Centralized error logging
│   │   ├── notification.py        # Error notification system
│   │   └── config_loader.py       # Configuration loading utilities
│   │
│   ├── data/                      # Data storage
│   │   ├── raw/                   # Raw scraped data
│   │   │   ├── sales/             # Sales property data
│   │   │   └── rentals/           # Rental property data
│   │   └── processed/             # Processed data
│   │       ├── properties.db      # SQLite database (optional)
│   │       ├── investment_summary.csv
│   │       └── neighborhood_data.json
│   │
│   ├── tests/                     # Test suite
│   │   ├── __init__.py
│   │   ├── test_scrapers.py
│   │   ├── test_data_processing.py
│   │   ├── test_analysis.py
│   │   └── test_reporting.py
│   │
│   ├── main.py                    # Main application entry point
│   ├── setup.py                   # Package installation script
│   └── requirements.txt           # Package dependencies
│
├── config/                        # Project-level configuration files
│   ├── settings.py                # General settings
│   ├── neighborhoods.json         # Neighborhood definitions
│   ├── tax_tables.json            # Tax rate tables
│   └── expense_defaults.json      # Default expense parameters
│
├── docs/                          # Documentation
│   ├── README.md
│   ├── INTEGRATION_PLAN.md        # The integration plan
│   └── api_docs/                  # API documentation
│
├── .env.example                   # Example environment variables
├── .gitignore                     # Git ignore file
└── README.md                      # Project overview
```

## Key Design Principles

1. **Modular Organization**: The structure separates concerns into distinct modules (scrapers, data processing, analysis, reporting, UI).

2. **Package-Based Approach**: Each directory contains an `__init__.py` file, making it a proper Python package.

3. **Clear Separation of Code and Data**: Code is organized in the functional modules, while data is stored in dedicated data directories.

4. **Configuration Management**: A separate config directory for all configuration files.

5. **Comprehensive Testing**: A dedicated tests directory mirroring the structure of the main code.

6. **Documentation**: A docs directory for comprehensive project documentation.

## Module Responsibilities

### Data Flow

1. **Scrapers** collect data from real estate websites → stored in `investmentbot/propbot/data/raw/`
2. **Data Processing** cleans and normalizes the data → stored in `investmentbot/propbot/data/processed/`
3. **Analysis** performs expense and investment metrics calculations
4. **Reporting** generates summaries and reports
5. **UI** presents the data to users via web interface

### Cross-Cutting Concerns

- **Utils** provides common utilities used across all modules
- **Config** centralizes all configuration parameters
- **Tests** ensures reliability of all components

## Deployment Considerations

- The structure supports different deployment models (monolithic or microservices)
- Environment variables can be used for configuration in production
- The setup.py allows for installation as a package

This structure balances simplicity with scalability, allowing for future extension while maintaining clear organization of the current feature set. 

## 1. Data Collection Module

### Scraping Functions

**idealista_scraper.py & rental_scraper.py**:
- Fetch sales and rental listings using the ScrapingBee API
- Use premium proxies and JavaScript rendering as needed to bypass anti-bot measures

**scrape_additional_rentals.py**:
- Construct and iterate through paginated URLs to collect additional rental data

### Scheduler

**scheduler.py / monthly_rental_scheduler.py**:
- Schedule the scraping tasks (daily for sales data, monthly for rental data)

## 2. Data Normalization and Location Matching

- **Location Standardization**:
  - `investmentbot/propbot/analysis/metrics/rental_metrics.py`: 
    - `standardize_location()`: Clean and standardize location strings for comparison
    - `extract_neighborhoods()`: Extract neighborhood terms from location strings
    - `extract_parish()`: Extract parish/district names from locations
    - `extract_neighborhood()`: Match exact neighborhood names in location strings
    - `calculate_location_similarity()`: Calculate similarity between locations (fuzzy matching)

- **Rental Analysis Enhancements**:
  - `investmentbot/propbot/analysis/metrics/rental_metrics.py`: 
    - `load_complete_rental_data()`: Load and process rental data with filtering
    - `find_comparable_properties()`: Find similar rental properties based on size, location, and room type
    - `calculate_average_rent()`: Calculate average rent from comparables
    - `generate_income_report()`: Create rental income estimates for properties

## 3. Expense and Tax Analysis

### Expense Calculation

**expense_calculator.py**:
- Retrieve default expense parameters
- Calculate recurring expenses (property management, maintenance, vacancy loss, insurance, utilities)

### Tax Calculation

**calculate_taxes(property_value)**:
- Use the provided tax table to compute IMT (apply the rate and subtract the deductible) and Stamp Duty (0.8% of the property value)

### Confirm Expenses

- Allow user confirmation or adjustments for expense parameters

## 4. Investment Metrics Calculation

- **Rental Income Estimation**:
  - `investmentbot/propbot/analysis/metrics/rental_metrics.py`: 
    - `run_improved_analysis()`: Main function for running rental analysis
    - `generate_income_report()`: Calculate rental income and yield for each property

## 5. Investment Summary and Reporting

### Summary Generation

**create_investment_summary**:
- For each property, compile key metrics:
  - Purchase Price, Monthly/Annual Rent, Expense Breakdown, Total Recurring Expenses, Recurring Expenses (% of Rent)
  - Total One-Time Expenses, One-Time Expenses (% of Price)
  - Expense components (management, maintenance, vacancy, insurance, utilities)
  - Closing Costs, Total Taxes (IMT and Stamp Duty breakdown)
  - NOI, Cap Rate, Cash on Cash Return, Monthly Cash Flow
  - Price per sqm, and segmentation details

### Presentation

**present_investment_summary**:
- Format the summary as a comparative table (HTML/CSV) for easy analysis

### Additional Reporting

**create_neighborhood_report.py & update_investment_html.py**:
- Generate detailed neighborhood reports highlighting market trends

**save_report_to_json / save_report_to_csv**:
- Persist reports in JSON and CSV formats

## 6. User Interface and Filtering

### Frontend Module (investment_filter.js)

- Provide interactive table controls to filter, sort, and highlight top deals based on investment metrics
- Update row counts and display real-time summaries for user decision-making

## 7. Overall Integration Workflow

1. **Data Collection**:
   - Run scrapers to collect property sales and rental data
   - Store data in `investmentbot/propbot/data/raw/` directory

2. **Data Normalization**:
   - Process raw data using standardization functions
   - Store normalized data in `investmentbot/propbot/data/processed/` directory

3. **Expense and Income Analysis**:
   - Run `investmentbot/propbot/run_rental_analysis.py` to generate rental income reports
   - Calculate expenses for investment properties

4. **Investment Summary Generation**:
   - Combine rental estimates with expense data
   - Generate investment summary reports in JSON and CSV format

5. **HTML Report Generation**:
   - Create interactive HTML reports for visualizing results
   - Store reports in `investmentbot/propbot/data/processed/` directory

6. **Scheduled Updates**:
   - Regularly update data using incremental collection
   - Regenerate reports with latest data

## 8. Error Handling Strategy

### Data Collection Error Handling

**API Failure Recovery**:
- Implement exponential backoff retry logic in idealista_scraper.py and rental_scraper.py
- Log failed requests with timestamps and error codes
- Automatically resume scraping from the last successful page

**Credit Limit Management**:
- Monitor ScrapingBee credit usage via load_credits_usage() and update_credits_usage()
- Implement early termination if approaching credit limits
- Cache partial results to avoid data loss during interruptions

**Data Validation**:
- Verify minimum required fields in scraped listings
- Flag and log malformed or suspicious data for manual review

### Processing Error Handling

**Robust Data Extraction**:
- Enhance extraction functions (extract_size, extract_room_type, extract_neighborhood) with fallback patterns and confidence scoring

**Location Matching Fallbacks**:
- Use hierarchical matching in improve_location_matching.py (neighborhood → district → city)
- Maintain an "unmatched locations" report for further review

**Calculation Safety**:
- Protect against division-by-zero and null values in metric calculations
- Use default values when data is missing and generate calculation warnings

### Centralized Error Management

**Error Logging Service**:
- Create a centralized error_logger.py module with severity levels and context data

**Error Notification System**:
- Implement email or Slack alerts for critical errors
- Generate a daily digest of non-critical issues

**Recovery Procedures**:
- Document automated and manual recovery steps for common failures
- Implement a "safe mode" to run with reduced functionality when components fail

## 9. Incremental Update Strategy

### Property Tracking System

**Unique Identifier Management**:
- Extract and maintain consistent property IDs from URLs or listings
- Implement a persistent registry (or hashing mechanism) for properties

**Change Detection**:
- Compare new listings against existing data using property IDs
- Flag significant changes (e.g., price drops >5%) for reporting

### Differential Scraping

**Targeted Collection**:
- Prioritize new and recently updated listings
- Implement "light" (metadata only) and "deep" (full details) scraping modes

**Partial Data Updates**:
- Update only changed fields rather than replacing entire records
- Ensure atomic updates to avoid data corruption

### Historical Data Management

**Version Control**:
- Track historical changes for each property (e.g., price adjustments, time on market)

**Audit Trail**:
- Log all modifications with timestamps and change sources
- Implement rollback mechanisms and generate data freshness reports

### Optimized Scheduling

**Adaptive Scheduling**:
- Adjust scraping frequency based on market activity and last full update

**Partial Update Cycles**:
- Daily: New listings and price changes
- Weekly: Full details for active listings
- Monthly: Comprehensive rental data update
- Quarterly: Recalculate neighborhood statistics

**Update Coordination**:
- Ensure dependent data (sales listings and rental comparables) are updated in logical sequence
- Use "update markers" to trigger downstream processes and optimize update windows

## Final Remarks

This integration plan for PropBot outlines a comprehensive, modular approach to building a property investment analysis tool. It covers:

- Data collection via scheduled scraping (with premium proxies and JS rendering)
- Data normalization and enhanced location matching
- Expense and tax analysis using predefined parameters and tax tables
- Detailed calculation of investment metrics
- Segmentation of properties by neighborhood and comparative pricing
- Investment summary generation and interactive reporting
- Robust error handling, incremental updates, and optimized scheduling

This plan ensures that all modules work together cohesively to provide investors with reliable, up-to-date insights into the Lisbon real estate market.

Happy coding and investing! 