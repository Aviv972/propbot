# PropBot Changelog

## [1.2.0] - 2025-03-22

### Added
- New data cleaning module in `propbot/data_processing/data_cleaner.py` with JSON to CSV conversion
- New utility module for data validation in `propbot/utils/data_validator.py`
- Best properties finder functionality added to `propbot/analysis/metrics/investment_metrics.py`
- New command-line options in `main.py`:
  - `--convert`: Convert rental data JSON to CSV format
  - `--validate`: Validate and fix investment metrics in CSV file
  - `--find-best`: Find the best N investment properties and generate a report
- UI directory structure for storing HTML reports
- Comprehensive README.md with feature list and command examples

### Changed
- Updated file paths in `config.py` to store HTML reports in the UI directory
- Enhanced `main.py` to integrate all new functionality
- Added validation step after analysis to ensure metric accuracy
- Improved the project documentation

### Integrated
The following standalone scripts have been integrated into the PropBot structure:
1. `convert_rental_data.py` → `propbot/data_processing/data_cleaner.py`
2. `fix_investment_metrics.py` → `propbot/utils/data_validator.py`
3. `find_best_properties.py` → `propbot/analysis/metrics/investment_metrics.py`
4. `create_neighborhood_report.py` → `propbot/report_generator.py`
5. `check_csv_parsing.py`, `check_csv_patterns.py` → Data validation utilities

## [1.1.0] - 2025-03-20

### Added
- Investment summary generation with 18 key metrics
- Property classification system based on investment criteria
- Dashboard generation for visualizing investment metrics
- HTML report generation for easier data viewing

### Changed
- Enhanced neighborhood analysis with better location matching
- Improved expense and tax calculations
- Updated data processing pipeline

## [1.0.0] - 2025-03-15

### Initial Release
- Property data collection from multiple sources
- Rental income estimation based on comparable properties
- Expense and tax calculations for investment properties
- Basic investment metrics calculation
- CSV report generation 