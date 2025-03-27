# PropBot - Property Investment Analysis Tool

## Project Function Reference

This document provides a comprehensive reference of all functions across the PropBot codebase, organized by file.

## JavaScript Files

### investment_filter.js
- **processTableData()** - Processes table data to add data attributes to each cell for filtering and sorting
- **createFilterControls()** - Creates UI controls for filtering properties by various metrics
- **initTableSorting()** - Adds sorting functionality to table headers
- **createResetButton()** - Creates a button to reset all filters
- **highlightTopDeals()** - Highlights top-performing properties in the table
- **initStickyHeaderBehavior()** - Manages sticky header appearance during scrolling
- **applyFilters()** - Applies selected filters to the property table
- **resetFilters()** - Resets all filters to their default state
- **sortTable()** - Sorts the table based on a specified column and direction
- **updateTableDisplay()** - Updates the display of filtered and sorted table rows
- **setupFilterButtons()** - Sets up filter button behaviors
- **updateRowCount()** - Updates the displayed count of visible/filtered properties

## Core Python Files

### expense_calculator.py
- **log_message(message)** - Logs a message with timestamp
- **get_expense_parameters()** - Returns default expense parameters for property investments
- **confirm_expenses(user_input=None)** - Gets user confirmation for expense parameters
- **calculate_taxes(property_value)** - Calculates taxes for a property based on its value
- **calculate_expenses(property_data, expense_params=None, rental_estimate=None)** - Calculates expenses for a property
- **generate_expense_report(properties, rental_estimates=None)** - Generates expense reports for multiple properties
- **save_expense_report_to_json(expense_reports, filename="property_expense_report.json")** - Saves expense reports to JSON
- **save_expense_report_to_csv(expense_reports, filename="property_expense_report.csv")** - Saves expense reports to CSV
- **run_expense_analysis(properties_for_sale, rental_estimates=None)** - Runs expense analysis on a list of properties

### find_best_properties.py
- **log_message(message)** - Logs a message with timestamp
- **load_expense_report(filename="property_expense_report.json")** - Loads property expense report from JSON
- **calculate_metrics(property_data)** - Calculates investment metrics for properties
- **find_best_properties()** - Identifies the best properties based on investment metrics

### idealista_scraper.py
- **log_message(message)** - Logs a message with timestamp
- **load_stored_listings()** - Loads previously stored property listings from JSON
- **save_listings(listings)** - Saves property listings to JSON
- **load_credits_usage()** - Loads ScrapingBee credits usage data
- **update_credits_usage(credits_used)** - Updates ScrapingBee credits usage records
- **fetch_page(url, page_num=1)** - Fetches a web page using ScrapingBee API
- **extract_properties(html_content)** - Extracts property data from HTML content
- **run_scraper()** - Runs the web scraper to collect property listings

### improve_location_matching.py
- **log_message(message)** - Logs a message with timestamp
- **load_sales_data(filename="idealista_listings.json")** - Loads property sales data
- **load_rental_data(filename="rental_data_2025-03.csv")** - Loads rental data
- **create_location_mapping()** - Creates mapping for standardizing locations
- **standardize_location(location, mapping)** - Standardizes location strings
- **find_best_match(location, standard_locations, threshold=70)** - Finds best match for location names
- **generate_location_report(sales_data, rental_locations, mapping)** - Generates location matching report
- **enhance_rental_analysis()** - Enhances rental analysis with improved location matching

### improved_rental_analysis.py
- **extract_size(details)** - Extracts property size from details text
- **extract_room_type(details)** - Extracts room type from details text
- **extract_neighborhood(location_str)** - Extracts neighborhood from location string
- **load_property_data(json_file)** - Loads property data from JSON file
- **load_complete_rental_data(filename)** - Loads complete rental data from file
- **standardize_location(location_text)** - Standardizes location text
- **find_comparable_properties(property_data, rental_properties, size_tolerance_pct=0.2)** - Finds comparable rental properties
- **calculate_average_rent(comparables)** - Calculates average rent from comparable properties
- **generate_income_report(properties_for_sale, rental_properties, similarity_threshold=75)** - Generates rental income report
- **run_improved_analysis(similarity_threshold=75)** - Runs improved rental analysis

### monthly_rental_scheduler.py
- **scheduled_monthly_job()** - Scheduled job for monthly rental data collection
- **main()** - Main function to run the scheduler

### rental_analysis.py
- **log_message(message)** - Logs a message with timestamp
- **load_sales_data(filename="idealista_listings.json")** - Loads property sales data
- **extract_size(details)** - Extracts property size from details text
- **extract_room_type(details)** - Extracts room type from details text
- **load_rental_data(filename)** - Loads rental data from CSV
- **create_location_mapping()** - Creates mapping for standardizing locations
- **standardize_location(location, mapping)** - Standardizes location text
- **find_comparable_properties(property_data, rental_properties, size_tolerance_pct=0.3, size_adjustment_factor=0.2)** - Finds comparable rental properties
- **calculate_average_rent(comparables)** - Calculates average rent from comparable properties
- **estimate_rental_income(property_data, rental_properties)** - Estimates rental income for a property
- **generate_income_report(properties_for_sale, rental_properties)** - Generates rental income report
- **save_report_to_json(income_estimates, filename="rental_income_report.json")** - Saves report to JSON
- **save_report_to_csv(income_estimates, filename="rental_income_report.csv")** - Saves report to CSV
- **run_analysis()** - Runs the rental analysis process

### rental_scraper.py
- **log_message(message)** - Logs a message with timestamp
- **load_stored_listings()** - Loads previously stored rental listings
- **save_listings(listings)** - Saves rental listings to file
- **load_credits_usage()** - Loads ScrapingBee credits usage data
- **update_credits_usage(credits_used)** - Updates ScrapingBee credits usage records
- **fetch_page(url, page_num=1)** - Fetches a web page using ScrapingBee API
- **extract_rental_properties(html_content)** - Extracts rental property data from HTML
- **run_rental_scraper()** - Runs the rental scraper to collect rental listings
- **generate_monthly_csv(listings, month)** - Generates monthly CSV report of rental listings

### run_complete_analysis.py
- **run_complete_analysis()** - Runs complete property investment analysis

### scheduler.py
- **scheduled_job(mode="recent")** - Scheduled job for property data collection
- **main()** - Main function to run the scheduler

### scrape_additional_rentals.py
- **construct_page_url(base_url, page_num)** - Constructs URL for pagination
- **fetch_page(url, page_num)** - Fetches a web page using ScrapingBee API
- **scrape_additional_pages()** - Scrapes additional pages of rental listings

### update_rows_fixed.py
Contains script to update the investment summary HTML table (no function definitions, operates procedurally)

### validate_location_matching.py
- **log_message(message)** - Logs a message with timestamp
- **open_google_maps(address)** - Opens Google Maps with a property address
- **validate_location_match(report_file="rental_income_report.json", sample_size=5)** - Validates location matching
- **generate_validation_report(report_file="rental_income_report.json", output_file="location_validation_report.html")** - Generates report for location validation

### update_investment_with_rental.py
- **log_message(message)** - Logs a message with timestamp
- **load_rental_report(filename)** - Loads rental income report from CSV
- **load_investment_summary(filename)** - Loads investment summary from CSV
- **update_investment_with_rental(investment_df, rental_df)** - Updates investment summary with rental data
- **main()** - Main function to update investment summary with rental data

## Utility Scripts and Tools

### improved_rental_analysis_updated.py
- **extract_size(details)** - Extracts property size from details text with enhanced pattern matching
- **extract_room_type(details)** - Extracts room type from details text
- **extract_neighborhood(location_str)** - Extracts neighborhood from location using expanded list
- **load_property_data(json_file)** - Loads property data from JSON with special case handling
- **load_complete_rental_data(filename)** - Loads complete rental data from file
- **standardize_location(location_text)** - Standardizes location text
- **find_comparable_properties(property_data, rental_properties, size_tolerance_pct=0.2)** - Finds comparable rental properties with debug logging
- **calculate_average_rent(comparables)** - Calculates average rent from comparable properties
- **generate_income_report(properties_for_sale, rental_properties)** - Generates rental income report with direct rental price averaging
- **run_improved_analysis(similarity_threshold=75)** - Runs improved rental analysis

### calculate_avg_rent.py
- Script to calculate average monthly rent from rental data CSV files

### create_neighborhood_report.py
- **read_csv_data(file_path)** - Reads property data from CSV file
- **calculate_neighborhood_stats(df)** - Calculates statistics for each neighborhood
- **save_neighborhood_stats(stats, file_path)** - Saves neighborhood statistics to JSON
- **format_currency(value)** - Formats numbers as Euro currency
- **format_number(value, decimals)** - Formats numbers with specified decimals
- **generate_html_report(stats, output_file)** - Generates HTML report from neighborhood statistics
- **main()** - Main function to generate neighborhood report

### update_neighborhood_data.py
- **extract_neighborhood(location_str)** - Extracts neighborhood from location string using expanded list
- **load_json_data(json_file)** - Loads property data from original JSON file
- **extract_and_update_neighborhoods(data, csv_file)** - Updates neighborhoods in CSV from JSON data
- **calculate_neighborhood_stats(df)** - Calculates statistics for each neighborhood
- **format_currency(value)** - Formats numbers as Euro currency
- **format_number(value, decimals)** - Formats numbers with specified decimals
- **generate_html_report(stats, output_file)** - Generates updated HTML report with all neighborhoods
- **main()** - Main function to update neighborhoods and generate report

### run_neighborhood_update.py
- **extract_neighborhood(location_str)** - Extracts neighborhood from location string using expanded list
- **load_property_data(json_file)** - Loads property data from JSON file
- **update_neighborhoods(csv_file, property_data)** - Updates neighborhoods in CSV file
- **main()** - Main function to update neighborhood classifications

### update_investment_html.py
- **update_html_file(file_path)** - Updates neighborhoods count and link in investment summary HTML

### fix_rental_data.py
- **extract_price(price_str)** - Extracts numeric price from a price string with currency
- **extract_size(size_str)** - Extracts numeric size from a size string with units
- **fix_rental_data(input_file, output_file)** - Fixes errors in rental data CSV file
- **main()** - Main function to fix rental data

## Project Overview

PropBot is a property investment analysis tool designed to help investors identify promising real estate opportunities in Portugal. The project includes modules for:

1. Web scraping property listings from real estate websites using ScrapingBee API
2. Analyzing investment metrics including cap rate, cash flow, and ROI
3. Estimating rental income based on comparable properties
4. Calculating expenses and taxes for property investments
5. Matching properties with neighborhood data and generating neighborhood reports
6. Presenting investment opportunities in an interactive HTML interface with filtering and sorting
7. Scheduled data collection for maintaining up-to-date information
8. Analyzing property data by neighborhood to identify market trends

The HTML interface provides a user-friendly way to filter and sort properties based on various investment criteria, with visual indicators for above-average and below-average deals. Detailed neighborhood reports help identify the most promising areas for investment. 