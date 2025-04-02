# PropBot Investment Analyzer

A real estate investment analysis tool that scrapes property listings from Idealista, processes them, and provides detailed investment metrics and visualizations.

## Features

- Automated scraping of property listings (sales and rentals)
- Database storage for property data
- Investment analysis metrics (rental yield, cap rate, cash flow)
- Interactive dashboard for exploring investment opportunities
- Neighborhood-based analysis

## Installation and Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database
- Heroku CLI (for deployment)

### Local Development Setup

1. Clone the repository:
```
git clone https://github.com/yourusername/propbot-investment-analyzer.git
cd propbot-investment-analyzer
```

2. Create a virtual environment and install dependencies:
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Copy the example .env file and fill in your values:
```
cp .env.example .env
```

4. Update the `.env` file with your configuration:
```
DATABASE_URL=postgres://username:password@host:port/database
SCRAPINGBEE_API_KEY=your_api_key
```

5. Run the dashboard server:
```
python -m propbot.run_dashboard_server
```

### Running the Full Data Pipeline

To run the complete end-to-end data pipeline:

```
python -m propbot.run_dashboard_server --run-analysis
```

This will:
1. Scrape new property listings from Idealista
2. Update the database with new data
3. Process and consolidate the data
4. Run investment analysis
5. Generate an updated dashboard

### Deploying to Heroku

1. Make sure you have the Heroku CLI installed and you're logged in:
```
heroku login
```

2. Run the deployment script:
```
./deploy_to_heroku.sh
```

3. Check the environment variables on Heroku:
```
./check_heroku_env.sh
```

4. Access your application at:
```
https://propbot-investment-analyzer-b56a7b23f6c1.herokuapp.com/
```

## Troubleshooting

### Environment Variables

If you encounter issues with database connections or missing data, check that your environment variables are properly set:

1. For local development, verify `.env` file has `DATABASE_URL` properly configured
2. For Heroku deployment, run:
```
heroku config -a propbot-investment-analyzer
```

### Data Pipeline Errors

To debug data pipeline issues:

1. Check the logs:
   - Local: Terminal output
   - Heroku: `heroku logs --tail -a propbot-investment-analyzer`

2. Run individual components separately to isolate the issue:
   - Scraping: `python -m propbot.scrapers.idealista_scraper`
   - Database update: `python -m propbot.data_processing.update_db`
   - Investment analysis: `python -m propbot.run_investment_analysis`

## Recent Fixes

- Fixed environment variable loading with centralized `env_loader.py`
- Added error handling for None values in rental metrics calculation
- Improved database connection reliability

## License

This project is licensed under the MIT License - see the LICENSE file for details.