import schedule
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from rental_scraper import run_rental_scraper

def scheduled_monthly_job():
    """Run the rental scraper as a scheduled monthly job."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Running scheduled monthly rental scraping job")
    
    # Always use premium proxies for the rental scraper to ensure reliability
    os.environ["USE_PREMIUM_PROXY"] = "true"
    
    try:
        properties_found = run_rental_scraper()
        print(f"[{timestamp}] Monthly rental job completed: {properties_found} rental properties collected")
    except Exception as e:
        print(f"[{timestamp}] Error in monthly rental job: {str(e)}")

def main():
    """Configure and run the scheduler for monthly rental data collection."""
    print("Starting Idealista rental scraper monthly scheduler")
    print("Schedule:")
    print("- Monthly on the 1st at 09:00: Collect rental property data")
    
    # Run monthly on the 1st of each month
    def check_and_run_if_first_day():
        if datetime.now().day == 1:
            scheduled_monthly_job()
    
    # Check every day at 9:00 AM if it's the 1st of the month
    schedule.every().day.at("09:00").do(check_and_run_if_first_day)
    
    # Option to run immediately for testing or initial setup
    run_now = input("Do you want to run the rental scraper immediately? (y/n): ").strip().lower()
    if run_now == 'y':
        print("Running initial rental scraping job...")
        scheduled_monthly_job()
    
    print("Scheduler started. Press Ctrl+C to stop.")
    
    # Keep the script running and check for scheduled jobs
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("Scheduler stopped.")

if __name__ == "__main__":
    # Load environment variables first
    load_dotenv()
    main() 