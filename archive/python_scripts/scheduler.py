import schedule
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from idealista_scraper import run_scraper

def scheduled_job(mode="recent"):
    """Run the scraper as a scheduled job with the specified mode."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] Running scheduled scraping job in '{mode}' mode")
    
    # Set the scan mode in the environment variable
    os.environ["SCAN_MODE"] = mode
    
    # Set premium proxies based on mode
    if mode == "full":
        os.environ["USE_PREMIUM_PROXY"] = "true"  # Always use premium for full scans
    else:
        # For recent scans, use premium only if specifically set in .env
        os.environ["USE_PREMIUM_PROXY"] = os.getenv("USE_PREMIUM_PROXY", "false")
    
    try:
        new_found, updated_found = run_scraper()
        print(f"[{timestamp}] Scheduled job completed ({mode} mode): {new_found} new, {updated_found} updated listings")
    except Exception as e:
        print(f"[{timestamp}] Error in scheduled job: {str(e)}")

def main():
    """Configure and run the scheduler with optimized 3-tier strategy."""
    print("Starting Idealista scraper scheduler with optimized 3-tier strategy")
    print("Schedule:")
    print("- Daily at 08:00 and 20:00: Run in 'recent' mode (last 48 hours)")
    print("- Weekly on Monday at 10:00: Run in 'week' mode (last week)")
    print("- Monthly on the 1st at 12:00: Run in 'full' mode (all listings)")
    
    # Daily jobs (recent mode - 48h) - morning and evening to catch new listings
    schedule.every().day.at("08:00").do(scheduled_job, mode="recent")
    schedule.every().day.at("20:00").do(scheduled_job, mode="recent")
    
    # Weekly job (week mode - last week) - Monday morning
    schedule.every().monday.at("10:00").do(scheduled_job, mode="week")
    
    # Monthly job (full mode - all listings) - 1st of each month
    def monthly_job():
        # Check if today is the 1st of the month
        if datetime.now().day == 1:
            scheduled_job(mode="full")
    
    # Run the monthly check every day at noon
    schedule.every().day.at("12:00").do(monthly_job)
    
    # Run the scraper once immediately (in recent mode for daily updates)
    print(f"Running initial scraping job in 'recent' mode...")
    scheduled_job(mode="recent")
    
    # Keep the script running and check for scheduled jobs
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    # Load environment variables first
    load_dotenv()
    main() 