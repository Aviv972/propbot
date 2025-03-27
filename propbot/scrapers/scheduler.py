#!/usr/bin/env python3
"""
Scheduler for Property Scraping Tasks

This module schedules regular scraping tasks for property data collection.
It allows for automated daily scraping of sales data and monthly scraping of rental data.
"""

import os
import time
import logging
import schedule
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from typing import Callable, Dict, List

# Import scraper modules
from propbot.scrapers.idealista_scraper import scrape_sales_properties
from propbot.scrapers.rental_scraper import scrape_rental_properties
from propbot.scrapers.scrape_additional_rentals import scrape_additional_rental_sources

# Configure logging
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Constants
ENABLE_SALES_SCRAPING = os.getenv("ENABLE_SALES_SCRAPING", "true").lower() == "true"
ENABLE_RENTAL_SCRAPING = os.getenv("ENABLE_RENTAL_SCRAPING", "true").lower() == "true"
ENABLE_ADDITIONAL_RENTAL_SCRAPING = os.getenv("ENABLE_ADDITIONAL_RENTAL_SCRAPING", "true").lower() == "true"

# Schedule times
SALES_SCRAPE_TIME = os.getenv("SALES_SCRAPE_TIME", "01:00")  # Default to 1 AM
RENTAL_SCRAPE_DAY = int(os.getenv("RENTAL_SCRAPE_DAY", "1"))  # Default to 1st day of month
RENTAL_SCRAPE_TIME = os.getenv("RENTAL_SCRAPE_TIME", "02:00")  # Default to 2 AM
ADDITIONAL_RENTAL_SCRAPE_TIME = os.getenv("ADDITIONAL_RENTAL_SCRAPE_TIME", "03:00")  # Default to 3 AM

def log_task_start(task_name: str):
    """
    Log the start of a scheduled task.
    
    Args:
        task_name: Name of the task
    """
    logger.info(f"Starting scheduled task: {task_name} at {datetime.now().isoformat()}")


def log_task_completion(task_name: str, result: Dict = None):
    """
    Log the completion of a scheduled task.
    
    Args:
        task_name: Name of the task
        result: Optional result data to log
    """
    if result:
        logger.info(f"Completed task: {task_name} at {datetime.now().isoformat()}. Result: {result}")
    else:
        logger.info(f"Completed task: {task_name} at {datetime.now().isoformat()}")


def sales_scraping_job():
    """Job to scrape sales property data."""
    task_name = "Sales Property Scraping"
    log_task_start(task_name)
    
    try:
        properties = scrape_sales_properties()
        log_task_completion(task_name, {"properties_collected": len(properties)})
    except Exception as e:
        logger.error(f"Error in {task_name}: {e}")


def rental_scraping_job():
    """Job to scrape rental property data."""
    task_name = "Rental Property Scraping"
    log_task_start(task_name)
    
    try:
        properties = scrape_rental_properties()
        log_task_completion(task_name, {"properties_collected": len(properties)})
    except Exception as e:
        logger.error(f"Error in {task_name}: {e}")


def additional_rental_scraping_job():
    """Job to scrape additional rental property data."""
    task_name = "Additional Rental Property Scraping"
    log_task_start(task_name)
    
    try:
        rental_data = scrape_additional_rental_sources()
        total_collected = sum(len(rentals) for rentals in rental_data.values())
        log_task_completion(task_name, {
            "sources": len(rental_data),
            "total_properties_collected": total_collected
        })
    except Exception as e:
        logger.error(f"Error in {task_name}: {e}")


def setup_schedules():
    """Set up the scheduled tasks."""
    # Schedule sales scraping job (daily)
    if ENABLE_SALES_SCRAPING:
        schedule.every().day.at(SALES_SCRAPE_TIME).do(sales_scraping_job)
        logger.info(f"Scheduled sales scraping job for every day at {SALES_SCRAPE_TIME}")
    
    # Schedule rental scraping job (monthly)
    if ENABLE_RENTAL_SCRAPING:
        schedule.every().month.at(RENTAL_SCRAPE_DAY).at(RENTAL_SCRAPE_TIME).do(rental_scraping_job)
        logger.info(f"Scheduled rental scraping job for day {RENTAL_SCRAPE_DAY} of every month at {RENTAL_SCRAPE_TIME}")
    
    # Schedule additional rental scraping job (twice monthly)
    if ENABLE_ADDITIONAL_RENTAL_SCRAPING:
        # Schedule for 1st and 15th of each month
        schedule.every().month.at(1).at(ADDITIONAL_RENTAL_SCRAPE_TIME).do(additional_rental_scraping_job)
        schedule.every().month.at(15).at(ADDITIONAL_RENTAL_SCRAPE_TIME).do(additional_rental_scraping_job)
        logger.info(f"Scheduled additional rental scraping job for days 1 and 15 of every month at {ADDITIONAL_RENTAL_SCRAPE_TIME}")


def run_scheduler(run_now: bool = False):
    """
    Run the scheduler.
    
    Args:
        run_now: If True, run all jobs immediately before starting the scheduler
    """
    setup_schedules()
    
    if run_now:
        logger.info("Running all enabled jobs immediately")
        if ENABLE_SALES_SCRAPING:
            sales_scraping_job()
        if ENABLE_RENTAL_SCRAPING:
            rental_scraping_job()
        if ENABLE_ADDITIONAL_RENTAL_SCRAPING:
            additional_rental_scraping_job()
    
    logger.info("Starting scheduler. Press Ctrl+C to exit.")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check for pending jobs every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
    except Exception as e:
        logger.error(f"Scheduler stopped due to error: {e}")


if __name__ == "__main__":
    # Setup logging when script is run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("propbot/scrapers/scheduler.log"),
            logging.StreamHandler()
        ]
    )
    
    # Run the scheduler
    run_scheduler(run_now=False) 