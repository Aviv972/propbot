#!/bin/bash
set -e

echo "Starting git repository cleanup..."
echo "This script will remove unnecessary files from git tracking without deleting them from your local system."

# Create a backup branch just in case
git branch -m master main 2>/dev/null || true
echo "Creating a backup branch..."
git checkout -b cleanup_backup_$(date +%Y%m%d%H%M%S)

# Switch back to main branch
git checkout main

# Remove files from git tracking without deleting them locally
echo "Removing unnecessary files from git tracking..."

# Development/test HTML files
git rm --cached dashboard.html dashboard_new.html neighborhood.html 2>/dev/null || true

# Local data files
git rm --cached rental_data_*.csv rental_listings.json rental_income_report.csv rental_income_report.json \
  rental_credits_usage.json idealista_listings.json credits_usage.json local_rentals_data.csv 2>/dev/null || true

# Scripts used for development only
git rm --cached download_data.sh verify_core_files.sh 2>/dev/null || true
git rm --cached propbot/test_pipeline.sh 2>/dev/null || true

# Documentation files not needed for production
git rm --cached .cursorrules.md integrated_files.txt INTEGRATION_PLAN.md 2>/dev/null || true

# Remove archive directory from git tracking
git rm -r --cached archive/ 2>/dev/null || true

# Remove log files
git rm --cached propbot/*.log 2>/dev/null || true
git rm -r --cached propbot/logs/*.json 2>/dev/null || true
git rm -r --cached propbot/data/logs/ 2>/dev/null || true

# Remove DS_Store files
find . -name ".DS_Store" -exec git rm --cached {} \; 2>/dev/null || true

# Remove backup files
git rm --cached **/*.bak **/*.bak_* **/*.backup 2>/dev/null || true

echo "Files have been removed from git tracking but remain in your local directory."
echo "To complete the process, commit these changes:"
echo "git add .gitignore"
echo "git commit -m \"Remove unnecessary files from git tracking\""
echo "git push"

echo "Cleanup complete!" 