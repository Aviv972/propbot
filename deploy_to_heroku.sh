#!/bin/bash
# Script to deploy fixes to Heroku

echo "Deploying fixes to Heroku..."

# Ensure Heroku CLI is installed
if ! command -v heroku &> /dev/null; then
    echo "Heroku CLI is not installed. Please install it first."
    echo "Visit: https://devcenter.heroku.com/articles/heroku-cli"
    exit 1
fi

# Check if logged in to Heroku
heroku_status=$(heroku auth:whoami 2>&1)
if [[ "$heroku_status" == *"Error"* ]]; then
    echo "Not logged in to Heroku. Please login first:"
    heroku login
fi

# Verify the app exists and we have access
if ! heroku apps:info -a propbot-investment-analyzer &> /dev/null; then
    echo "Cannot access the Heroku app. Please ensure you have access to propbot-investment-analyzer."
    exit 1
fi

# Make sure we're in a git repository
if [ ! -d .git ]; then
    echo "Initializing git repository..."
    git init
fi

# Check if Heroku remote exists, add if not
if ! git remote | grep -q heroku; then
    echo "Adding Heroku remote..."
    heroku git:remote -a propbot-investment-analyzer
fi

# Add the changed files to git
echo "Adding modified files to git..."
git add propbot/env_loader.py
git add propbot/database_utils.py
git add propbot/analysis/metrics/rental_metrics.py
git add propbot/analysis/metrics/rental_analysis.py
git add propbot/run_investment_analysis.py

# Commit the changes
echo "Committing changes..."
git commit -m "Fix environment loading and None value handling"

# Push to Heroku
echo "Pushing to Heroku..."
git push heroku main

# Trigger the data processing pipeline on Heroku
echo "Triggering data processing pipeline..."
curl -X POST https://propbot-investment-analyzer-b56a7b23f6c1.herokuapp.com/run-analysis

echo "Deployment completed! Check the Heroku logs for progress:"
echo "heroku logs --tail -a propbot-investment-analyzer" 