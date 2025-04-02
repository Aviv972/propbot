#!/bin/bash
# Script to verify Heroku environment variables

echo "Checking Heroku environment variables..."

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

# Verify the DATABASE_URL is set
echo "Checking if DATABASE_URL is set..."
db_url=$(heroku config:get DATABASE_URL -a propbot-investment-analyzer 2>&1)

if [[ "$db_url" == *"Error"* ]]; then
    echo "Error accessing Heroku config. Please ensure you have access to propbot-investment-analyzer."
    exit 1
elif [ -z "$db_url" ]; then
    echo "⚠️ DATABASE_URL is not set! Please set it with:"
    echo "heroku config:set DATABASE_URL='postgres://...' -a propbot-investment-analyzer"
    
    # Try to get the value from local .env file
    if [ -f .env ]; then
        local_db_url=$(grep DATABASE_URL .env | cut -d= -f2-)
        if [ ! -z "$local_db_url" ]; then
            echo "Found DATABASE_URL in local .env file. Use this command to set it on Heroku:"
            echo "heroku config:set DATABASE_URL='$local_db_url' -a propbot-investment-analyzer"
        fi
    fi
else
    echo "✅ DATABASE_URL is set"
fi

# Check other important environment variables
echo "Checking other environment variables..."

port=$(heroku config:get PORT -a propbot-investment-analyzer 2>&1)
if [ -z "$port" ]; then
    echo "⚠️ PORT is not set, but this should be set automatically by Heroku"
else
    echo "✅ PORT is set to $port"
fi

scan_mode=$(heroku config:get SCAN_MODE -a propbot-investment-analyzer 2>&1)
if [ -z "$scan_mode" ]; then
    echo "⚠️ SCAN_MODE is not set. Setting to 'recent'"
    heroku config:set SCAN_MODE=recent -a propbot-investment-analyzer
else
    echo "✅ SCAN_MODE is set to $scan_mode"
fi

# List all config vars
echo "All configuration variables:"
heroku config -a propbot-investment-analyzer 