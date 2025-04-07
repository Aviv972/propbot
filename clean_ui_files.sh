#!/bin/bash
set -e

echo "This script will PERMANENTLY DELETE all non-essential UI files."
echo "Essential files that will be KEPT:"
echo "  - propbot/ui/__init__.py"
echo "  - propbot/ui/components/__init__.py"
echo "  - propbot/ui/dashboards/__init__.py"
echo "  - propbot/ui/generators/__init__.py"
echo "  - propbot/ui/investment_dashboard_latest.html"
echo "  - propbot/ui/standalone_dashboard.html"
echo "  - propbot/ui/neighborhood_report_updated.html"
echo "  - propbot/ui/dashboard_redirect.html"
echo "  - propbot/ui/neighborhood_redirect.html"
echo ""
echo "All other files in the propbot/ui/ directory will be PERMANENTLY DELETED."
echo ""
read -p "Are you sure you want to continue? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Operation cancelled."
    exit 1
fi

echo "Creating backup of UI directory just in case..."
BACKUP_DIR="propbot_ui_backup_$(date +%Y%m%d%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -r propbot/ui/* "$BACKUP_DIR/"
echo "Backup created at: $BACKUP_DIR"

echo "Removing non-essential UI files..."

# Remove all .DS_Store files
find propbot/ui -name ".DS_Store" -delete

# Remove all backup files
find propbot/ui -name "*.bak" -delete
find propbot/ui -name "*.backup" -delete
find propbot/ui -name "*.bak_*" -delete

# Remove specific non-essential templates
rm -f propbot/ui/expense_report.html
rm -f propbot/ui/investment_dashboard_updated.html
rm -f propbot/ui/location_matching_report.html
rm -f propbot/ui/neighborhood_report.html
rm -f propbot/ui/new_investment_dashboard.html
rm -f propbot/ui/run_propbot.html

echo "Cleanup complete! Non-essential UI files have been removed."
echo "A backup was created at: $BACKUP_DIR" 