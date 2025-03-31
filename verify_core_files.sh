#!/bin/bash
# Verify that core files are properly included in Git

# Define colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Checking if essential Python scripts will be included in Git...${NC}"

# Main scripts
for file in propbot/main.py propbot/run_dashboard_server.py propbot/serve_neighborhood.py propbot/serve_static.py; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

# Data processing scripts
for file in propbot/data_processing/pipeline/standard.py propbot/data_processing/pipeline/workflow.py propbot/data_processing/data_processor.py propbot/data_processing/rebuild_data_pipeline.py; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

# Analysis scripts
for file in propbot/run_investment_analysis.py propbot/analysis/metrics/investment_metrics.py propbot/analysis/metrics/rental_metrics.py propbot/analysis/location_analyzer.py; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

# Dashboard and reporting
for file in propbot/generate_dashboard.py propbot/reporting/neighborhood_report.py propbot/update_dashboard.py; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

echo -e "\n${YELLOW}Checking if critical data files will be included in Git...${NC}"

# Critical data files
for file in propbot/data/processed/sales_current.csv propbot/data/processed/rentals_current.csv propbot/data/processed/sales_listings_consolidated.json propbot/data/processed/rental_listings_consolidated.json propbot/data/metadata/neighborhood_stats.json; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

# UI files
echo -e "\n${YELLOW}Checking if UI files will be included in Git...${NC}"
for file in propbot/ui/investment_dashboard_latest.html propbot/ui/neighborhood_report_latest.html; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

echo -e "\n${YELLOW}Checking if Heroku deployment files will be included in Git...${NC}"
for file in Procfile runtime.txt app.json requirements.txt; do
  if git check-ignore -q "$file"; then
    echo -e "${RED}❌ $file is ignored by Git${NC}"
  else
    echo -e "${GREEN}✓ $file will be included in Git${NC}"
  fi
done

echo -e "\n${YELLOW}Done checking files.${NC}" 