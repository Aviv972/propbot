#!/bin/bash
# PropBot End-to-End Test Script

# Set up directories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"
BASE_DIR="$PROJECT_ROOT"
DATA_DIR="$BASE_DIR/propbot/data"
LOG_DIR="$BASE_DIR/propbot/logs"
TEST_DIR="$BASE_DIR/propbot/tests"
mkdir -p "$LOG_DIR"
mkdir -p "$TEST_DIR"

echo "======= PropBot Pipeline Test ======="
echo "Started at: $(date)"
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"
echo "Working directory: $(pwd)"
echo "Data directory: $DATA_DIR"

# Create log file
LOG_FILE="$LOG_DIR/pipeline_test_$(date +%Y%m%d_%H%M%S).log"
echo "Logging to: $LOG_FILE"

# Function to run a command and log its output
run_cmd() {
    echo -e "\n==== Running: $1 ====" | tee -a "$LOG_FILE"
    eval "$1" 2>&1 | tee -a "$LOG_FILE"
    EXIT_CODE=${PIPESTATUS[0]}
    if [ $EXIT_CODE -eq 0 ]; then
        echo "✅ Command completed successfully" | tee -a "$LOG_FILE"
    else
        echo "❌ Command failed with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
        echo "Continuing with next step..."
    fi
    echo "" | tee -a "$LOG_FILE"
    return $EXIT_CODE
}

# Check the environment
run_cmd "ls -la propbot"
run_cmd "ls -la propbot/data_processing"
run_cmd "which python3"

# Create sample test data for sales and rentals
echo "===== Creating Sample Test Data =====" | tee -a "$LOG_FILE"
mkdir -p "$DATA_DIR/raw/sales"
mkdir -p "$DATA_DIR/raw/rentals"
mkdir -p "$DATA_DIR/processed"

# Create a sample sales listing file
cat <<EOF > "$DATA_DIR/raw/sales/test_sales.json"
[
  {
    "url": "https://example.com/property/123",
    "title": "Test Property 1",
    "price": "250000",
    "size": "85 m²",
    "location": "Lisbon",
    "details": "2 bedrooms, 1 bathroom"
  },
  {
    "url": "https://example.com/property/456",
    "title": "Test Property 2",
    "price": "350000",
    "size": "120 m²",
    "location": "Porto",
    "details": "3 bedrooms, 2 bathrooms"
  }
]
EOF

# Create a sample rentals listing file
cat <<EOF > "$DATA_DIR/raw/rentals/test_rentals.json"
[
  {
    "url": "https://example.com/rental/123",
    "title": "Test Rental 1",
    "price": "900",
    "size": "60 m²",
    "location": "Lisbon",
    "details": "T1",
    "is_rental": true
  },
  {
    "url": "https://example.com/rental/456",
    "title": "Test Rental 2",
    "price": "1200",
    "size": "85 m²",
    "location": "Porto",
    "details": "T2",
    "is_rental": true
  }
]
EOF

# Run validation module test
echo "===== Testing Validation Module =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.validation.precheck $DATA_DIR/raw/sales/test_sales.json $DATA_DIR/raw/rentals/test_rentals.json"

# Run schema validation test
echo "===== Testing Schema Validation =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.validation.schemas $DATA_DIR/raw/sales/test_sales.json --sales"

# Run consolidation module tests
echo "===== Testing Sales Consolidation =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.consolidation.sales $DATA_DIR/raw/sales/test_sales.json $DATA_DIR/processed/test_sales_consolidated.json $DATA_DIR/raw/sales"

echo "===== Testing Rentals Consolidation =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.consolidation.rentals $DATA_DIR/raw/rentals/test_rentals.json $DATA_DIR/processed/test_rentals_consolidated.json $DATA_DIR/raw/rentals"

# Run conversion module tests
echo "===== Testing Sales Conversion =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.conversion.sales $DATA_DIR/processed/test_sales_consolidated.json $DATA_DIR/processed/test_sales.csv"

echo "===== Testing Rentals Conversion =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.data_processing.conversion.rentals $DATA_DIR/processed/test_rentals_consolidated.json $DATA_DIR/processed/test_rentals.csv"

# Run full pipeline for sales
echo "===== Testing Sales Pipeline =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.main --type sales --data-dir $DATA_DIR"

# Run full pipeline for rentals
echo "===== Testing Rentals Pipeline =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.main --type rentals --data-dir $DATA_DIR"

# Run full pipeline for both
echo "===== Testing Full Pipeline =====" | tee -a "$LOG_FILE"
run_cmd "python3 -m propbot.main --type both --data-dir $DATA_DIR"

# Check the results
echo "===== Checking Results =====" | tee -a "$LOG_FILE"
run_cmd "ls -l $DATA_DIR/processed"

# Clean up test data if test was successful
if run_cmd "test -f $DATA_DIR/processed/sales.csv -a -f $DATA_DIR/processed/rentals.csv"; then
    echo "===== Test Successful - Cleaning Up =====" | tee -a "$LOG_FILE"
    run_cmd "mv $DATA_DIR/raw/sales/test_sales.json $TEST_DIR/ 2>/dev/null || true"
    run_cmd "mv $DATA_DIR/raw/rentals/test_rentals.json $TEST_DIR/ 2>/dev/null || true"
    echo "Sample test data moved to test directory" | tee -a "$LOG_FILE"
fi

echo "===== Pipeline Test Completed =====" | tee -a "$LOG_FILE"
echo "Finished at: $(date)" | tee -a "$LOG_FILE" 