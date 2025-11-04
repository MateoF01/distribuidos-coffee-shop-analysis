#!/bin/bash

# Script to validate client results against Kaggle expected results
# Usage: ./validate_results.sh <client_results_dir|client_dir> [full]
# Example: ./validate_results.sh client/results/client_da5a9d237a56/request_1
# Example: ./validate_results.sh client/results/client_da5a9d237a56  # validates all requests
# Example: ./validate_results.sh client/results/client_da5a9d237a56/request_1 full

if [ $# -eq 0 ]; then
    echo "Usage: $0 <client_results_dir|client_dir> [full]"
    echo ""
    echo "Arguments:"
    echo "  <client_results_dir>  Path to client results directory"
    echo "                        - Specific request: client/results/client_xxxxx/request_1"
    echo "                        - All requests:     client/results/client_xxxxx"
    echo "  [full]                Optional: Use 'full' to compare against full_kaggle_* files"
    echo ""
    echo "Examples:"
    echo "  $0 client/results/client_da5a9d237a56/request_1        # Validate single request"
    echo "  $0 client/results/client_da5a9d237a56                  # Validate all requests"
    echo "  $0 client/results/client_da5a9d237a56/request_1 full   # Validate with full dataset"
    exit 1
fi

CLIENT_INPUT=$1
USE_FULL=${2:-""}

# Check if input is a specific request directory or a client directory
if [[ "$CLIENT_INPUT" =~ request_[0-9]+$ ]]; then
    # Specific request directory
    REQUEST_DIRS=("$CLIENT_INPUT")
    VALIDATE_MODE="single"
elif [ -d "$CLIENT_INPUT" ]; then
    # Client directory - find all request subdirectories
    REQUEST_DIRS=($(find "$CLIENT_INPUT" -maxdepth 1 -type d -name "request_*" | sort))
    if [ ${#REQUEST_DIRS[@]} -eq 0 ]; then
        echo "❌ Error: No request directories found in $CLIENT_INPUT"
        exit 1
    fi
    VALIDATE_MODE="multiple"
else
    echo "❌ Error: Invalid path: $CLIENT_INPUT"
    exit 1
fi

# Determine which Kaggle results to use
if [ "$USE_FULL" == "full" ]; then
    KAGGLE_PREFIX="full_kaggle"
    echo "📊 Using FULL Kaggle results for comparison"
else
    KAGGLE_PREFIX="kaggle"
    echo "📊 Using standard Kaggle results for comparison"
fi

KAGGLE_RESULTS_DIR="results"

# Check if Kaggle results directory exists
if [ ! -d "$KAGGLE_RESULTS_DIR" ]; then
    echo "❌ Error: Kaggle results directory not found: $KAGGLE_RESULTS_DIR"
    exit 1
fi

# Display header based on validation mode
if [ "$VALIDATE_MODE" == "multiple" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔍 Validating All Requests for Client"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📁 Client directory: $CLIENT_INPUT"
    echo "📊 Found ${#REQUEST_DIRS[@]} request(s) to validate"
    echo "📁 Kaggle results: $KAGGLE_RESULTS_DIR (${KAGGLE_PREFIX}_*)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
fi

# Global counters for multiple requests
GLOBAL_TOTAL_TESTS=0
GLOBAL_PASSED_TESTS=0
GLOBAL_FAILED_TESTS=0
GLOBAL_REQUEST_RESULTS=()

# Function to validate a single request directory
validate_request_directory() {
    local CLIENT_RESULTS_DIR=$1
    local REQUEST_NAME=$(basename "$CLIENT_RESULTS_DIR")
    
    if [ "$VALIDATE_MODE" == "multiple" ]; then
        echo "┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓"
        echo "┃ 📋 Validating: $REQUEST_NAME"
        echo "┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛"
        echo ""
    fi
    
    # Check if directory exists
    if [ ! -d "$CLIENT_RESULTS_DIR" ]; then
        echo "❌ Error: Request directory not found: $CLIENT_RESULTS_DIR"
        if [ "$VALIDATE_MODE" == "multiple" ]; then
            GLOBAL_REQUEST_RESULTS+=("$REQUEST_NAME: ❌ FAILED (directory not found)")
            GLOBAL_FAILED_TESTS=$((GLOBAL_FAILED_TESTS + 5))
            GLOBAL_TOTAL_TESTS=$((GLOBAL_TOTAL_TESTS + 5))
        fi
        return 1
    fi
    
    if [ "$VALIDATE_MODE" == "single" ]; then
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "🔍 Validating Client Results"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "📁 Client results: $CLIENT_RESULTS_DIR"
        echo "📁 Kaggle results: $KAGGLE_RESULTS_DIR (${KAGGLE_PREFIX}_*)"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
    fi
    
    # Reset counters for this request
    TOTAL_TESTS=0
    PASSED_TESTS=0
    FAILED_TESTS=0

# Function to compare simple queries (Q1, Q2_A, Q2_B, Q3)
compare_simple_query() {
    local query_name=$1
    # Client files use uppercase (Q1, Q2_A), so construct the actual filename
    local client_file="${CLIENT_RESULTS_DIR}/${query_name}_results.csv"
    # Kaggle uses lowercase (q1, q2_a)
    local kaggle_query_name=$(echo "$query_name" | tr '[:upper:]' '[:lower:]')
    local kaggle_file="${KAGGLE_RESULTS_DIR}/${KAGGLE_PREFIX}_${kaggle_query_name}.csv"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔎 Testing ${query_name}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ ! -f "$client_file" ]; then
        echo "❌ FAILED: Client file not found: $client_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo ""
        return 1
    fi
    
    if [ ! -f "$kaggle_file" ]; then
        echo "❌ FAILED: Kaggle file not found: $kaggle_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo ""
        return 1
    fi
    
    # Sort both files and compare
    DIFF_OUTPUT=$(diff <(sort "$client_file") <(sort "$kaggle_file") 2>&1)
    
    if [ $? -eq 0 ]; then
        echo "✅ PASSED: Results match perfectly"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        
        # Show line count
        CLIENT_LINES=$(wc -l < "$client_file")
        echo "   📄 Total records: $((CLIENT_LINES - 1))"
    else
        echo "❌ FAILED: Results do not match"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        
        CLIENT_LINES=$(wc -l < "$client_file")
        KAGGLE_LINES=$(wc -l < "$kaggle_file")
        
        echo "   📄 Client records: $((CLIENT_LINES - 1))"
        echo "   📄 Kaggle records: $((KAGGLE_LINES - 1))"
        echo ""
        echo "   Differences:"
        echo "$DIFF_OUTPUT" | head -n 20
        if [ $(echo "$DIFF_OUTPUT" | wc -l) -gt 20 ]; then
            echo "   ... (showing first 20 lines of diff)"
        fi
    fi
    
    echo ""
}

# Function to validate Q4 (complex top-3 validation)
validate_q4() {
    local client_file="${CLIENT_RESULTS_DIR}/Q4_results.csv"
    # Kaggle files use lowercase
    local kaggle_file="${KAGGLE_RESULTS_DIR}/${KAGGLE_PREFIX}_q4.csv"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🔎 Testing Q4 (Top-3 validation with purchase quantity logic)"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ ! -f "$client_file" ]; then
        echo "❌ FAILED: Client file not found: $client_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo ""
        return 1
    fi
    
    if [ ! -f "$kaggle_file" ]; then
        echo "❌ FAILED: Kaggle file not found: $kaggle_file"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        echo ""
        return 1
    fi
    
    # Use Python for complex Q4 validation
    python3 - <<'PYTHON_SCRIPT' "$client_file" "$kaggle_file"
import sys
import csv
from collections import defaultdict

client_file = sys.argv[1]
kaggle_file = sys.argv[2]

# Read client results (store_name, birthdate)
client_results = []
with open(client_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        client_results.append((row['store_name'], row['birthdate']))

# Read kaggle results (store_name, birthdate, purchases_qty)
kaggle_data = defaultdict(list)
with open(kaggle_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        store = row['store_name']
        birthdate = row['birthdate']
        qty = int(row['purchases_qty'])
        kaggle_data[store].append((birthdate, qty))

# Group client results by store
client_by_store = defaultdict(list)
for store, birthdate in client_results:
    client_by_store[store].append(birthdate)

all_valid = True
stores_checked = 0
errors = []
store_validations = defaultdict(str)

for store in client_by_store:
    stores_checked += 1
    client_birthdates = client_by_store[store]
    
    if store not in kaggle_data:
        errors.append(f"   ❌ Store '{store}' not found in Kaggle results")
        all_valid = False
        continue
    
    # Get all entries for this store from kaggle, sorted by quantity descending
    store_entries = sorted(kaggle_data[store], key=lambda x: x[1], reverse=True)
    
    # Client should have exactly 3 results for this store
    if len(client_birthdates) != 3:
        errors.append(f"   ❌ Store '{store}': Client has {len(client_birthdates)} results (expected exactly 3)")
        all_valid = False
        continue
    
    # Get the top 3 entries by quantity from kaggle
    # This means: sort by qty desc, take top 3
    top_3_kaggle = store_entries[:3]
    top_3_birthdates = {bd for bd, qty in top_3_kaggle}
    top_3_quantities = [qty for bd, qty in top_3_kaggle]
    
    # Get all entries that have the same quantity as the 3rd highest
    # This handles ties: if multiple entries share the 3rd place quantity, any of them are valid
    third_place_qty = top_3_quantities[2] if len(top_3_quantities) >= 3 else 0
    
    # Get all valid birthdates: anything with quantity >= third_place_qty is valid for top 3
    valid_birthdates = {bd for bd, qty in store_entries if qty >= third_place_qty}
    
    # Check if all client birthdates are valid (in the top-3 candidate pool)
    invalid_entries = []
    for birthdate in client_birthdates:
        if birthdate not in valid_birthdates:
            # Find this birthdate's quantity in kaggle
            client_qty = next((qty for bd, qty in store_entries if bd == birthdate), None)
            if client_qty is None:
                invalid_entries.append(f"{birthdate} (not found in Kaggle)")
            else:
                invalid_entries.append(f"{birthdate} (qty={client_qty}, minimum required={third_place_qty})")
    
    if invalid_entries:
        errors.append(f"   ❌ Store '{store}': Invalid birthdates not in top-3:")
        for entry in invalid_entries:
            errors.append(f"      - {entry}")
        errors.append(f"      Top 3 quantities: {top_3_quantities}")
        all_valid = False
    else:
        # Success - show what we validated
        unique_qtys = sorted(set(top_3_quantities), reverse=True)
        num_candidates = len(valid_birthdates)
        if num_candidates > 3:
            store_validations[store] = f"✅ Valid (3 of {num_candidates} candidates with qty >= {third_place_qty})"
        else:
            store_validations[store] = f"✅ Valid (all {num_candidates} top entries, quantities: {unique_qtys})"

# Print results
if all_valid:
    print(f"✅ PASSED: All Q4 results are valid")
    print(f"   📊 Stores validated: {stores_checked}")
    print(f"   📄 Total client records: {len(client_results)}")
    
    # Show some sample validations
    sample_count = min(5, len(store_validations))
    if sample_count > 0:
        print(f"   📋 Sample validations (showing {sample_count}):")
        for i, (store, msg) in enumerate(list(store_validations.items())[:sample_count]):
            store_display = store[:60] + "..." if len(store) > 60 else store
            print(f"      {store_display}")
            print(f"      {msg}")
    
    sys.exit(0)
else:
    print(f"❌ FAILED: Q4 validation failed")
    print(f"   📊 Stores checked: {stores_checked}")
    print(f"   📄 Total client records: {len(client_results)}")
    print("")
    print("   Errors found:")
    for error in errors:
        print(error)
    sys.exit(1)
PYTHON_SCRIPT
    
    if [ $? -eq 0 ]; then
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    
    echo ""
}

# Run all validations
compare_simple_query "Q1"
compare_simple_query "Q2_A"
compare_simple_query "Q2_B"
compare_simple_query "Q3"
validate_q4

# Request summary
if [ "$VALIDATE_MODE" == "single" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 FINAL SUMMARY"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Total tests:  $TOTAL_TESTS"
    echo "✅ Passed:    $PASSED_TESTS"
    echo "❌ Failed:    $FAILED_TESTS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ $FAILED_TESTS -eq 0 ]; then
        echo "🎉 All validations passed!"
    else
        echo "⚠️  Some validations failed. Please review the results above."
    fi
else
    # Multiple request mode - show request summary
    echo "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  📊 $REQUEST_NAME Summary"
    echo "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  Total tests:  $TOTAL_TESTS"
    echo "  ✅ Passed:    $PASSED_TESTS"
    echo "  ❌ Failed:    $FAILED_TESTS"
    echo "  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    # Update global counters
    GLOBAL_TOTAL_TESTS=$((GLOBAL_TOTAL_TESTS + TOTAL_TESTS))
    GLOBAL_PASSED_TESTS=$((GLOBAL_PASSED_TESTS + PASSED_TESTS))
    GLOBAL_FAILED_TESTS=$((GLOBAL_FAILED_TESTS + FAILED_TESTS))
    
    # Store result
    if [ $FAILED_TESTS -eq 0 ]; then
        GLOBAL_REQUEST_RESULTS+=("$REQUEST_NAME: ✅ PASSED ($PASSED_TESTS/$TOTAL_TESTS)")
    else
        GLOBAL_REQUEST_RESULTS+=("$REQUEST_NAME: ❌ FAILED ($PASSED_TESTS/$TOTAL_TESTS passed)")
    fi
fi
}

# Execute validation for all request directories
for request_dir in "${REQUEST_DIRS[@]}"; do
    validate_request_directory "$request_dir"
done

# Final summary for multiple requests
if [ "$VALIDATE_MODE" == "multiple" ]; then
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "📊 GLOBAL SUMMARY - ALL REQUESTS"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Total requests validated: ${#REQUEST_DIRS[@]}"
    echo "Total tests:              $GLOBAL_TOTAL_TESTS"
    echo "✅ Passed:                $GLOBAL_PASSED_TESTS"
    echo "❌ Failed:                $GLOBAL_FAILED_TESTS"
    echo ""
    echo "Results by request:"
    for result in "${GLOBAL_REQUEST_RESULTS[@]}"; do
        echo "  $result"
    done
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [ $GLOBAL_FAILED_TESTS -eq 0 ]; then
        echo "🎉 All requests passed validation!"
        exit 0
    else
        echo "⚠️  Some requests failed validation. Please review the results above."
        exit 1
    fi
fi

# Exit based on test results
if [ $FAILED_TESTS -eq 0 ]; then
    exit 0
else
    exit 1
fi
