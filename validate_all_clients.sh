#!/bin/bash

# Script to validate all client results
# Usage: ./validate_all_clients.sh [full]

USE_FULL=${1:-""}

if [ "$USE_FULL" == "full" ]; then
    echo "📊 Validating all clients against FULL Kaggle dataset"
else
    echo "📊 Validating all clients against standard Kaggle dataset"
fi

CLIENT_RESULTS_DIR="client/results"

if [ ! -d "$CLIENT_RESULTS_DIR" ]; then
    echo "❌ Error: Client results directory not found: $CLIENT_RESULTS_DIR"
    exit 1
fi

# Find all client directories
CLIENT_DIRS=($(find "$CLIENT_RESULTS_DIR" -maxdepth 1 -type d -name "client_*" | sort))

if [ ${#CLIENT_DIRS[@]} -eq 0 ]; then
    echo "❌ Error: No client directories found in $CLIENT_RESULTS_DIR"
    exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔍 Validating All Clients"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📁 Results directory: $CLIENT_RESULTS_DIR"
echo "📊 Found ${#CLIENT_DIRS[@]} client(s) to validate"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

TOTAL_CLIENTS=${#CLIENT_DIRS[@]}
PASSED_CLIENTS=0
FAILED_CLIENTS=0
CLIENT_RESULTS=()

for client_dir in "${CLIENT_DIRS[@]}"; do
    CLIENT_NAME=$(basename "$client_dir")
    
    echo "╔═══════════════════════════════════════════════════════════════╗"
    echo "║ 🔎 Validating: $CLIENT_NAME"
    echo "╚═══════════════════════════════════════════════════════════════╝"
    echo ""
    
    # Run validation for this client (all requests)
    if [ "$USE_FULL" == "full" ]; then
        ./validate_results.sh "$client_dir" full
    else
        ./validate_results.sh "$client_dir"
    fi
    
    if [ $? -eq 0 ]; then
        CLIENT_RESULTS+=("✅ $CLIENT_NAME: PASSED")
        PASSED_CLIENTS=$((PASSED_CLIENTS + 1))
    else
        CLIENT_RESULTS+=("❌ $CLIENT_NAME: FAILED")
        FAILED_CLIENTS=$((FAILED_CLIENTS + 1))
    fi
    
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
done

# Final global summary
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║ 📊 FINAL SUMMARY - ALL CLIENTS"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Total clients validated: $TOTAL_CLIENTS"
echo "✅ Passed:               $PASSED_CLIENTS"
echo "❌ Failed:               $FAILED_CLIENTS"
echo ""
echo "Results by client:"
for result in "${CLIENT_RESULTS[@]}"; do
    echo "  $result"
done
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $FAILED_CLIENTS -eq 0 ]; then
    echo "🎉 All clients passed validation!"
    exit 0
else
    echo "⚠️  Some clients failed validation. Please review the results above."
    exit 1
fi
