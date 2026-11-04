#!/bin/bash

set -e

PROJECT_DIR="/mnt/c/BDA-miniproject"
QUERY_DIR="$PROJECT_DIR/scripts/hive/queries"
RESULT_DIR="$PROJECT_DIR/reports/hive_results"

mkdir -p "$RESULT_DIR"

export JAVA_TOOL_OPTIONS="-Dorg.jline.terminal.providers=dumb -Dorg.jline.terminal.type=dumb"

QUERIES=(
query01_top_rated
query02_restaurants_by_locality
query03_popular_cuisines
query04_online_ordering
query05_average_rating
query06_top_voted
query07_average_cost
query08_restaurant_type
query09_table_booking
query10_rating_by_category
)

for QUERY in "${QUERIES[@]}"; do
    echo "========================================"
    echo "Executing $QUERY"
    echo "========================================"

    SQL=$(cat "$QUERY_DIR/$QUERY.sql")

    beeline -u 'jdbc:hive2:///' \
        --silent=true \
        --showHeader=true \
        --outputformat=tsv2 \
        -e "$SQL" \
        > "$RESULT_DIR/$QUERY.txt" 2>&1

    echo "Completed: $QUERY"
done

echo "========================================"
echo "All Hive queries executed successfully."
echo "Results saved in: $RESULT_DIR"
echo "========================================"
