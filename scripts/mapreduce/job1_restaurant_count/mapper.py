#!/usr/bin/env python3
"""
FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System
Stage 4 - MapReduce Job 1: Restaurant Count by Locality
Mapper: mapper.py

Description:
Reads restaurant records from standard input (sys.stdin).
Extracts the 'location' column (index 8) from the CSV row.
Emits the key-value pair to standard output:
    <location>\t1

CSV Schema (17 columns):
0: url, 1: address, 2: name, 3: online_order, 4: book_table, 5: rate,
6: votes, 7: phone, 8: location, 9: rest_type, 10: dish_liked,
11: cuisines, 12: approx_cost_for_two_people, 13: reviews_list,
14: menu_item, 15: listed_in_type, 16: listed_in_city
"""

import sys
import csv

# Increase field size limit to handle large text fields safely
csv.field_size_limit(2147483647)


def main():
    # Read CSV rows streamed from Hadoop TextInputFormat via standard input
    reader = csv.reader(sys.stdin)

    for row in reader:
        # Skip empty lines
        if not row:
            continue

        # Guard: Check and skip the CSV header line
        if len(row) > 8 and (row[0].strip().lower() == "url" or row[8].strip().lower() == "location"):
            continue

        # Extract the location column (index 8)
        if len(row) > 8:
            location = row[8].strip()

            # Handle missing or empty location gracefully
            if not location or location.lower() == "nan":
                location = "Unknown"

            # Emit key and count of 1 separated by a tab
            # Hadoop shuffle-and-sort phase will sort records by this key
            print(f"{location}\t1")


if __name__ == "__main__":
    main()

