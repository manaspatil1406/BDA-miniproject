#!/usr/bin/env python3
"""
FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System
Stage 4 - MapReduce Job 1: Restaurant Count by Locality
Reducer: reducer.py

Description:
Receives sorted <location>\t1 key-value pairs from standard input (sys.stdin).
Aggregates the total count of restaurants for each unique locality.
Emits the aggregated key-value pair to standard output:
    <location>\t<total_restaurant_count>
"""

import sys


def main():
    current_location = None
    current_count = 0

    # Process each line streamed from Hadoop Shuffle & Sort phase
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        # Split line on tab delimiter into key (location) and value (count)
        parts = line.split("\t")
        if len(parts) != 2:
            continue

        location, count_str = parts[0], parts[1]

        # Safely convert count to integer
        try:
            count = int(count_str)
        except ValueError:
            # Skip record if count is not a valid integer
            continue

        # In Hadoop MapReduce, all identical keys arrive consecutively
        if current_location == location:
            current_count += count
        else:
            # New key encountered: emit previously accumulated location count
            if current_location is not None:
                print(f"{current_location}\t{current_count}")
            current_location = location
            current_count = count

    # Emit the last accumulated location after the input stream ends
    if current_location is not None:
        print(f"{current_location}\t{current_count}")


if __name__ == "__main__":
    main()

