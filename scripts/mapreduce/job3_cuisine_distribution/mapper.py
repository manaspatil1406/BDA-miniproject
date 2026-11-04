
#!/usr/bin/env python3

import sys
import csv

csv.field_size_limit(sys.maxsize)

reader = csv.reader(sys.stdin)

header_skipped = False

for row in reader:
    if not header_skipped:
        header_skipped = True
        continue

    try:
        cuisines = row[11].strip()

        if not cuisines:
            continue

        individual_cuisines = cuisines.split(",")

        for cuisine in individual_cuisines:
            cuisine = cuisine.strip()

            if cuisine:
                print(f"{cuisine}\t1")

    except IndexError:
        continue
