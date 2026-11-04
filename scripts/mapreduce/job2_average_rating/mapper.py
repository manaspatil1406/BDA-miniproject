
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
        location = row[8].strip()
        rating = row[5].strip()

        if not location or not rating:
            continue

        rating = float(rating)

        if rating < 1 or rating > 5:
            continue

        print(f"{location}\t{rating}")

    except (ValueError, IndexError):
        continue
