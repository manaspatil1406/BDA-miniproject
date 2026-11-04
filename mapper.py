
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
        online_order = row[3].strip().lower()

        if not location:
            location = "Unknown"

        if online_order not in ("true", "false"):
            continue

        online_count = 1 if online_order == "true" else 0

        print(f"{location}\t{online_count}\t1")

    except IndexError:
        continue
