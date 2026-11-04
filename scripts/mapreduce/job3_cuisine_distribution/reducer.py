
#!/usr/bin/env python3

import sys

current_cuisine = None
total_count = 0

for line in sys.stdin:
    line = line.strip()

    try:
        cuisine, count = line.rsplit("\t", 1)
        count = int(count)
    except ValueError:
        continue

    if current_cuisine == cuisine:
        total_count += count
    else:
        if current_cuisine is not None:
            print(f"{current_cuisine}\t{total_count}")

        current_cuisine = cuisine
        total_count = count

if current_cuisine is not None:
    print(f"{current_cuisine}\t{total_count}")
