
#!/usr/bin/env python3

import sys

current_location = None
total_rating = 0.0
rating_count = 0

for line in sys.stdin:
    line = line.strip()

    try:
        location, rating = line.rsplit("\t", 1)
        rating = float(rating)
    except ValueError:
        continue

    if current_location == location:
        total_rating += rating
        rating_count += 1
    else:
        if current_location is not None and rating_count > 0:
            average = total_rating / rating_count
            print(
                f"{current_location}\t{average:.2f}\t{rating_count}"
            )

        current_location = location
        total_rating = rating
        rating_count = 1

if current_location is not None and rating_count > 0:
    average = total_rating / rating_count
    print(f"{current_location}\t{average:.2f}\t{rating_count}")
