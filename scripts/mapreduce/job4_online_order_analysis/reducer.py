
#!/usr/bin/env python3

import sys

current_location = None
online_total = 0
restaurant_total = 0

def output_result(location, online, total):
    if total > 0:
        adoption = (online / total) * 100
        print(
            f"{location}\t{online}\t{total}\t{adoption:.2f}"
        )

for line in sys.stdin:
    line = line.strip()

    try:
        location, online, total = line.rsplit("\t", 2)
        online = int(online)
        total = int(total)
    except ValueError:
        continue

    if current_location == location:
        online_total += online
        restaurant_total += total
    else:
        if current_location is not None:
            output_result(
                current_location,
                online_total,
                restaurant_total
            )

        current_location = location
        online_total = online
        restaurant_total = total

if current_location is not None:
    output_result(
        current_location,
        online_total,
        restaurant_total
    )
