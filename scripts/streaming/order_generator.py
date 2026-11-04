
import json
import os
import random
import time
from datetime import datetime

# ==========================================
# FOODIEANALYTICS - SIMULATED ORDER GENERATOR
# ==========================================

OUTPUT_DIR = (
    "/mnt/c/BDA-miniproject/dataset/streaming/orders_input"
)

TOTAL_BATCHES = 10
ORDERS_PER_BATCH = 5
INTERVAL_SECONDS = 5

RESTAURANT_IDS = [
    "REST001", "REST002", "REST003",
    "REST004", "REST005", "REST006",
    "REST007", "REST008", "REST009",
    "REST010"
]

ORDER_STATUSES = [
    "Placed",
    "Preparing",
    "Out for Delivery",
    "Delivered"
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("\n==========================================")
print(" FOODIEANALYTICS - ORDER GENERATOR")
print("==========================================")

print("Output directory:", OUTPUT_DIR)
print("Total batches:", TOTAL_BATCHES)
print("Orders per batch:", ORDERS_PER_BATCH)

order_counter = 1001

for batch_number in range(1, TOTAL_BATCHES + 1):

    orders = []

    for _ in range(ORDERS_PER_BATCH):

        order = {
            "order_id": f"ORD{order_counter}",
            "restaurant_id": random.choice(RESTAURANT_IDS),
            "customer_id": f"CUST{random.randint(1, 100)}",
            "order_amount": round(
                random.uniform(150, 2000), 2
            ),
            "order_status": random.choice(ORDER_STATUSES),
            "order_timestamp": datetime.now().isoformat()
        }

        orders.append(order)
        order_counter += 1

    filename = f"orders_batch_{batch_number:03d}.json"
    filepath = os.path.join(OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as file:
        for order in orders:
            file.write(json.dumps(order) + "\n")

    print(
        f"Batch {batch_number}/{TOTAL_BATCHES} generated: "
        f"{len(orders)} orders -> {filename}"
    )

    if batch_number < TOTAL_BATCHES:
        time.sleep(INTERVAL_SECONDS)

print("\n==========================================")
print("ORDER GENERATION COMPLETED")
print("Total orders generated:", order_counter - 1001)
print("Total batches generated:", TOTAL_BATCHES)
print("==========================================")
