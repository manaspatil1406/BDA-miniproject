#!/usr/bin/env python3
"""
FoodieAnalytics: Food Delivery Analytics and Restaurant Recommendation System
Stage 4 - Preprocessing Utility: Prepare Newline-Safe Intermediate Input for Hadoop

Background:
Hadoop Streaming uses line-oriented TextInputFormat by default. In the cleaned
Zomato dataset, free-form text columns (e.g. 'reviews_list' and 'address') contain
embedded physical newline characters ('\n' and '\r').

When TextInputFormat splits such files across byte/line boundaries, a single
logical restaurant record is fractured across multiple lines, causing malformed
mapper inputs.

Purpose:
This script creates a safe, 1-to-1 line-oriented intermediate CSV:
    dataset/processed/hadoop_safe_zomato.csv
It preserves all 17 columns and all 51,656 data records, replacing physical
embedded newlines inside cell text with a single space.
This guarantees that:
    1 logical restaurant record == exactly 1 physical text line.
The original cleaned CSV ('cleaned_zomato.csv') remains completely unaltered.
"""

from pathlib import Path
import csv
import sys

# Increase CSV field size limit
csv.field_size_limit(2147483647)


def get_project_root():
    current_dir = Path.cwd()
    if (current_dir / "dataset" / "processed" / "cleaned_zomato.csv").exists():
        return current_dir
    elif (current_dir.parent / "dataset" / "processed" / "cleaned_zomato.csv").exists():
        return current_dir.parent
    elif (current_dir.parent.parent / "dataset" / "processed" / "cleaned_zomato.csv").exists():
        return current_dir.parent.parent
    return Path(__file__).resolve().parent.parent.parent


def prepare_safe_dataset():
    root = get_project_root()
    input_path = root / "dataset" / "processed" / "cleaned_zomato.csv"
    output_path = root / "dataset" / "processed" / "hadoop_safe_zomato.csv"

    if not input_path.exists():
        print(f"[ERROR] Cleaned dataset not found at: {input_path}")
        sys.exit(1)

    print("=" * 70)
    print(" FOODIEANALYTICS: PREPARING HADOOP NEWLINE-SAFE INTERMEDIATE INPUT ")
    print("=" * 70)
    print(f"Source (Unaltered) : {input_path}")
    print(f"Destination Safe   : {output_path}")

    record_count = 0
    header = None

    with open(input_path, "r", encoding="utf-8", errors="replace") as infile, \
         open(output_path, "w", encoding="utf-8", newline="") as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)

        for row in reader:
            if header is None:
                header = row
                writer.writerow(header)
                continue

            # Replace embedded carriage returns and newlines within cell text with a single space
            sanitized_row = [
                cell.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").strip()
                for cell in row
            ]
            writer.writerow(sanitized_row)
            record_count += 1

    # Verify line counts
    physical_lines = 0
    with open(output_path, "r", encoding="utf-8") as f:
        for _ in f:
            physical_lines += 1

    file_size_mb = output_path.stat().st_size / (1024 * 1024)

    print(f"SUCCESS: Intermediate safe dataset generated!")
    print(f"  - Data Records Processed: {record_count:,}")
    print(f"  - Total Physical Lines  : {physical_lines:,} (1 Header + {record_count:,} Records)")
    print(f"  - Intermediate File Size: {file_size_mb:.2f} MB")
    print(f"  - Line-Oriented Integrity: {'VERIFIED (1 record per line)' if physical_lines == record_count + 1 else 'MISMATCH'}")
    print("=" * 70)


if __name__ == "__main__":
    prepare_safe_dataset()

