import csv
import json
import os


def count_csv_records_and_columns(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        records = list(reader)

    # Count number of records and columns in CSV file
    record_count = len(records)
    column_count = len(records[0]) if records else 0

    return record_count, column_count