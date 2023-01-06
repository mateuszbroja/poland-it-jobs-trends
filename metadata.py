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


def count_json_records_and_columns(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Count number of records and columns in JSON file
    record_count = len(data)
    column_count = len(data[0]) if data else 0

    return record_count, column_count


# Set the directory you want to read
directory = 'output/justjoinit'

# Iterate through all files in the directory
for filename in os.listdir(directory):
    file_path = os.path.join(directory, filename)

    # Check if file is in CSV format
    if filename.endswith('.csv'):
        record_count, column_count = count_csv_records_and_columns(file_path)

        print(f"File: {filename}")
        print(f"Number of records: {record_count}")
        print(f"Number of columns: {column_count}")

    # Check if file is in JSON format
    elif filename.endswith('.json'):
        record_count, column_count = count_json_records_and_columns(file_path)

        print(f"File: {filename}")
        print(f"Number of records: {record_count}")
        print(f"Number of columns: {column_count}")
