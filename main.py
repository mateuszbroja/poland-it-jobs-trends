import time
from helpers import read_yaml, scrape_data, save_json, flat_and_save_csv
from tests import count_json_records_and_columns, count_csv_records_and_columns

config = read_yaml('docs/config.yaml')
timestamp = time.strftime("%y%m%d_%H%M%S")

for website_name, website in config['api_websites'].items():
    json_path = f"{website['output_path']}{timestamp}.json"
    csv_path = f"{website['output_path']}{timestamp}.csv"

    jobs_json = scrape_data(website['url'])
    save_json(jobs_json, json_path)

    if website_name == 'nofluffjobs':
        flat_and_save_csv(jobs_json['postings'], csv_path)
    elif website_name == 'justjoinit':
        flat_and_save_csv(jobs_json, csv_path)

    record_count, column_count = count_csv_records_and_columns(csv_path)
    print(f"File: {csv_path}")
    print(f"Number of records: {record_count}")
    print(f"Number of columns: {column_count}")