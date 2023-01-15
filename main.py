import time
from helpers import read_yaml, scrape_data, flat_and_save_csv, json_to_string, upload_to_bucket

# from tests import count_csv_records_and_columns

config = read_yaml('docs/config.yaml')
timestamp = time.strftime("%y%m%d_%H%M%S")

project_id = config['google_cloud']['project_id']
credentials_path = config['google_cloud']['credentials_path']
bucket_name = config['google_cloud']['bucket_name']

for website_name, website in config['api_websites'].items():
    # json_path = f"{website['output_path']}{timestamp}.json"
    # csv_path = f"{website['output_path']}{timestamp}.csv"

    # Request data from API
    jobs_json = scrape_data(website['url'])
    jobs_string = json_to_string(jobs_json)

    # Load to Google Cloud Storage
    upload_to_bucket(credentials_path, bucket_name, f'{timestamp}.json', jobs_string)

    # Load to Google BigQuery
    bronze_table_id = f'{project_id}.Landing.bronze_{website_name}'
    uri = f'gs://landing_jobs/{timestamp}.json'
    upload_to_bigquery(credentials_path, bronze_table_id, bucket_uri)

    if website_name == 'nofluffjobs':
        flat_and_save_csv(jobs_json['postings'], csv_path)
    elif website_name == 'justjoinit':
        flat_and_save_csv(jobs_json, csv_path)

    upload_to_bucket(credentials_path, bucket_name, f'{timestamp}.json', jobs_string)
    # do some tests on the data
    # create some metadata to all this

upload_to_bigquery(credentials_path, bronze_table_id, uri)