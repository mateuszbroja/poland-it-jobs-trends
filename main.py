import time
from helpers import read_yaml, scrape_data, flat_json, upload_to_bucket,\
                    upload_to_bigquery, execute_query

config = read_yaml('docs/config.yaml')
timestamp = time.strftime("%y%m%d_%H%M%S")

project_id = config['google_cloud']['project_id']
credentials_path = config['google_cloud']['credentials_path']
bucket_name = config['google_cloud']['bucket_name']


for website_name, website in config['api_websites'].items():
    # Ingest data from APIs
    if website_name == 'nofluffjobs':
        jobs_json = scrape_data(website['url'])['postings']
    else:
        jobs_json = scrape_data(website['url'])

    # Upload to the Google Cloud Storage
    df_jobs = flat_json(jobs_json)
    upload_to_bucket(credentials_path, bucket_name, f'{timestamp}.csv', df_jobs)

    # Load to the Google BigQuery - bronze
    bronze_table_id = f'{project_id}.Landing.bronze_{website_name}'
    query = f"TRUNCATE TABLE `{bronze_table_id}`"
    execute_query(credentials_path, query)

    uri = f'gs://landing_jobs/{timestamp}.csv'
    upload_to_bigquery(credentials_path, bronze_table_id, uri)

