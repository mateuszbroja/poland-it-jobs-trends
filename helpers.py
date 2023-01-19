import requests
import yaml
import pandas as pd
from flatten_json import flatten
from google.cloud import storage
from google.cloud import bigquery


def read_yaml(filepath):
    with open(filepath, 'r') as file:
        data = yaml.safe_load(file)
    return data


def scrape_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        return response_data
    else:
        print('Failed to download data. Status code:', response.status_code)
        return []


def flat_json(json_file):
    flattened_file = (flatten(record, '.') for record in json_file)
    return pd.DataFrame(flattened_file)


def upload_to_bucket(path_to_key, bucket_name, file_name, dataframe):
    client = storage.Client.from_service_account_json(path_to_key)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(dataframe.to_csv(index=False), 'text/csv')
    print(f'File {file_name} was uploaded to {bucket_name}.')


def upload_to_bigquery(path_to_key, table_id, uri):
    client = bigquery.Client.from_service_account_json(path_to_key)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV)

    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()

    print(f"File has been uploaded to {table_id}")


def execute_query(path_to_key, query):
    client = bigquery.Client.from_service_account_json(path_to_key)
    query_job = client.query(query)
    query_job.result()
    print(f"Query executed.")