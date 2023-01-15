import requests
import json
import pandas as pd
import yaml
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


# def save_json(data, filename):
#     with open(filename, 'w') as f:
#         json.dump(data, f)

def flat_json(json_file):
    flattened_file = (flatten(record, '.') for record in json_file)
    return flattened_file


# def flat_and_save_csv(jobs_json, output_csv_file):
#     offers_flattened = (flatten(record, '.') for record in jobs_json)
#     df = pd.DataFrame(offers_flattened)
#     # df.to_csv(output_csv_file, index=False)
#     pandas_gbq.to_gbq(df, table_id, project_id=project_id)
#     df, 'my_dataset.my_table', project_id=projectid, if_exists='fail',
# )

def json_to_string(json_data):
    json_string = json.dumps(json_data, ensure_ascii=False)
    return json_string


def upload_to_bucket(path_to_key, bucket_name, file_name, file):
    client = storage.Client.from_service_account_json(path_to_key)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(file, content_type='application/json')
    print(f'File {file_name} was uploaded to {bucket_name}.')


def upload_to_bigquery(path_to_key, table_id, uri):
    client = bigquery.Client.from_service_account_json(path_to_key)
    job_config = bigquery.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("name", "STRING"),
        #     bigquery.SchemaField("post_abbr", "STRING"),
        # ],
        autodetect=True,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    print(f"File has been uploaded to {table_id}")
