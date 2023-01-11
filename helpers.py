import requests
import json
import pandas as pd
import yaml
from flatten_json import flatten


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


def save_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


def flat_and_save_csv(jobs_json, output_csv_file):
    offers_flattened = (flatten(record, '.') for record in jobs_json)
    df = pd.DataFrame(offers_flattened)
    df.to_csv(output_csv_file, index=False)
