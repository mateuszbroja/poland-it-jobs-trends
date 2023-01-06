import time

import requests
import json
import pandas as pd
from flatten_json import flatten


def scrape_data(url, fields):
    response = requests.get(url)
    if response.status_code == 200:
        response_data = response.json()
        selected_response_data = [{key: offer[key] for key in fields} for offer in response_data]
        return selected_response_data
    else:
        print('Failed to download data. Status code:', response.status_code)
        return []


def save_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f)


def transform_json_to_csv(input_json_file, output_csv_file):
    with open(input_json_file, 'r') as f:
        offers = json.load(f)

    offers_flattened = (flatten(record, '.') for record in offers)
    df = pd.DataFrame(offers_flattened)
    df.to_csv(output_csv_file, index=False)


selected_fields = ['title', 'city', 'country_code', 'workplace_type', 'company_name', 'experience_level',
                   'published_at', 'id', 'marker_icon', 'employment_types', 'skills', 'remote']

timestamp = time.strftime("%y%m%d_%H%M%S")

jobs_data = scrape_data('https://justjoin.it/api/offers', selected_fields)
save_json(jobs_data, f'output/justjoinit/{timestamp}.json')
transform_json_to_csv(f'output/justjoinit/{timestamp}.json', f'output/justjoinit/{timestamp}.csv')