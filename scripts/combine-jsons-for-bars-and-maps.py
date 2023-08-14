# This script combines pairs of JSON files: for a given set of input variable-values,
# the file for the bar chart and the file for the map are combined.

import glob
import json
import os
import key_pop_api_downloader as pgp
from pathlib import Path

max_var_selections = pgp.get_config('input-txt-files/config.json', 'max_var_selections')

for i in range(1, max_var_selections + 1):
    combined_path = f'generated/{i}var-combined_percent/'

    filenames = glob.glob(f'generated/{i}var_percent/**/*.json', recursive=True)
    for filename in filenames:
        short_filename = filename.replace(f'generated/{i}var_percent/', '')
        with open(filename, 'r') as f:
            bar_chart_data = json.load(f)
        map_data_filename = f'generated/{i}var-by-ltla_percent/' + short_filename.replace('.json', '_by_geog.json')
        with open(map_data_filename, 'r') as f:
            map_data = json.load(f)
        combined_data = {
            'bar_chart_data': bar_chart_data,
            'map_data': map_data
        }
        Path(combined_path + short_filename).parent.mkdir(parents=True, exist_ok=True)
        with open(combined_path + short_filename, 'w') as f:
            json.dump(combined_data, f)


