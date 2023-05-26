import gzip
import itertools
import re
import requests
import time

from key_pop_api_downloader import *

url_pattern = "https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations?area-type=nat&dimensions={}&limit=10000000"

with open('input-txt-files/output-classifications.txt', 'r') as f:
    output_classifications = f.read().splitlines()
output_classifications.sort()

with open('input-txt-files/input-classifications.txt', 'r') as f:
    input_classifications = f.read().splitlines()
input_classifications.sort()

for num_vars in range(0, 4):
    input_classification_combinations = get_input_classification_combinations(input_classifications, num_vars)
    for i, cc in enumerate(input_classification_combinations):
        for j, c in enumerate(output_classifications if num_vars > 0 else list(set(input_classifications + output_classifications))):
            if remove_classification_number(c) in [remove_classification_number(c_) for c_ in cc]:
                # The API won't give data for two versions of the same variable
                continue
            c_str = ",".join(list(cc) + [c])
            print("{} var: Downloading {} of {} ({})".format(num_vars, i+1, len(input_classification_combinations), c_str))
            url = url_pattern.format(c_str)
            response = requests.get(url, stream=True)
            response_bytes = response.content
            compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(num_vars, c_str.replace(',', '-'))
            with gzip.open(compressed_file_path, 'wb') as f:
                f.write(response_bytes)
            time.sleep(0.5)
