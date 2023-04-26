import gzip
import itertools
import re
import requests
import time

url_pattern = "https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations?area-type=ltla&dimensions={}&limit=10000000"

with open('input-txt-files/output-classifications.txt', 'r') as f:
    output_classifications = f.read().splitlines()
output_classifications.sort()

with open('input-txt-files/input-classifications.txt', 'r') as f:
    input_classifications = f.read().splitlines()
input_classifications.sort()

for num_vars in range(1, 4):
    input_classification_combinations = [c for c in itertools.combinations(input_classifications, num_vars)]
    for i, cc in enumerate(input_classification_combinations):
        c_str = ",".join(cc)
        print("{} var: Downloading {} of {} ({})".format(num_vars, i+1, len(input_classification_combinations), c_str))
        url = url_pattern.format(c_str)
        response = requests.get(url, stream=True)
        response_bytes = response.content
        compressed_file_path = 'downloaded/{}var-by-ltla/{}_by_geog.json.gz'.format(num_vars, c_str.replace(',', '-'))
        with gzip.open(compressed_file_path, 'wb') as f:
            f.write(response_bytes)
        time.sleep(0.5)
