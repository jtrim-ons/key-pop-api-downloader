import gzip
import itertools
import requests
import time

url_pattern = "https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations?area-type=nat&dimensions={}&limit=1000000"

with open('classifications.txt', 'r') as f:
    classifications = f.read().splitlines()

classifications.sort()

for num_vars in range(2, 3):
    classification_combinations = [c for c in itertools.combinations(classifications, num_vars)]
    for i, c in enumerate(classification_combinations):
        c_str = ",".join(c)
        print("{} var: Downloading {} of {} ({})".format(num_vars, i+1, len(classification_combinations), c_str))
        url = url_pattern.format(c_str)
        response = requests.get(url, stream=True)
        response_bytes = response.content
        compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(num_vars, c_str.replace(',', '-'))
        with gzip.open(compressed_file_path, 'wb') as f:
            f.write(response_bytes)
        time.sleep(0.5)
