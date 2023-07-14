import gzip
import os.path
import requests
import sys
import time

from key_pop_api_downloader import remove_classification_number
from key_pop_api_downloader import get_input_classification_combinations
from key_pop_api_downloader import get_config
from key_pop_api_downloader import get_input_and_output_classification_codes

url_pattern = get_config("input-txt-files/config.json", "national_url_pattern")

skip_existing_files = '--skip-existing' in sys.argv

input_classifications, output_classifications = get_input_and_output_classification_codes()


def get_file(compressed_file_path, c_str):
    if skip_existing_files and os.path.isfile(compressed_file_path):
        print("Skipping existing file {}".format(compressed_file_path))
        return
    print("Downloading {}".format(compressed_file_path))
    url = url_pattern.format(c_str)
    response = requests.get(url, stream=True)
    response_bytes = response.content
    with gzip.open(compressed_file_path, 'wb') as f:
        f.write(response_bytes)
    time.sleep(0.5)


def main():
    for num_vars in range(0, 4):
        input_classification_combinations = get_input_classification_combinations(input_classifications, num_vars)
        for i, cc in enumerate(input_classification_combinations):
            if num_vars > 0:
                c_str = ",".join(cc)
                compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(num_vars, c_str.replace(',', '-'))
                get_file(compressed_file_path, c_str)
            for j, c in enumerate(output_classifications):
                if remove_classification_number(c) in [remove_classification_number(c_) for c_ in cc]:
                    # The API won't give data for two versions of the same variable
                    continue
                c_str = ",".join(list(cc) + [c])
                compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(num_vars, c_str.replace(',', '-'))
                get_file(compressed_file_path, c_str)


if __name__ == "__main__":
    main()
