import gzip
import os.path
import requests
import sys
import time

import key_pop_api_downloader as pgp


def main():
    url_pattern = pgp.get_config("input-txt-files/config.json", "ltla_url_pattern")
    max_var_selections = pgp.get_config("input-txt-files/config.json", "max_var_selections")
    skip_existing_files = '--skip-existing' in sys.argv
    input_classifications, _ = pgp.load_input_and_output_classification_codes()

    for num_vars in range(1, max_var_selections + 1):
        input_classification_combinations = pgp.get_input_classification_combinations(input_classifications, num_vars)
        for i, cc in enumerate(input_classification_combinations):
            c_str = ",".join(cc)
            compressed_file_path = 'downloaded/{}var-by-ltla/{}_by_geog.json.gz'.format(num_vars, c_str.replace(',', '-'))
            if skip_existing_files and os.path.isfile(compressed_file_path):
                print("{} var: Skipping existing file {} of {} ({})".format(num_vars, i+1, len(input_classification_combinations), c_str))
                continue
            print("{} var: Downloading {} of {} ({})".format(num_vars, i+1, len(input_classification_combinations), c_str))
            url = url_pattern.format(c_str)
            response = requests.get(url, stream=True)
            response_bytes = response.content
            with gzip.open(compressed_file_path, 'wb') as f:
                f.write(response_bytes)
            time.sleep(0.5)


if __name__ == "__main__":
    main()
