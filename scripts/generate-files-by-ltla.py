import gzip
import itertools
import json
import re


def remove_classification_number(c):
    return re.sub(r'_[0-9]{1,3}[a-z]$', '', c)


with open('generated/all-classifications.json', 'r') as f:
    all_classifications = json.load(f)

with open('downloaded/ltla-geog.json', 'r') as f:
    ltlas = [item["id"] for item in json.load(f)["items"]]

with open('input-txt-files/input-classifications.txt', 'r') as f:
    input_classifications = f.read().splitlines()
input_classifications.sort()


def generate_outfile_path(cc, category_list):
    filename_elements = sum(
        [[cat_id, opt['id']] for cat_id, opt in zip(cc, category_list)], []
    )
    return 'generated/{}var-by-ltla/{}_by_geog.json'.format(
        len(cc), '-'.join(filename_elements)
    )


def process_data(data, cc):
    # category_lists is a list of tuples like (1, 4), which means that the first
    # input variable has category 1 and the second input variable
    # has category 4.
    category_lists = itertools.product(
        *(all_classifications[c_]["categories"] for c_ in cc)
    )
    for category_list in category_lists:
        result = {}
        for ltla in ltlas:
            datum_key = frozenset(
                [
                    (cat_id, opt['id'])
                    for cat_id, opt in zip(cc, category_list)
                ] + [('ltla', ltla)]
            )
            if datum_key in data:
                result[ltla] = data[datum_key]
            else:
                result[ltla] = None
        with open(generate_outfile_path(cc, category_list), 'w') as f:
            json.dump(result, f)


def data_to_lookup(data):
    lookup = {}
    if data['blocked_areas'] == 331:
        # .observations will be null in the JSON file from the API
        data['observations'] = []
    for obs in data['observations']:
        dimensions = []
        for dim in obs['dimensions']:
            dimensions.append((dim['dimension_id'], dim['option_id']))
        lookup[frozenset(dimensions)] = obs['observation']

    return lookup


for num_vars in range(1, 4):
    input_classification_combinations = [
        c for c in itertools.combinations(input_classifications, num_vars)
    ]
    for i, cc in enumerate(input_classification_combinations):
        c_str = ",".join(cc)
        print("{} var: Processing {} of {} ({})".format(
            num_vars, i+1, len(input_classification_combinations), c_str)
        )
        compressed_file_path = 'downloaded/{}var-by-ltla/{}_by_geog.json.gz'.format(
            num_vars, c_str.replace(',', '-')
        )
        with gzip.open(compressed_file_path, 'r') as f:
            json_bytes = f.read()
        data = data_to_lookup(json.loads(json_bytes.decode('utf-8')))
        process_data(data, cc)
