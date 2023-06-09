import copy
import gzip
import itertools
import json
import os
import re

from key_pop_api_downloader import round_fraction
from key_pop_api_downloader import get_input_classification_combinations


with open('generated/all-classifications.json', 'r') as f:
    all_classifications = json.load(f)

with open('downloaded/ltla-geog.json', 'r') as f:
    ltlas = [item["id"] for item in json.load(f)["items"]]

with open('input-txt-files/input-classifications.txt', 'r') as f:
    input_classifications = f.read().splitlines()
input_classifications.sort()


def generate_outfile_path(cc, category_list):
    if len(cc) == 0:
        raise "cc should have at least one element."

    directory_names = [cat_id + '-' + opt['id'] for cat_id, opt in zip(cc, category_list)]
    directory = 'generated/{}var-by-ltla/{}'.format(len(cc), '/'.join(directory_names))
    os.makedirs(directory, exist_ok=True)
    return directory + '/' + cc[-1] + '_by_geog.json'


def generate_one_dataset(data, ltla_sums, cc, category_list):
    result = {}
    for ltla in ltlas:
        datum_key = frozenset(
            [
                (cat_id, opt['id'])
                for cat_id, opt in zip(cc, category_list)
            ] + [('ltla', ltla)]
        )
        if datum_key in data:
            count = data[datum_key]
            result[ltla] = [count, round_fraction(100 * count, ltla_sums[ltla], 1)]
        else:
            result[ltla] = None
    return result


def process_data(data, ltla_sums, cc):
    # category_lists is a list of tuples like (1, 4), which means that the first
    # input variable has category 1 and the second input variable
    # has category 4.  We do this for all but the last element of the list cc.
    #
    # For the final variable in cc, we will generate a dataset for
    # each value and combine these all in a single file.
    category_lists = itertools.product(
        *(all_classifications[c_]["categories"] for c_ in cc[:-1])
    )
    for category_list in category_lists:
        result = {}
        for last_var_category in all_classifications[cc[-1]]["categories"]:
            dataset = generate_one_dataset(data, ltla_sums, cc, (*category_list, last_var_category))
            result[last_var_category['id']] = dataset
        with open(generate_outfile_path(cc, category_list), 'w') as f:
            json.dump(result, f)


def data_to_lookups(data):
    lookup = {}
    ltla_sums = {}
    if data['blocked_areas'] == 331:
        # .observations will be null in the JSON file from the API
        data['observations'] = []
    for obs in data['observations']:
        dimensions = []
        for dim in obs['dimensions']:
            dimensions.append((dim['dimension_id'], dim['option_id']))
            if dim['dimension_id'] == 'ltla':
                ltla = dim['option_id']
                if ltla not in ltla_sums:
                    ltla_sums[ltla] = 0
                ltla_sums[ltla] += obs['observation']
        lookup[frozenset(dimensions)] = obs['observation']

    return lookup, ltla_sums


TMP_COUNT = 0

for num_vars in range(1, 4):
    input_classification_combinations = get_input_classification_combinations(input_classifications, num_vars)
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
        data, ltla_sums = data_to_lookups(json.loads(json_bytes.decode('utf-8')))
        TMP_COUNT += 1
        with open('generated/TMP-LTLA-POPS/{}.txt'.format(TMP_COUNT), 'w') as f:
            for k, v in ltla_sums.items():
                if v > 0:
                    f.write('{} {}\n'.format(k, v))
        process_data(data, ltla_sums, cc)

### # Finally, add male and female counts to give total population
### filenames_by_sex = [
###     "generated/1var-by-ltla/sex-1_by_geog.json",
###     "generated/1var-by-ltla/sex-2_by_geog.json"
### ]
### totals_filename = 'generated/0var-by-ltla/data_by_geog.json'
### data_by_sex = []
### for filename in filenames_by_sex:
###     with open(filename, 'r') as f:
###         data = json.load(f)
###         data_by_sex.append(data)
### totals = copy.deepcopy(data_by_sex[0])
### for key in totals:
###     totals[key] += data_by_sex[1][key]
### with open(totals_filename, 'w') as f:
###     json.dump(totals, f)
