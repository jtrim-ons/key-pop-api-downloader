import gzip
import itertools
import json
import os

from key_pop_api_downloader import *

with open('generated/all-classifications.json', 'r') as f:
    all_classifications = json.load(f)

with open('input-txt-files/output-classifications.txt', 'r') as f:
    output_classifications = f.read().splitlines()
output_classifications.sort()

with open('input-txt-files/input-classifications.txt', 'r') as f:
    input_classifications = f.read().splitlines()
input_classifications.sort()


def generate_outfile_path(cc, category_list):
    if len(cc) == 0:
        raise "cc should have at least one element."

    directory_names = [cat_id + '-' + opt['id'] for cat_id, opt in zip(cc, category_list)]
    directory = 'generated/{}var/{}'.format(len(cc), '/'.join(directory_names))
    os.makedirs(directory, exist_ok=True)
    return directory + '/' + cc[-1] + '.json'


def make_datum_key(cc, category_list, c, cat):
    datum_key = frozenset(
        list(
            (classification_id, opt['id'])
            for classification_id, opt in zip(cc, category_list)
            if (
                remove_classification_number(c) != "resident_age"
                or remove_classification_number(classification_id) != "resident_age"
            )
        ) + [(c, cat['id'])]
    )
    return datum_key


def generate_one_dataset(data, cc, category_list):
    result = {}
    for dataset in data:
        c = dataset['c']
        if dataset['data']['blocked']:
            result[c] = "blocked"
            continue
        output_categories = all_classifications[c]['categories']
        result[c] = {}
        for cat in output_categories:
            datum_key = make_datum_key(cc, category_list, c, cat)
            result[c][cat['id']] = dataset['data'][datum_key]
    return result


def process_data(data, cc):
    if len(cc) == 0:
        result = generate_one_dataset(data, cc, [])
        with open('generated/0var/data.json', 'w') as f:
            json.dump(result, f)
    else:
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
                dataset = generate_one_dataset(data, cc, (*category_list, last_var_category))
                result[last_var_category['id']] = dataset
            with open(generate_outfile_path(cc, category_list), 'w') as f:
                json.dump(result, f)


def data_to_lookup(data):
    if data["blocked_areas"] != 0:
        return {'blocked': True}
    lookup = {}
    for obs in data['observations']:
        dimensions = []
        for dim in obs['dimensions']:
            if dim['dimension_id'] == 'nat':
                # This is the geo dimension, so ignore it
                continue
            dimensions.append((dim['dimension_id'], dim['option_id']))
        lookup[frozenset(dimensions)] = obs['observation']

    lookup['blocked'] = False
    return lookup


def make_c_str(cc, c):
    # This is used to generate a file name for a list of input classifications and
    # an output classification.  If the output classification is resident_age_23a,
    # then any resident_age input classifications are deleted.
    classifications = []
    for c_ in list(cc):
        if remove_classification_number(c) != "resident_age" or remove_classification_number(c_) != "resident_age":
            classifications.append(c_)
    classifications.append(c)
    return len(classifications), ",".join(classifications)


for num_vars in range(0, 4):
    input_classification_combinations = get_input_classification_combinations(input_classifications, num_vars)
    for i, cc in enumerate(input_classification_combinations):
        data = []
        if num_vars > 0:
            classifications = output_classifications
        else:
            classifications = list(set(input_classifications + output_classifications))
        for j, c in enumerate(classifications):
            if remove_classification_number(c) != "resident_age" and remove_classification_number(c) in [
                remove_classification_number(c_) for c_ in cc
            ]:
                # The API won't give data for two versions of the same variable.
                # Since we haven't downloaded it, we can't use it to generate files :-)
                # The exception is for resident_age, which is a special case where
                # we just use the data for 23 categories.
                continue
            c_str_len, c_str = make_c_str(cc, c)
            print("{} var: Processing {} of {} ({})".format(
                num_vars, i+1, len(input_classification_combinations), c_str)
            )
            compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(
                c_str_len-1, c_str.replace(',', '-')
            )
            with gzip.open(compressed_file_path, 'r') as f:
                json_bytes = f.read()
            data.append({
                "c": c,
                "data": data_to_lookup(json.loads(json_bytes.decode('utf-8')))
            })
        process_data(data, cc)
