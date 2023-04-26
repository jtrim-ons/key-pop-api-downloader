import gzip
import itertools
import json
import re


def remove_classification_number(c):
    return re.sub(r'_[0-9]{1,3}[a-z]$', '', c)


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
        filename_elements = ["data"]
    else:
        filename_elements = sum(
            [[cat_id, opt['id']] for cat_id, opt in zip(cc, category_list)], []
        )
    return 'generated/{}var/{}.json'.format(len(cc), '-'.join(filename_elements))


def process_data(data, cc):
    # category_lists is a list of tuples like (1, 4), which means that the first
    # input variable has category 1 and the second input variable
    # has category 4.
    category_lists = itertools.product(
        *(all_classifications[c_]["categories"] for c_ in cc)
    )
    for category_list in category_lists:
        result = {}
        for dataset in data:
            c = dataset['c']
            if dataset['data']['blocked']:
                result[c] = "blocked"
                continue
            output_categories = all_classifications[c]['categories']
            result[c] = {}
            for cat in output_categories:
                datum_key = frozenset(
                    list(
                        (cat_id, opt['id'])
                        for cat_id, opt in zip(cc, category_list)
                    ) + [(c, cat['id'])]
                )
                result[c][cat['id']] = dataset['data'][datum_key]
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


for num_vars in range(0, 4):
    input_classification_combinations = [
        c for c in itertools.combinations(input_classifications, num_vars)
    ]
    for i, cc in enumerate(input_classification_combinations):
        data = []
        if num_vars > 0:
            classifications = output_classifications
        else:
            classifications = list(set(input_classifications + output_classifications))
        for j, c in enumerate(classifications):
            if remove_classification_number(c) in [
                remove_classification_number(c_) for c_ in cc
            ]:
                # The API won't give data for two versions of the same variable.
                # Since we haven't downloaded it, we can't use it to generate files :-)
                continue
            c_str = ",".join(list(cc) + [c])
            print("{} var: Processing {} of {} ({})".format(
                num_vars, i+1, len(input_classification_combinations), c_str)
            )
            compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(
                num_vars, c_str.replace(',', '-')
            )
            with gzip.open(compressed_file_path, 'r') as f:
                json_bytes = f.read()
            data.append({
                "c": c,
                "data": data_to_lookup(json.loads(json_bytes.decode('utf-8')))
            })
        process_data(data, cc)
