import itertools
import json

import key_pop_api_downloader as pgp

max_var_selections = pgp.get_config("input-txt-files/config.json", "max_var_selections")

with open('downloaded/ltla-geog.json', 'r') as f:
    ltlas = [item["id"] for item in json.load(f)["items"]]

all_classifications = pgp.load_all_classifications()
input_classifications, _ = pgp.load_input_and_output_classification_codes()


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
            result[ltla] = [count, pgp.round_fraction(100 * count, ltla_sums[ltla], 1)]
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
        with open(pgp.generate_outfile_path(cc, category_list, 'generated/{}var-by-ltla_percent/{}', '_by_geog.json'), 'w') as f:
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


for num_vars in range(1, max_var_selections + 1):
    input_classification_combinations = pgp.get_input_classification_combinations(input_classifications, num_vars)
    for i, cc in enumerate(input_classification_combinations):
        c_str = ",".join(cc)
        print("{} var: Processing {} of {} ({})".format(
            num_vars, i+1, len(input_classification_combinations), c_str)
        )
        compressed_file_path = 'downloaded/{}var-by-ltla/{}_by_geog.json.gz'.format(
            num_vars, c_str.replace(',', '-')
        )
        data, ltla_sums = data_to_lookups(pgp.read_json_gz(compressed_file_path))
        process_data(data, ltla_sums, cc)
