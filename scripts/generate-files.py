import gzip
import itertools
import json
import os

from key_pop_api_downloader import round_fraction
from key_pop_api_downloader import remove_classification_number
from key_pop_api_downloader import get_input_classification_combinations
from key_pop_api_downloader import age_band_text_to_numbers
from key_pop_api_downloader import generate_outfile_path
from key_pop_api_downloader import load_input_and_output_classification_codes
from key_pop_api_downloader import load_all_classifications
from key_pop_api_downloader import load_output_classification_details
from key_pop_api_downloader import get_config

max_var_selections = get_config("input-txt-files/config.json", "max_var_selections")
all_classifications = load_all_classifications()
input_classifications, output_classifications = load_input_and_output_classification_codes()
output_classification_details_dict = load_output_classification_details(all_classifications)


def make_datum_key(cc, category_list, c, cell_id):
    return frozenset(
        list(
            (classification_id, opt['id'])
            for classification_id, opt in zip(cc, category_list)
            if (
                remove_classification_number(c) != "resident_age"
                or remove_classification_number(classification_id) != "resident_age"
            )
        ) + [(c, str(cell_id))]
    )


def make_datum_key_for_pop_totals(cc, category_list):
    return frozenset([
        (classification_id, opt['id'])
        for classification_id, opt in zip(cc, category_list)
    ])


def input_age_range(cc, category_list):
    for i, classification in enumerate(cc):
        if remove_classification_number(classification) == "resident_age":
            return age_band_text_to_numbers(category_list[i]["label"])
    return [0, 999]


def nests_nicely(c, input_age_range):
    if remove_classification_number(c) != "resident_age":
        return True
    for category in all_classifications[c]['categories']:
        age_band = age_band_text_to_numbers(category['label'])
        if age_band[0] < input_age_range[0] and age_band[1] >= input_age_range[0]:
            return False
        if age_band[0] <= input_age_range[1] and age_band[1] > input_age_range[1]:
            return False
    return True


def sum_of_cell_values(dataset, cc, category_list, c, cell_ids):
    input_ages = input_age_range(cc, category_list)
    if not nests_nicely(c, input_ages):
        return 0
    total = 0
    for cell_id in cell_ids:
        if remove_classification_number(c) == "resident_age":
            output_ages = age_band_text_to_numbers(all_classifications[c]['categories_map'][str(cell_id)])
            if output_ages[0] < input_ages[0] or output_ages[1] > input_ages[1]:
                continue
        datum_key = make_datum_key(cc, category_list, c, cell_id)
        total += dataset['data'][datum_key]
    return total


def generate_one_dataset(data, total_pops_data, cc, category_list):
    result = {}
    for dataset in data:
        c = dataset['c']
        if dataset['data']['blocked']:
            result[c] = "blocked"
            continue
        output_categories = output_classification_details_dict[c]['categories']
        result[c] = {"count": [], "percent": []}
        if len(cc) > 0:
            result["total_pop"] = total_pops_data[make_datum_key_for_pop_totals(cc, category_list)]
        overall_total = 0
        for cat in output_categories:
            overall_total += sum_of_cell_values(dataset, cc, category_list, c, cat['cells'])
        for cat in output_categories:
            cat_total = sum_of_cell_values(dataset, cc, category_list, c, cat['cells'])
            result[c]["count"].append(cat_total)
            if overall_total == 0:
                result[c]["percent"].append(None)
            else:
                result[c]["percent"].append(round_fraction(cat_total * 100, overall_total, 1))
    return result


def process_data(data, total_pops_data, cc):
    if len(cc) == 0:
        result = generate_one_dataset(data, None, cc, [])
        os.makedirs('generated/0var_percent', exist_ok=True)
        with open('generated/0var_percent/data.json', 'w') as f:
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
                dataset = generate_one_dataset(data, total_pops_data, cc, (*category_list, last_var_category))
                result[last_var_category['id']] = dataset
            with open(generate_outfile_path(cc, category_list, 'generated/{}var_percent/{}', '.json'), 'w') as f:
                json.dump(result, f)


def data_to_lookup(data):
    if data["blocked_areas"] != 0:
        return {'blocked': True}

    lookup = {'blocked': False}
    for obs in data['observations']:
        dimensions = []
        for dim in obs['dimensions']:
            if dim['dimension_id'] != 'nat':   # ignore the geo dimension
                dimensions.append((dim['dimension_id'], dim['option_id']))
        lookup[frozenset(dimensions)] = obs['observation']

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


def data_from_downloaded_file(filename):
    with gzip.open(filename, 'r') as f:
        json_bytes = f.read()
    return data_to_lookup(json.loads(json_bytes.decode('utf-8')))


for num_vars in range(0, max_var_selections + 1):
    input_classification_combinations = get_input_classification_combinations(input_classifications, num_vars)
    for i, cc in enumerate(input_classification_combinations):
        data = []
        total_pops_data = None
        for j, c in enumerate(output_classifications):
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
            data.append({
                "c": c,
                "data": data_from_downloaded_file(compressed_file_path)
            })
        if num_vars > 0:
            # We can get the exact total pop for the categories selected in the web-app.
            total_pops_compressed_file_path = 'downloaded/{}var/{}.json.gz'.format(
                num_vars, "-".join(cc)
            )
            total_pops_data = data_from_downloaded_file(total_pops_compressed_file_path)
        process_data(data, total_pops_data, cc)
