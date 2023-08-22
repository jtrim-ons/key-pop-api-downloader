"""Generate national-level files from the files already downloaded from the API."""

import itertools
import json
import os

import key_pop_api_downloader as pgp

all_classifications = pgp.load_all_classifications()
input_classifications, output_classifications = pgp.load_input_and_output_classification_codes()
output_classification_details_dict = pgp.load_output_classification_details(all_classifications)


def is_resident_age(c):
    return pgp.remove_classification_number(c) == "resident_age"


def make_datum_key(cc, category_list, c, cell_id):
    return frozenset(
        list(
            (classification_id, opt['id'])
            for classification_id, opt in zip(cc, category_list)
            if not is_resident_age(c) or not is_resident_age(classification_id)
        ) + [(c, str(cell_id))]
    )


def make_datum_key_for_pop_totals(cc, category_list):
    return frozenset([
        (classification_id, opt['id'])
        for classification_id, opt in zip(cc, category_list)
    ])


def sum_of_cell_values(dataset, cc, category_list, c, cell_ids):
    total = 0
    for cell_id in cell_ids:
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
                result[c]["percent"].append(pgp.round_fraction(cat_total * 100, overall_total, 1))
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
            with open(pgp.generate_outfile_path(cc, category_list, 'generated/{}var_percent/{}', '.json'), 'w') as f:
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
    # an output classification.  If the output classification is for resident age,
    # then any resident_age input classifications are deleted.
    if is_resident_age(c):
        classifications = [c_ for c_ in list(cc) if not is_resident_age(c_)] + [c]
    else:
        classifications = list(cc) + [c]
    return len(classifications), "-".join(classifications)


def generate_files(num_vars, unblocked_combination_counts):
    icc = pgp.get_input_classification_combinations(input_classifications, num_vars)
    for i, cc in enumerate(icc):
        data = []
        total_pops_data = None
        for c in output_classifications:
            if not is_resident_age(c) and pgp.remove_classification_number(c) in [
                    pgp.remove_classification_number(c_) for c_ in cc
                ]:
                # The API won't give data for two versions of the same variable.
                # Since we haven't downloaded it, we can't use it to generate files :-)
                # The exception is for resident_age, which is a special case where
                # we just use the data for 18 categories.
                continue
            c_str_len, c_str = make_c_str(cc, c)
            print("{} var: Processing {} of {} ({})".format(num_vars, i+1, len(icc), c_str))
            file_path = 'downloaded/{}var/{}.json.gz'.format(c_str_len-1, c_str)
            data.append({
                    "c": c,
                    "data": data_to_lookup(pgp.read_json_gz(file_path))
                })
        if num_vars > 0:
            # We can get the exact total pop for the categories selected in the web-app.
            total_pops_file_path = 'downloaded/{}var/{}.json.gz'.format(num_vars, "-".join(cc))
            total_pops_data = data_to_lookup(pgp.read_json_gz(total_pops_file_path))
        process_data(data, total_pops_data, cc)
        unblocked_combination_counts[','.join(cc)] = sum(not d['data']['blocked'] for d in data)


def main():
    unblocked_combination_counts = {}

    max_var_selections = pgp.get_config("input-txt-files/config.json", "max_var_selections")
    for num_vars in range(0, max_var_selections + 1):
        generate_files(num_vars, unblocked_combination_counts)

    with open('generated/unblocked-combination-counts.json', 'w') as f:
        json.dump(unblocked_combination_counts, f)


if __name__ == "__main__":
    main()