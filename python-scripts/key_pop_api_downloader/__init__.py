import gzip
import itertools
import json
import os
import re


def load_output_classification_details(all_classifications):
    with open('input-txt-files/output-classifications-with-details.json', 'r') as f:
        output_classification_details = json.load(f)
    for item in output_classification_details:
        if item["categories"] is None:
            item["categories"] = [
                {"label": cat["label"], "cells": [int(cat["id"])]}
                for cat in all_classifications[item["code"]]["categories"]
                if cat["id"] != "-8"
            ]
    return {var["code"]: var for var in output_classification_details}


def load_all_classifications():
    with open('generated/all-classifications.json', 'r') as f:
        all_classifications = json.load(f)
    for c in all_classifications:
        all_classifications[c]["categories_map"] = {
            cat['id']: cat['label'] for cat in all_classifications[c]['categories']
        }
    return all_classifications


def load_input_and_output_classification_codes():
    with open('input-txt-files/input-classifications.txt', 'r') as f:
        input_classifications = f.read().splitlines()
    input_classifications.sort()

    with open('input-txt-files/output-classifications.txt', 'r') as f:
        output_classifications = f.read().splitlines()
    output_classifications.sort()

    return input_classifications, output_classifications


def read_json_gz(filename):
    with gzip.open(filename, 'r') as f:
        json_bytes = f.read()
    return json.loads(json_bytes.decode('utf-8'))


def generate_outfile_path(cc, category_list, directory_pattern, suffix):
    if len(cc) == 0:
        raise ValueError("cc should have at least one element.")
    if len(cc) != len(category_list) + 1:
        raise ValueError("cc should have one more element than category_list")

    directory_names = [cat_id + '-' + opt['id'] for cat_id, opt in zip(cc, category_list)]
    directory = directory_pattern.format(len(cc), '/'.join(directory_names))
    os.makedirs(directory, exist_ok=True)
    return directory + '/' + cc[-1] + suffix


def get_config(filename, key):
    with open(filename, "r") as f:
        config = json.load(f)
    return config[key]


def remove_classification_number(c):
    return re.sub(r'_[0-9]{1,3}[a-z]$', '', c)


def get_input_classification_combinations(input_classifications, num_vars):
    result = []
    for cc in itertools.combinations(input_classifications, num_vars):
        classification_families = [remove_classification_number(c) for c in cc]
        if len(classification_families) == len(set(classification_families)):
            result.append(cc)
    return result


def age_band_text_to_numbers(age_band_text):
    if re.fullmatch(r'Aged [0-9]+ years and under', age_band_text):
        age = int(re.findall(r'[0-9]+', age_band_text)[0])
        return [0, age]
    if re.fullmatch(r'Aged [0-9]+ to [0-9]+ years', age_band_text):
        return [int(s) for s in re.findall(r'[0-9]+', age_band_text)]
    if re.fullmatch(r'Aged [0-9]+ years', age_band_text):
        age = int(re.findall(r'[0-9]+', age_band_text)[0])
        return [age, age]
    if re.fullmatch(r'Aged [0-9]+ years and over', age_band_text):
        age = int(re.findall(r'[0-9]+', age_band_text)[0])
        return [age, 999]
    raise ValueError('Unrecognised age band pattern: ' + age_band_text)


def round_fraction(numerator, denominator, digits=0):
    """ Round a positive fraction to a given number of decimal places,
        using 'round half up'.

        Parameters:
          numerator
          denominator
          digits      the required number of decimal places

        The return value is an integer if digits=0; otherwise, it is a float.
        In the latter case, the result may be inaccurate by a tiny amount
        because of floating-point imprecision.

        Why does it work?  Assuming rounding to zero decimal places:
             round(p/q)
           = floor(p/q + 1/2)
           = floor(2p/2q + q/2q)
           = floor((2p + q) / 2q)
           = (2p + q) // 2q       (where // is integer division)
    """
    if not isinstance(numerator, int):
        raise ValueError(f"Numerator must be an int ({numerator})")
    if not isinstance(denominator, int):
        raise ValueError(f"Denominator must be an int ({denominator})")
    if not isinstance(digits, int):
        raise ValueError(f"Numerator must be an int ({digits})")
    if numerator < 0:
        raise ValueError("Numerator must not be negative")
    if denominator < 1:
        raise ValueError("Denominator must be at least 1")
    if digits < 0:
        raise ValueError("Digits must not be negative")
    if digits == 0:
        return (2 * numerator + denominator) // (2 * denominator)
    else:
        m = pow(10, digits)
        return ((2 * m * numerator + denominator) // (2 * denominator)) / m
