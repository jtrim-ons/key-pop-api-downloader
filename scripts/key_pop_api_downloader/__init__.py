import itertools
import re


def remove_classification_number(c):
    return re.sub(r'_[0-9]{1,3}[a-z]$', '', c)


def get_input_classification_combinations(input_classifications, num_vars):
    result = []
    for cc in itertools.combinations(input_classifications, num_vars):
        classification_families = [remove_classification_number(c) for c in cc]
        if len(classification_families) == len(set(classification_families)):
            print('+', cc)
            result.append(cc)
        else:
            print('-', cc)
    return result
