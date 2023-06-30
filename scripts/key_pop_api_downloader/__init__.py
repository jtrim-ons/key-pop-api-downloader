import itertools
import re
import unittest


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


class Tests(unittest.TestCase):
    def test_age_band_text_to_numbers(self):
        self.assertEqual(age_band_text_to_numbers("Aged 2 years and under"), [0, 2])
        self.assertEqual(age_band_text_to_numbers("Aged 10 to 14 years"), [10, 14])
        self.assertEqual(age_band_text_to_numbers("Aged 15 years"), [15, 15])
        self.assertEqual(age_band_text_to_numbers("Aged 85 years and over"), [85, 999])

    def test_age_band_text_to_numbers_unrecognised_pattern(self):
        with self.assertRaises(ValueError):
            age_band_text_to_numbers("Aged 45 to 49")


if __name__ == '__main__':
    unittest.main()
