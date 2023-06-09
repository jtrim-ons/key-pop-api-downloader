import itertools
import math
import re
import unittest


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


class Tests(unittest.TestCase):
    def test_age_band_text_to_numbers(self):
        self.assertEqual(age_band_text_to_numbers("Aged 2 years and under"), [0, 2])
        self.assertEqual(age_band_text_to_numbers("Aged 10 to 14 years"), [10, 14])
        self.assertEqual(age_band_text_to_numbers("Aged 15 years"), [15, 15])
        self.assertEqual(age_band_text_to_numbers("Aged 85 years and over"), [85, 999])

    def test_age_band_text_to_numbers_unrecognised_pattern(self):
        with self.assertRaises(ValueError):
            age_band_text_to_numbers("Aged 45 to 49")

    def test_remove_classification_number(self):
        for c in ['resident_age_4b', 'resident_age_23a', 'resident_age']:
            self.assertEqual(remove_classification_number(c), 'resident_age')
            self.assertEqual(remove_classification_number(c), 'resident_age')
            self.assertEqual(remove_classification_number(c), 'resident_age')

    def test_get_input_classification_combinations(self):
        classifications = [
            'resident_age_4a', 'resident_age_23a', 'sex'
        ]
        combos0 = get_input_classification_combinations(classifications, 0)
        self.assertEqual(len(combos0), 1)
        self.assertEqual(len(combos0[0]), 0)
        combos1 = get_input_classification_combinations(classifications, 1)
        self.assertEqual(len(combos1), 3)
        self.assertEqual(all(len(combo) == 1 for combo in combos1), True)
        combos2 = get_input_classification_combinations(classifications, 2)
        self.assertEqual(len(combos2), 2)
        self.assertEqual(all(len(combo) == 2 for combo in combos2), True)
        combos3 = get_input_classification_combinations(classifications, 3)
        self.assertEqual(len(combos3), 0)

    def rough_round(self, numerator, denominator, digits):
        z = (numerator / denominator) * 10 ** digits
        return math.floor(z + 0.500000001) / 10 ** digits

    def test_round_fraction(self):
        for inputs in [
            [-1, 1, 1],  # negative numerator
            [1, -1, 1],  # negative denominator
            [1, 1, -1],  # negative digits
            [1, 0, 1],   # division by zero
            [1., 1, 1],  # non-integer
            [1, 1., 1],  # non-integer
            [1, 1, 1.]   # non-integer
        ]:
            with self.assertRaises(ValueError):
                round_fraction(*inputs)
        for p in range(7):
            self.assertEqual(round_fraction(p, 14, 0), 0)
        for p in range(7, 21):
            self.assertEqual(round_fraction(p, 14, 0), 1)
        self.assertEqual(round_fraction(6, 14, 1), 0.4)
        self.assertEqual(round_fraction(7, 14, 1), 0.5)
        self.assertEqual(round_fraction(5554, 100, 1), 55.5)
        self.assertEqual(round_fraction(5555, 100, 1), 55.6)
        self.assertEqual(round_fraction(55554, 1000, 2), 55.55)
        self.assertEqual(round_fraction(55555, 1000, 2), 55.56)

    def test_round_fraction_extra_cases(self):
        for numerator in range(501):
            for denominator in range(1, 201):
                for digits in range(4):
                    self.assertEqual(
                        round_fraction(numerator, denominator, digits),
                        self.rough_round(numerator, denominator, digits)
                    )


if __name__ == '__main__':
    unittest.main()
