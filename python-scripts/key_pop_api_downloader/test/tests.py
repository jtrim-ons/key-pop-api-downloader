import key_pop_api_downloader as pgp
import unittest
import math


class Tests(unittest.TestCase):
    def test_age_band_text_to_numbers(self):
        self.assertEqual(pgp.age_band_text_to_numbers("Aged 2 years and under"), [0, 2])
        self.assertEqual(pgp.age_band_text_to_numbers("Aged 10 to 14 years"), [10, 14])
        self.assertEqual(pgp.age_band_text_to_numbers("Aged 15 years"), [15, 15])
        self.assertEqual(pgp.age_band_text_to_numbers("Aged 85 years and over"), [85, 999])

    def test_age_band_text_to_numbers_unrecognised_pattern(self):
        with self.assertRaises(ValueError):
            pgp.age_band_text_to_numbers("Aged 45 to 49")

    def test_remove_classification_number(self):
        for c in ['resident_age_4b', 'resident_age_23a', 'resident_age']:
            self.assertEqual(pgp.remove_classification_number(c), 'resident_age')
            self.assertEqual(pgp.remove_classification_number(c), 'resident_age')
            self.assertEqual(pgp.remove_classification_number(c), 'resident_age')

    def test_get_input_classification_combinations(self):
        classifications = [
            'resident_age_4a', 'resident_age_23a', 'sex'
        ]
        combos0 = pgp.get_input_classification_combinations(classifications, 0)
        self.assertEqual(len(combos0), 1)
        self.assertEqual(len(combos0[0]), 0)
        combos1 = pgp.get_input_classification_combinations(classifications, 1)
        self.assertEqual(len(combos1), 3)
        self.assertEqual(all(len(combo) == 1 for combo in combos1), True)
        combos2 = pgp.get_input_classification_combinations(classifications, 2)
        self.assertEqual(len(combos2), 2)
        self.assertEqual(all(len(combo) == 2 for combo in combos2), True)
        combos3 = pgp.get_input_classification_combinations(classifications, 3)
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
                pgp.round_fraction(*inputs)
        for p in range(7):
            self.assertEqual(pgp.round_fraction(p, 14, 0), 0)
        for p in range(7, 21):
            self.assertEqual(pgp.round_fraction(p, 14, 0), 1)
        self.assertEqual(pgp.round_fraction(6, 14, 1), 0.4)
        self.assertEqual(pgp.round_fraction(7, 14, 1), 0.5)
        self.assertEqual(pgp.round_fraction(5554, 100, 1), 55.5)
        self.assertEqual(pgp.round_fraction(5555, 100, 1), 55.6)
        self.assertEqual(pgp.round_fraction(55554, 1000, 2), 55.55)
        self.assertEqual(pgp.round_fraction(55555, 1000, 2), 55.56)

    def test_round_fraction_extra_cases(self):
        for numerator in range(501):
            for denominator in range(1, 201):
                for digits in range(4):
                    self.assertEqual(
                        pgp.round_fraction(numerator, denominator, digits),
                        self.rough_round(numerator, denominator, digits)
                    )

    def test_get_config(self):
        self.assertEqual(
            pgp.get_config("../input-txt-files/config.json", "test_key"),
            "test_value"
        )

    def test_generate_outfile_path(self):
        self.assertEqual(
            pgp.generate_outfile_path(
                ['resident_age_4b', 'sex'],
                [{"id": "1"}],
                'generated/{}var-by-ltla/{}',
                '_by_geog.json'
            ),
            "generated/2var-by-ltla/resident_age_4b-1/sex_by_geog.json"
        )
        with self.assertRaises(ValueError):
            pgp.generate_outfile_path(
                [],
                [],
                'generated/{}var-by-ltla/{}',
                '_by_geog.json'
            )
        with self.assertRaises(ValueError):
            pgp.generate_outfile_path(
                ['resident_age_4b', 'sex'],
                [{"id": "1"}, {"id": "2"}],
                'generated/{}var-by-ltla/{}',
                '_by_geog.json'
            )


if __name__ == '__main__':
    unittest.main()
