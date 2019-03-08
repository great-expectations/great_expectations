import json
import numpy as np
import unittest

import great_expectations as ge


class TestUtilMethods(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestUtilMethods, self).__init__(*args, **kwargs)
        self.D = ge.read_csv(
            './tests/test_sets/distributional_expectations_data_base.csv')

        with open('./tests/test_sets/test_partitions.json', 'r') as file:
            self.test_partitions = json.loads(file.read())

    def test_DotDict(self):
        D = ge.util.DotDict({
            'x': [1, 2, 4],
            'y': [1, 2, 5],
            'z': ['hello', 'jello', 'mello'],
        })
        self.assertEqual(D.x[0], D.y[0])
        self.assertNotEqual(D.x[0], D.z[0])

    def test_continuous_partition_data_error(self):
        with self.assertRaises(ValueError):
            test_partition = ge.dataset.util.continuous_partition_data(
                self.D['norm_0_1'], bins=-1)
            self.assertFalse(
                ge.dataset.util.is_valid_continuous_partition_object(test_partition))
            test_partition = ge.dataset.util.continuous_partition_data(
                self.D['norm_0_1'], n_bins=-1)
            self.assertFalse(
                ge.dataset.util.is_valid_continuous_partition_object(test_partition))

    def test_partition_data_norm_0_1(self):
        test_partition = ge.dataset.util.continuous_partition_data(
            self.D.norm_0_1)
        for key, val in self.test_partitions['norm_0_1_auto'].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_partition_data_bimodal(self):
        test_partition = ge.dataset.util.continuous_partition_data(
            self.D.bimodal)
        for key, val in self.test_partitions['bimodal_auto'].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_kde_partition_data_norm_0_1(self):
        test_partition = ge.dataset.util.kde_partition_data(self.D.norm_0_1)
        for key, val in self.test_partitions['norm_0_1_kde'].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_kde_partition_data_bimodal(self):
        test_partition = ge.dataset.util.kde_partition_data(self.D.bimodal)
        for key, val in self.test_partitions['bimodal_kde'].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_categorical_data_fixed(self):
        test_partition = ge.dataset.util.categorical_partition_data(
            self.D.categorical_fixed)
        for k in self.test_partitions['categorical_fixed']['values']:
            # Iterate over each categorical value and check that the weights equal those computed originally.
            self.assertEqual(
                self.test_partitions['categorical_fixed']['weights'][self.test_partitions['categorical_fixed']['values'].index(
                    k)],
                test_partition['weights'][test_partition['values'].index(k)])

    def test_categorical_data_na(self):
        df = ge.dataset.PandasDataset({
            'my_column': ["A", "B", "A", "B", None]
        })
        partition = ge.dataset.util.categorical_partition_data(df['my_column'])
        self.assertTrue(
            ge.dataset.util.is_valid_categorical_partition_object(partition))
        self.assertTrue(len(partition['values']) == 2)

    def test_is_valid_partition_object_simple(self):
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(
            ge.dataset.util.continuous_partition_data(self.D['norm_0_1'])))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(
            ge.dataset.util.continuous_partition_data(self.D['bimodal'])))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(
            ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], bins='auto')))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(
            ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], bins='uniform', n_bins=10)))

    def test_generated_partition_objects(self):
        for partition_name, partition_object in self.test_partitions.items():
            result = ge.dataset.util.is_valid_partition_object(
                partition_object)
            if not result:
                print("Partition object " + partition_name + " is invalid.")
            self.assertTrue(result)

    def test_is_valid_partition_object_fails_length(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object(
            {'bins': [0, 1], 'weights': [0, 1, 2]}))

    def test_is_valid_partition_object_fails_weights(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object(
            {'bins': [0, 1, 2], 'weights': [0.5, 0.6]}))

    def test_is_valid_partition_object_fails_structure(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object(
            {'weights': [0.5, 0.5]}))
        self.assertFalse(
            ge.dataset.util.is_valid_partition_object({'bins': [0, 1, 2]}))

    def test_validate_distribution_parameters(self):
        D = ge.read_csv(
            './tests/test_sets/fixed_distributional_test_dataset.csv')

        # ------ p_value ------
        with self.assertRaises(ValueError):
            # p_value is 0
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=[
                                                                                              0, 1],
                                                                                          p_value=0)
        with self.assertRaises(ValueError):
            # p_value negative
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=[
                                                                                              0, 1],
                                                                                          p_value=-0.1)
        with self.assertRaises(ValueError):
            P_value = 1
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=[
                                                                                              0, 1],
                                                                                          p_value=1)

        with self.assertRaises(ValueError):
            # p_value greater than 1
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=[
                                                                                              0, 1],
                                                                                          p_value=1.1)
        with self.assertRaises(ValueError):
            # params is none
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=None)

        # ---- std_dev ------
        with self.assertRaises(ValueError):
            # std_dev is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params={
                                                                                              'mean': 0,
                                                                                              'std_dev': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # std_dev is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params={
                                                                                              'mean': 0,
                                                                                              'std_dev': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # std_dev is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm',
                                                                                          distribution='norm',
                                                                                          params=[0, 0])
        with self.assertRaises(ValueError):
            # std_dev is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm',
                                                                                          distribution='norm',
                                                                                          params=[0, -1])

        # ------- beta ------
        with self.assertRaises(ValueError):
            # beta, alpha is 0, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'alpha': 0,
                                                                                              'beta': 0.1
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, alpha is negative, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'alpha': -1,
                                                                                              'beta': 0.1
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, beta is 0, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'alpha': 0.1,
                                                                                              'beta': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, beta is negative, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'alpha': 0,
                                                                                              'beta': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, alpha is 0, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[0, 0.1])
        with self.assertRaises(ValueError):
            # beta, alpha is negative, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[-1, 0.1])
        with self.assertRaises(ValueError):
            # beta, beta is 0, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[0.1, 0])
        with self.assertRaises(ValueError):
            # beta, beta is negative, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[0.1, -1])

        with self.assertRaises(ValueError):
            # beta, missing alpha, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'beta': 0.1
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, missing beta, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params={
                                                                                              'alpha': 0.1
                                                                                          })
        with self.assertRaises(ValueError):
            # beta, missing beta, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[1])
        with self.assertRaises(ValueError):
            # beta, missing beta, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('beta',
                                                                                          distribution='beta',
                                                                                          params=[1, 1, 1, 1, 1])

        # ------ Gamma -------
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params={
                                                                                              'alpha': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # gamma, alpha is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params={
                                                                                              'alpha': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params={
                                                                                              'alpha': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # gamma, alpha is missing, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params={
                                                                                          })
        with self.assertRaises(ValueError):
            # gamma, alpha is missing, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params=[])
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params=[0])
        with self.assertRaises(ValueError):
            # gamma, alpha is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params=[-1])
        with self.assertRaises(ValueError):
            # gamma, too many arguments, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('gamma',
                                                                                          distribution='gamma',
                                                                                          params=[1, 1, 1, 1])

        # ----- chi2 --------
        with self.assertRaises(ValueError):
            # chi2, df is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params={
                                                                                              'df': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # chi2, df is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params={
                                                                                              'df': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # chi2, df is missing, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params={
                                                                                          })
        with self.assertRaises(ValueError):
            # chi2, df is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params=[0])
        with self.assertRaises(ValueError):
            # chi2, df is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params=[-1])
        with self.assertRaises(ValueError):
            # chi2, df is missing, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params=[])
        with self.assertRaises(ValueError):
            # chi2, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('chi2',
                                                                                          distribution='chi2',
                                                                                          params=[1, 1, 1, 5])
        # ----- norm ------
        with self.assertRaises(ValueError):
            # norm, too many arguments, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('norm', distribution='norm',
                                                                                          params=[0, 1, 500])

        # ----- uniform -----
        with self.assertRaises(ValueError):
            # uniform, scale is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('uniform', distribution='uniform',
                                                                                          params=[0, 0])
        with self.assertRaises(ValueError):
            # uniform, scale is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('uniform', distribution='uniform',
                                                                                          params=[0, -1])
        with self.assertRaises(ValueError):
            # uniform, scale is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('uniform', distribution='uniform',
                                                                                          params={
                                                                                              'loc': 0,
                                                                                              'scale': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # uniform, scale is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('uniform', distribution='uniform',
                                                                                          params={
                                                                                              'loc': 0,
                                                                                              'scale': 0
                                                                                          })

        with self.assertRaises(ValueError):
            # uniform, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('uniform', distribution='uniform',
                                                                                          params=[0, 1, 500])

        # --- expon ---
        with self.assertRaises(ValueError):
            # expon, scale is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='expon',
                                                                                          params=[0, 0])
        with self.assertRaises(ValueError):
            # expon, scale is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='expon',
                                                                                          params=[0, -1])
        with self.assertRaises(ValueError):
            # expon, scale is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='expon',
                                                                                          params={
                                                                                              'loc': 0,
                                                                                              'scale': 0
                                                                                          })
        with self.assertRaises(ValueError):
            # expon, scale is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='expon',
                                                                                          params={
                                                                                              'loc': 0,
                                                                                              'scale': -1
                                                                                          })
        with self.assertRaises(ValueError):
            # expon, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='expon',
                                                                                          params=[0, 1, 500])

        # --- misc ---
        with self.assertRaises(AttributeError):
            # non-supported distribution
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than('exponential', distribution='fakedistribution',
                                                                                          params=[0, 1])

    def test_infer_distribution_parameters(self):
        D = ge.read_csv(
            './tests/test_sets/fixed_distributional_test_dataset.csv')

        with self.assertRaises(TypeError):
            ge.dataset.util.infer_distribution_parameters(data=D.norm,
                                                          distribution='norm',
                                                          params=['wrong_param_format'])
        t = ge.dataset.util.infer_distribution_parameters(data=D.norm_std,
                                                          distribution='norm',
                                                          params=None)
        self.assertEqual(t['mean'], D.norm_std.mean())
        self.assertEqual(t['std_dev'], D.norm_std.std())
        self.assertEqual(t['loc'], 0)
        self.assertEqual(t['scale'], 1)

        # beta
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.beta, distribution='beta')
        self.assertEqual(t['alpha'], (t['mean'] ** 2) * (
                        ((1 - t['mean']) / t['std_dev'] ** 2) - (1 / t['mean'])), "beta dist, alpha infer")
        self.assertEqual(t['beta'], t['alpha'] *
                         ((1 / t['mean']) - 1), "beta dist, beta infer")

        # gamma
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.gamma, distribution='gamma')
        self.assertEqual(t['alpha'], D.gamma.mean())

        # uniform distributions
        t = ge.dataset.util.infer_distribution_parameters(data=D.uniform,
                                                          distribution='uniform')
        self.assertEqual(t['min'], min(D.uniform), "uniform, min infer")
        self.assertEqual(t['max'], max(D.uniform) -
                         min(D.uniform), "uniform, max infer")

        uni_loc = 5
        uni_scale = 10
        t = ge.dataset.util.infer_distribution_parameters(data=D.uniform,
                                                          distribution='uniform',
                                                          params={
                                                              'loc': uni_loc,
                                                              'scale': uni_scale
                                                          })
        self.assertEqual(t['min'], uni_loc, "uniform, min infer")
        self.assertEqual(t['max'], uni_scale, "uniform, max infer")

        # expon distribution
        with self.assertRaises(AttributeError):
            ge.dataset.util.infer_distribution_parameters(data=D.norm,
                                                          distribution='fakedistribution')

        # chi2
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.chi2, distribution='chi2')
        self.assertEqual(t['df'], D.chi2.mean())

    def test_create_multiple_expectations(self):
        D = ge.dataset.PandasDataset({
            'x': [1, 2, 3, 4, 5, 6],
            'y': [0, 2, 4, 6, 8, 10],
            'z': ['hi', 'hello', 'hey', 'howdy', 'hola', 'holy smokes'],
            'zz': ['a', 'b', 'c', 'hi', 'howdy', 'hola']
        })

        # Test kwarg
        results = ge.dataset.util.create_multiple_expectations(D,
                                                               ['x', 'y'],
                                                               'expect_column_values_to_be_in_set',
                                                               value_set=[1, 2, 3, 4, 5, 6])
        self.assertTrue(results[0]['success'])
        self.assertFalse(results[1]['success'])

        # Test positional argument
        results = ge.dataset.util.create_multiple_expectations(D,
                                                               ['x', 'y'],
                                                               'expect_column_values_to_be_in_set',
                                                               [1, 2, 3, 4, 5, 6])
        self.assertTrue(results[0]['success'])
        self.assertFalse(results[1]['success'])

        results = ge.dataset.util.create_multiple_expectations(D,
                                                               ['z', 'zz'],
                                                               'expect_column_values_to_match_regex',
                                                               'h')
        self.assertTrue(results[0]['success'])
        self.assertFalse(results[1]['success'])

        # Non-argumentative expectation
        results = ge.dataset.util.create_multiple_expectations(D,
                                                               ['z', 'zz'],
                                                               'expect_column_values_to_not_be_null')
        self.assertTrue(results[0]['success'])
        self.assertTrue(results[1]['success'])

        # Key error when non-existant column is called
        with self.assertRaises(KeyError):
            ge.dataset.util.create_multiple_expectations(D,
                                                         ['p'],
                                                         'expect_column_values_to_be_in_set',
                                                         ['hi'])
        # Attribute error when non-existant expectation is called
        with self.assertRaises(AttributeError):
            ge.dataset.util.create_multiple_expectations(D,
                                                         ['z'],
                                                         'expect_column_values_to_be_fake_news')


if __name__ == "__main__":
    unittest.main()
