import unittest
import json
import numpy as np
import sys

import great_expectations as ge
from ..test_utils import assertDeepAlmostEqual
from great_expectations.dataset.util import is_valid_continuous_partition_object


class TestDistributionalExpectations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDistributionalExpectations, self).__init__(*args, **kwargs)
        self.D = ge.read_csv(
            './tests/test_sets/distributional_expectations_data_test.csv')

        with open('./tests/test_sets/test_partitions.json', 'r') as infile:
            self.test_partitions = json.loads(infile.read())

    def test_expect_column_chisquare_test_p_value_to_be_greater_than(self):
        T = [
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed'],
                    'p': 0.05
                },
                'out': {'success': True, 'observed_value': 1.}
            },
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed_alternate'],
                    'p': 0.05
                },
                'out': {'success': False, 'observed_value': 5.1397782097623862e-53}
            },
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed_alternate'],
                    'p': 0.05, 'result_format': 'SUMMARY'
                },
                'out': {'success': False, 'observed_value': 5.1397782097623862e-53,
                        'details': {
                            'observed_partition': {
                                'values': [u'A', u'B', u'C'],
                                'weights': [540, 320, 140]
                            },
                            'expected_partition': {
                                'values': [u'A', u'B', u'C'],
                                'weights': [333.3333333333333, 333.3333333333333, 333.3333333333333]
                            }
                        }
                        }
            }
        ]
        for t in T:
            out = self.D.expect_column_chisquare_test_p_value_to_be_greater_than(
                *t['args'], **t['kwargs'])
            self.assertEqual(t['out']['success'], out['success'])
            self.assertEqual(t['out']['observed_value'],
                             out['result']['observed_value'])
            if 'result_format' in t['kwargs'] and t['kwargs']['result_format'] == 'SUMMARY':
                self.assertDictEqual(
                    t['out']['details'], out['result']['details'])

    def test_expect_column_chisquare_test_p_value_to_be_greater_than_new_categorical_val(self):
        # Note: Chisquare test with true zero expected could be treated subtly. Here, we tolerate a warning from stats.
        categorical_list = (['A'] * 25) + (['B'] * 25) + \
            (['C'] * 25) + (['D'] * 25)
        df = ge.dataset.PandasDataset({'categorical': categorical_list})

        out = df.expect_column_chisquare_test_p_value_to_be_greater_than(
            'categorical', self.test_partitions['categorical_fixed_alternate'])
        self.assertEqual(out['success'], False)

        out = df.expect_column_chisquare_test_p_value_to_be_greater_than(
            'categorical', self.test_partitions['categorical_fixed_alternate'], tail_weight_holdout=0.25)
        self.assertEqual(out['success'], True)

    def test_expect_column_chisquare_test_p_value_to_be_greater_than_missing_categorical_val(self):
        categorical_list = (['A'] * 61) + (['B'] * 39)
        df = ge.dataset.PandasDataset({'categorical': categorical_list})
        out = df.expect_column_chisquare_test_p_value_to_be_greater_than(
            'categorical', self.test_partitions['categorical_fixed'])
        self.assertEqual(out['success'], False)

    def test_expect_column_kl_divergence_to_be_less_than_discrete(self):
        T = [
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed'],
                    'threshold': 0.1
                },
                'out': {'success': True, 'observed_value': 0.}
            },
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed_alternate'],
                    'threshold': 0.1
                },
                'out': {'success': False, 'observed_value': 0.12599700286677529}
            },
            {
                'args': ['categorical_fixed'],
                'kwargs': {
                    'partition_object': self.test_partitions['categorical_fixed_alternate'],
                    'threshold': 0.1, 'result_format': 'SUMMARY'
                },
                'out': {'success': False, 'observed_value': 0.12599700286677529,
                        'details': {
                            'observed_partition': {
                                'weights': [0.54, 0.32, 0.14],
                                'values': [u'A', u'B', u'C']},
                            'expected_partition': {
                                'weights': [0.3333333333333333, 0.3333333333333333, 0.3333333333333333],
                                'values': [u'A', u'B', u'C']
                            }
                        }
                        }
            }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_to_be_less_than(
                *t['args'], **t['kwargs'])
            self.assertTrue(np.allclose(out['success'], t['out']['success']))
            self.assertTrue(np.allclose(
                out['result']['observed_value'], t['out']['observed_value']))
            if 'result_format' in t['kwargs'] and t['kwargs']['result_format'] == 'SUMMARY':
                self.assertDictEqual(
                    out['result']['details'], t['out']['details'])

    def test_expect_column_kl_divergence_to_be_less_than_discrete_holdout(self):
        df = ge.dataset.PandasDataset({'a': ['a', 'a', 'b', 'c']})
        out = df.expect_column_kl_divergence_to_be_less_than('a',
                                                             {'values': ['a', 'b'], 'weights': [
                                                                 0.6, 0.4]},
                                                             threshold=0.1,
                                                             tail_weight_holdout=0.1)
        self.assertEqual(out['success'], True)
        self.assertTrue(np.allclose(
            out['result']['observed_value'], [0.099431384003497381]))

        out = df.expect_column_kl_divergence_to_be_less_than('a',
                                                             {'values': ['a', 'b'], 'weights': [
                                                                 0.6, 0.4]},
                                                             threshold=0.1,
                                                             tail_weight_holdout=0.05)
        self.assertEqual(out['success'], False)
        self.assertTrue(np.isclose(
            out['result']['observed_value'], [0.23216776319077681]))

        out = df.expect_column_kl_divergence_to_be_less_than('a',
                                                             {'values': ['a', 'b'], 'weights': [
                                                                 0.6, 0.4]},
                                                             threshold=0.1)
        self.assertEqual(out['success'], False)
        self.assertEqual(out['result']['observed_value'], None)

    def test_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(self):
        T = [
            {
                'args': ['norm_0_1'],
                'kwargs': {'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05},
                'out': {'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_0_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                'out':{'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_0_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile'], "p": 0.05},
                'out':{'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_0_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde'], "p": 0.05},
                'out':{'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_1_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_1_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_1_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile'], "p": 0.05},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['norm_1_1'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde'], "p": 0.05},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['bimodal'],
                'kwargs':{'partition_object': self.test_partitions['bimodal_auto'], "p": 0.05},
                'out':{'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['bimodal'],
                'kwargs':{'partition_object': self.test_partitions['bimodal_kde'], "p": 0.05},
                'out':{'success': True, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['bimodal'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05,
                          'include_config': True},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['bimodal'],
                'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                'out':{'success': False, 'observed_value': "RANDOMIZED"}
            },
            {
                'args': ['bimodal'],
                'kwargs': {'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05, 'result_format': 'SUMMARY'},
                'out': {'success': False, 'observed_value': "RANDOMIZED",
                        'details': {
                            'expected_cdf': {
                                'cdf_values': [0.0, 0.001, 0.009000000000000001, 0.056, 0.184, 0.429, 0.6779999999999999, 0.8899999999999999, 0.9689999999999999, 0.9929999999999999, 0.9999999999999999],
                                'x': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364]
                            },
                            'observed_partition': {
                                'weights': [0.001, 0.006, 0.022, 0.07, 0.107, 0.146, 0.098, 0.04, 0.01, 0.0, 0.5],
                                'bins': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364, 12.8787297644972]
                            },
                            'bootstrap_samples': 1000,
                            'observed_cdf': {
                                'cdf_values': [0, 0.001, 0.007, 0.028999999999999998, 0.099, 0.20600000000000002, 0.352, 0.44999999999999996, 0.48999999999999994, 0.49999999999999994, 0.49999999999999994, 1.0],
                                'x': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364, 12.8787297644972]
                            },
                            'expected_partition': {
                                'weights': [0.001, 0.008, 0.047, 0.128, 0.245, 0.249, 0.212, 0.079, 0.024, 0.007],
                                'bins': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364]
                            },
                            'bootstrap_sample_size': 20
                        }
                        }
            }
        ]
        for t in T:
            out = self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
                *t['args'], **t['kwargs'])
            if out['success'] != t['out']['success']:
                print("Test case error:")
                print(t)
                print(out)
            self.assertEqual(out['success'], t['out']['success'])
            if 'result_format' in t['kwargs'] and t['kwargs']['result_format'] == 'SUMMARY':
                self.assertTrue(np.allclose(
                    out['result']['details']['observed_cdf']['x'], t['out']['details']['observed_cdf']['x']))
                self.assertTrue(np.allclose(out['result']['details']['observed_cdf']
                                            ['cdf_values'], t['out']['details']['observed_cdf']['cdf_values']))
                self.assertTrue(np.allclose(
                    out['result']['details']['expected_cdf']['x'], t['out']['details']['expected_cdf']['x']))
                self.assertTrue(np.allclose(out['result']['details']['expected_cdf']
                                            ['cdf_values'], t['out']['details']['expected_cdf']['cdf_values']))
                self.assertTrue(np.allclose(
                    out['result']['details']['observed_partition']['bins'], t['out']['details']['observed_partition']['bins']))
                self.assertTrue(np.allclose(out['result']['details']['observed_partition']
                                            ['weights'], t['out']['details']['observed_partition']['weights']))
                self.assertTrue(np.allclose(
                    out['result']['details']['expected_partition']['bins'], t['out']['details']['expected_partition']['bins']))
                self.assertTrue(np.allclose(out['result']['details']['expected_partition']
                                            ['weights'], t['out']['details']['expected_partition']['weights']))

    def test_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than_expanded_partitions(self):
        # Extend observed above and below expected
        out = self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than('norm_0_1', {'bins': np.linspace(-1, 1, 11), 'weights': [0.1] * 10},
                                                                                   result_format='SUMMARY')
        self.assertTrue(out['result']['details']['observed_cdf']['x'][0] < -1)
        self.assertTrue(out['result']['details']['observed_cdf']['x'][-1] > 1)
        # Extend observed below expected
        out = self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than('norm_0_1',
                                                                                   {'bins': np.linspace(-10, 1, 11), 'weights': [
                                                                                       0.1] * 10},
                                                                                   result_format='SUMMARY')
        self.assertTrue(out['result']['details']
                        ['observed_cdf']['x'][0] == -10)
        self.assertTrue(out['result']['details']['observed_cdf']['x'][-1] > 1)
        # Extend observed above expected
        out = self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than('norm_0_1',
                                                                                   {'bins': np.linspace(-1, 10, 11), 'weights': [
                                                                                       0.1] * 10},
                                                                                   result_format='SUMMARY')
        self.assertTrue(out['result']['details']['observed_cdf']['x'][0] < -1)
        self.assertTrue(out['result']['details']
                        ['observed_cdf']['x'][-1] == 10)
        # Extend expected above and below observed
        out = self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than('norm_0_1',
                                                                                   {'bins': np.linspace(-10, 10, 11), 'weights': [
                                                                                       0.1] * 10},
                                                                                   result_format='SUMMARY')
        self.assertTrue(out['result']['details']
                        ['observed_cdf']['x'][0] == -10)
        self.assertTrue(out['result']['details']
                        ['observed_cdf']['x'][-1] == 10)

    def test_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than_bad_partition(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
                'norm_0_1', {'bins': [-np.inf, 0, 1, 2, 3], 'weights': [0.25, 0.25, 0.25, 0.25]})

    def test_expect_column_kl_divergence_to_be_less_than_continuous_infinite_partition(self):
        # Manually build a partition extending to -Inf and Inf
        test_partition = self.test_partitions['norm_0_1_auto']
        test_partition['bins'] = [-np.inf] + test_partition['bins'] + [np.inf]
        scaled_weights = np.array(test_partition['weights']) * (1-0.01)
        test_partition['weights'] = [0.005] + scaled_weights.tolist() + [0.005]
        out = self.D.expect_column_kl_divergence_to_be_less_than(
            'norm_0_1', test_partition, 0.5, internal_weight_holdout=0.01)
        self.assertTrue(out['success'])

        # This should fail: tails have internal weight zero, which is highly unlikely.
        out = self.D.expect_column_kl_divergence_to_be_less_than(
            'norm_0_1', test_partition, 0.5)
        self.assertFalse(out['success'])

        # Build one-sided to infinity test partitions
        test_partition = {
            'bins': [-np.inf, 0, 1, 2, 3],
            'weights': [0.25, 0.25, 0.25, 0.25]
        }
        summary_expected_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.25, 0.25, 0.25],
            'tail_weights':[0.25,0]
        }
        summary_observed_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.25, 0.25, 0.25],
            'tail_weights':[0.25,0]
        }
        test_df = ge.dataset.PandasDataset(
            {'x': [-0.5, 0.5, 1.5, 2.5]})
        # This should succeed: our data match the partition
        out = test_df.expect_column_kl_divergence_to_be_less_than(
            'x', test_partition, 0.5, result_format='SUMMARY')
        self.assertTrue(out['success'])
        self.assertDictEqual(
            out['result']['details']['observed_partition'], summary_observed_partition)
        self.assertDictEqual(
            out['result']['details']['expected_partition'], summary_expected_partition)

        # Build one-sided to infinity test partitions
        test_partition = {
            'bins': [0, 1, 2, 3, np.inf],
            'weights': [0.25, 0.25, 0.25, 0.25]
        }
        summary_expected_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.25, 0.25, 0.25],
            'tail_weights':[0,0.25]
        }
        summary_observed_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.2, 0.2, 0.2],
            'tail_weights':[0.2,0.2]
        }
        test_df = ge.dataset.PandasDataset(
            {'x': [-0.5, 0.5, 1.5, 2.5, 3.5]})
        out = test_df.expect_column_kl_divergence_to_be_less_than(
            'x', test_partition, 0.5, result_format='SUMMARY')
        # This should fail: we expect zero weight less than 0
        self.assertFalse(out['success'])
        self.assertDictEqual(
            out['result']['details']['observed_partition'], summary_observed_partition)
        self.assertDictEqual(
            out['result']['details']['expected_partition'], summary_expected_partition)

        # Build two-sided to infinity test partition
        test_partition = {
            'bins': [-np.inf, 0, 1, 2, 3, np.inf],
            'weights': [0.1, 0.2, 0.4, 0.2, 0.1]
        }
        summary_expected_partition = {
            'bins': [ 0, 1, 2, 3],
            'weights': [0.2, 0.4, 0.2],
            'tail_weights':[0.1,0.1]
        }
        summary_observed_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.2, 0.4, 0.2],
            'tail_weights':[0.1,0.1]
        }
        test_df = ge.dataset.PandasDataset(
            {'x': [-0.5, 0.5, 0.5, 1.5, 1.5, 1.5, 1.5, 2.5, 2.5, 3.5]})
        # This should succeed: our data match the partition
        out = test_df.expect_column_kl_divergence_to_be_less_than(
            'x', test_partition, 0.5, result_format='SUMMARY')
        self.assertTrue(out['success'])
        self.assertDictEqual(
            out['result']['details']['observed_partition'], summary_observed_partition)
        self.assertDictEqual(
            out['result']['details']['expected_partition'], summary_expected_partition)

        # Tail weight holdout is not defined for partitions already extending to infinity:
        with self.assertRaises(ValueError):
            test_df.expect_column_kl_divergence_to_be_less_than(
                'x', test_partition, 0.5, tail_weight_holdout=0.01)

    def test_expect_column_kl_divergence_to_be_less_than_continuous_serialized_infinite_partition(self):
        with open('./tests/test_sets/test_partition_serialized_infinity_bins.json', 'r') as infile:
            test_partition = json.loads(infile.read())['test_partition']

        summary_expected_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.2, 0.4, 0.2],
            'tail_weights':[0.1,0.1]
        }
        summary_observed_partition = {
            'bins': [0, 1, 2, 3],
            'weights': [0.2, 0.4, 0.2],
            'tail_weights':[0.1,0.1]
        }
        test_df = ge.dataset.PandasDataset(
            {'x': [-0.5, 0.5, 0.5, 1.5, 1.5, 1.5, 1.5, 2.5, 2.5, 3.5]})
        # This should succeed: our data match the partition
        out = test_df.expect_column_kl_divergence_to_be_less_than(
            'x', test_partition, 0.5, result_format='SUMMARY')
        self.assertTrue(out['success'])
        self.assertDictEqual(
            out['result']['details']['observed_partition'], summary_observed_partition)
        self.assertDictEqual(
            out['result']['details']['expected_partition'], summary_expected_partition)

        # Confirm serialization of resulting expectations config
        expectation_config = test_df.get_expectations_config()
        found_expectation = False
        for expectation in expectation_config['expectations']:
            if 'expectation_type' in expectation and expectation['expectation_type'] == 'expect_column_kl_divergence_to_be_less_than':
                self.assertEqual(
                    json.dumps(expectation['kwargs']
                               ['partition_object']['bins']),
                    '[-Infinity, 0, 1, 2, 3, Infinity]'
                )
                found_expectation = True
        self.assertTrue(found_expectation)

    def test_expect_column_kl_divergence_to_be_less_than_continuous(self):
        T = [
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'observed_value': 'NOTTESTED'}
                },
                ## Note higher threshold example for kde
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde'],
                              "threshold": 0.3,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 1e-5,
                              "internal_weight_holdout": 1e-5},
                    'out':{'success':False, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['bimodal_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'observed_value': 'NOTTESTED'}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'observed_value': "NOTTESTED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'observed_value': "NOTTESTED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs': {"partition_object": self.test_partitions['norm_0_1_uniform'],
                               "threshold": 0.1,
                               "tail_weight_holdout": 0.01,
                               "internal_weight_holdout": 0.01,
                               "result_format": "SUMMARY"},
                    'out': {'success': False, 'observed_value': "NOTTESTED",
                            'details':
                                {'observed_partition':
                                     {'weights': [0.001, 0.006, 0.022, 0.07, 0.107, 0.146, 0.098, 0.04, 0.01, 0.0],
                                      'bins': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364],
                                      'tail_weights':[0.0,0.5]
                                     },
                                 'missing_percent': 0.0,
                                 'element_count': 1000,
                                 'missing_count': 0,
                                 'expected_partition': {'bins': [-3.721835843971108, -3.02304158492966, -2.324247325888213, -1.625453066846767, -0.926658807805319, -0.227864548763872, 0.470929710277574, 1.169723969319022, 1.868518228360469, 2.567312487401916, 3.266106746443364],
                                                        'weights': [0.00098, 0.00784, 0.04606, 0.12544, 0.24009999999999998, 0.24402, 0.20776, 0.07742, 0.02352, 0.00686],
                                                        'tail_weights':[0.005,0.005]
                                                        }
                                 }
                    }
                }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_to_be_less_than(
                *t['args'], **t['kwargs'])
            if t['out']['observed_value'] != 'NOTTESTED':
                if not np.allclose(out['observed_value'], t['out']['observed_value']):
                    print("Test case error:")
                    print(t)
                    print(out)
                self.assertTrue(np.allclose(
                    out['observed_value'], t['out']['observed_value']))
            if 'result_format' in t['kwargs'] and t['kwargs']['result_format'] == 'SUMMARY':
                self.assertTrue(np.allclose(out['result']['details']['observed_partition']['bins'],t['out']['details']['observed_partition']['bins']))
                self.assertTrue(np.allclose(out['result']['details']['observed_partition']['weights'],t['out']['details']['observed_partition']['weights']))
                self.assertTrue(np.allclose(out['result']['details']['observed_partition']['tail_weights'],t['out']['details']['observed_partition']['tail_weights']))
                self.assertTrue(np.allclose(out['result']['details']['expected_partition']['bins'],t['out']['details']['expected_partition']['bins']))
                self.assertTrue(np.allclose(out['result']['details']['expected_partition']['weights'],t['out']['details']['expected_partition']['weights']))
                self.assertTrue(np.allclose(out['result']['details']['expected_partition']['tail_weights'],t['out']['details']['expected_partition']['tail_weights']))

            if not out['success'] == t['out']['success']:
                print("Test case error:")
                print(t)
                print(out)
            self.assertEqual(out['success'], t['out']['success'])

    def test_expect_column_kl_divergence_to_be_less_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_to_be_less_than(
                'norm_0_1', {}, threshold=0.1)
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_to_be_less_than(
                'norm_0_1', self.test_partitions['norm_0_1_auto'])
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_to_be_less_than(
                'norm_0_1', self.test_partitions['norm_0_1_auto'], threshold=0.1, tail_weight_holdout=2)
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_to_be_less_than(
                'norm_0_1', self.test_partitions['norm_0_1_auto'], threshold=0.1, internal_weight_holdout=2)
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_to_be_less_than(
                'categorical_fixed', self.test_partitions['categorical_fixed'], threshold=0.1, internal_weight_holdout=0.01)

    def test_expect_column_kl_divergence_to_be_less_than_infinite_observed_value(self):

        # Test with discrete distribution
        discrete_df = ge.dataset.PandasDataset(
            {"x1": ['a', 'a', 'a', 'a', 'b', 'b', 'b', 'b', 'c', 'c']})
        discrete_partition_object = {"weights": [0.5, 0.5],
                                     "values": ["a", "b"]}
        discrete_out = discrete_df.expect_column_kl_divergence_to_be_less_than("x1",
                                                                               discrete_partition_object,
                                                                               0.05)
        discrete_out_json = json.dumps(
            discrete_out['result']['observed_value'])
        self.assertEqual(discrete_out['result']['observed_value'], None)
        self.assertEqual(discrete_out_json,
                         'null')

        # Test with continuous distribution
        continuous_df = ge.dataset.PandasDataset({"x2": [-1.5, -1.5, -1.35, -1.2, -1.19, -1.12, -1.04, -1, 0, 0, 0, 0.25,
                                                         0.34, 1, 1.1, 1.13, 1.22, 1.34, 1.4, 1.46, 1.5, 1.5, 1.5]})

        continuous_partition_object1 = {"weights": [0.1, 0.4, 0, 0, 0.4, 0.1],
                                        "bins": [-2, -1.5, -1, 0, 1, 1.5, 2]}  # Partition sets weight to zero for to a non-empty bin

        continuous_out1 = continuous_df.expect_column_kl_divergence_to_be_less_than("x2",
                                                                                    continuous_partition_object1,
                                                                                    0.05)
        continuous_out_json1 = json.dumps(
            continuous_out1['result']['observed_value'])

        self.assertEqual(continuous_out1['result']['observed_value'], None)
        self.assertEqual(continuous_out_json1,
                         'null')

        continuous_partition_object2 = {"weights": [0.25, 0.25, 0.25, 0.25],
                                        "bins": [-1, 0, 1, 1.5, 2]}  # Partition bins do not cover all of data set

        continuous_out2 = continuous_df.expect_column_kl_divergence_to_be_less_than("x2",
                                                                                    continuous_partition_object2,
                                                                                    0.05)
        continuous_out_json2 = json.dumps(
            continuous_out2['result']['observed_value'])

        self.assertEqual(continuous_out2['result']['observed_value'], None)
        self.assertEqual(continuous_out_json2,
                         'null')
        
        
    def test_expect_column_kl_divergence_to_be_less_than_infinite_return_bins(self):
    
    
        continuous_df=ge.dataset.PandasDataset({"x":[-1.95, 1.03, 1.00, 0.81, -2.27,  0.52, 2.45, -1.19,
                                                     -0.17, -1.54, 2.20, -2.66,  1.71,  1.59, 2.19]})
        
        
        #Both endpoints at infinity, no tail_holdout or internal_holdout
        continuous_partition_object={"weights":[0.3,0.15,0.05,0.05,0.2,0.25],
            "bins":[-np.inf,-2,-1,0,1,2,np.inf]}
        
        
        expected_details={
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-2,-1,0,1,2],
                                "weights": [3/15.0, 1/15.0, 2/15.0, 4/15.0],
                                "tail_weights": [2/15.0,3/15.0]
                            },
                            "expected_partition": {
                                "bins": [-2,-1,0,1,2],
                                "weights": [0.15,0.05,0.05,0.2],
                                "tail_weights":[0.3,0.25]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
        assertDeepAlmostEqual(expected_details,actual_details)
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
        
        
        #Both endpoints bounded, no tail_holdout or internal_holdout
        
        continuous_partition_object={"weights":[0.3,0.15,0.05,0.05,0.2,0.25],
            "bins":[-3,-2,-1,0,1,2,3]}
        
        
        expected_details={
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-3,-2,-1,0,1,2,3],
                                "weights": [2/15.0,3/15.0, 1/15.0, 2/15.0, 4/15.0,3/15.0],
                                "tail_weights": [0,0]
                            },
                            "expected_partition": {
                                "bins": [-3,-2,-1,0,1,2,3],
                                "weights": [0.3,0.15,0.05,0.05,0.2,0.25],
                                "tail_weights":[0,0]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
        assertDeepAlmostEqual(expected_details,actual_details)
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
        
        #Infinite KL Divergence, no tail_holdout or internal_holdout
        
        continuous_partition_object={"weights":[0.45,0.05,0.05,0.2,0.25],
            "bins":[-2,-1,0,1,2,3]}
        
        
        expected_details={
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-2,-1,0,1,2,3],
                                "weights": [3/15.0, 1/15.0, 2/15.0, 4/15.0,3/15.0],
                                "tail_weights": [2/15.0,0]
                            },
                            "expected_partition": {
                                "bins": [-2,-1,0,1,2,3],
                                "weights": [0.45,0.05,0.05,0.2,0.25],
                                "tail_weights":[0,0]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
        assertDeepAlmostEqual(expected_details,actual_details)
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
        
    
        #Both endpoints at infinity, non-zero tail hold_out
        continuous_partition_object={"weights":[0.3,0.15,0.05,0.05,0.2,0.25],
            "bins":[-np.inf,-2,-1,0,1,2,np.inf]}
        
        with self.assertRaises(ValueError):
            
            actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0.01, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
            
            
        
        #Upper at infinity, non-zero tail hold_out
        continuous_partition_object={"weights":[0.3,0.15,0.05,0.05,0.2,0.25],
            "bins":[-3,-2,-1,0,1,2,np.inf]}
        
        
        expected_details={
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-3,-2,-1,0,1,2],
                                "weights": [2/15.0,3/15.0, 1/15.0, 2/15.0, 4/15.0],
                                "tail_weights": [0,3/15.0]
                            },
                            "expected_partition": {
                                "bins": [-3,-2,-1,0,1,2],
                                "weights": [0.3*0.98,0.15*0.98,0.05*0.98,0.05*0.98,0.2*0.98],
                                "tail_weights":[0.02,0.98*0.25]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0.02, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
        assertDeepAlmostEqual(expected_details,actual_details)
        
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
    
            
        
        #Lower at infinity, non-zero tail holdout
        continuous_partition_object={"weights":[0.3,0.15,0.05,0.05,0.45],
            "bins":[-np.inf,-2,-1,0,1,2]}
        
        
        expected_details={
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-2,-1,0,1,2],
                                "weights": [3/15.0, 1/15.0, 2/15.0, 4/15.0],
                                "tail_weights": [2/15.0,3/15.0]
                            },
                            "expected_partition": {
                                "bins": [-2,-1,0,1,2],
                                "weights": [0.15*0.95,0.05*0.95,0.05*0.95,0.45*0.95],
                                "tail_weights":[0.3*0.95,0.05]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0.05, 
                                                                                 internal_weight_holdout=0,
                                                                                 result_format="COMPLETE")["result"]["details"]
        assertDeepAlmostEqual(expected_details,actual_details)
        
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
        
        #Bounded Endpoints, non-zero tail holdout, non-zero internal holdout
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.45],
            "bins":[-3,-2,-1,0,1,2]}
        
        
        expected_details= {
                            "observed_partition": {
                                # return expected_bins, since we used those bins to compute the observed_weights
                                "bins": [-3,-2,-1,0,1,2],
                                "weights": [2/15.0,3/15.0, 1/15.0, 2/15.0, 4/15.0],
                                "tail_weights": [0.0,3/15.0]
                            },
                            "expected_partition": {
                                "bins": [-3,-2,-1,0,1,2],
                                "weights": [0.3*0.9,0.15*0.9,0.05,0.10*0.9,0.45*0.9],
                                "tail_weights":[0.05/2,0.05/2]
                            }
                        }
                            
        actual_details=continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                                 continuous_partition_object,
                                                                                 0.05,
                                                                                 tail_weight_holdout=0.05, 
                                                                                 internal_weight_holdout=0.05,
                                                                                 result_format="COMPLETE")["result"]["details"]
        
        assertDeepAlmostEqual(expected_details,actual_details)
        
        self.assertTrue(is_valid_continuous_partition_object(actual_details["observed_partition"]))
        self.assertTrue(is_valid_continuous_partition_object(actual_details["expected_partition"]))
        
        
    def test_expect_column_kl_divergence_to_be_less_than_user_tail_weights(self):
    
        continuous_df=ge.dataset.PandasDataset({"x":[-1.95, 1.03, 1.00, 0.81, -2.27,  0.52, 2.45, -1.19,
                                                 -0.17, -1.54, 2.20, -2.66,  1.71,  1.59, 2.19]})
    
    
    
    #This should exit without error 
    
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.15],
        "bins":[-3,-2,-1,0,1,2], "tail_weights":[0.15,0.15]}
        
        continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                  continuous_partition_object,
                                                                  0.05)
        
            
    #Error: Only one tail weight
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.30],
                                     "bins":[-3,-2,-1,0,1,2], "tail_weights":[0.15]}
    
        self.assertFalse(is_valid_continuous_partition_object(continuous_partition_object))
                                                                  

        #Error: Use of tail_weights with infinite end points partition
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.15],
        "bins":[-3,-2,-1,0,1,np.inf], "tail_weights":[0.15,0.15]}
        
        with self.assertRaises(ValueError):
            continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                      continuous_partition_object,
                                                                      0.05) 
        
        
        #Error: Use of tail_weights and tail_weight_holdout
        
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.15],
        "bins":[-3,-2,-1,0,1,2], "tail_weights":[0.15,0.15]}
        
        with self.assertRaises(ValueError):
            continuous_df.expect_column_kl_divergence_to_be_less_than("x",
                                                                      continuous_partition_object,
                                                                      0.05,
                                                                      tail_weight_holdout=0.01)
        
        #Error: Tail_weights and weights don't add to one
        
        continuous_partition_object={"weights":[0.3,0.15,0.0,0.10,0.16],
        "bins":[-3,-2,-1,0,1,2], "tail_weights":[0.15,0.15]}
        
        self.assertFalse(is_valid_continuous_partition_object(continuous_partition_object))
        
        
        

    def test_expect_column_bootstrapped_ks_test_p_value_to_be_greater_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
                'norm_0_1', self.test_partitions['categorical_fixed'])
        test_partition = ge.dataset.util.kde_partition_data(
            self.D['norm_0_1'], estimate_tails=False)
        with self.assertRaises(ValueError):
            self.D.expect_column_bootstrapped_ks_test_p_value_to_be_greater_than(
                'norm_0_1', test_partition)

    def test_expect_column_chisquare_test_p_value_to_be_greater_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_chisquare_test_p_value_to_be_greater_than(
                'categorical_fixed', self.test_partitions['norm_0_1_auto'])


if __name__ == "__main__":
    unittest.main()
