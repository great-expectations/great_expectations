import unittest
import json
import numpy as np

import great_expectations as ge


class TestDistributionalExpectations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDistributionalExpectations, self).__init__(*args, **kwargs)
        self.D = ge.read_csv('./tests/test_sets/distributional_expectations_data_test.csv')

        with open('./tests/test_sets/test_partitions.json', 'r') as file:
            self.test_partitions = json.loads(file.read())

    def test_expect_column_chisquare_test_p_value_greater_than(self):
        T = [
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed'],
                        'p': 0.05
                        },
                    'out': {'success': True, 'true_value': 1.}
                },
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed_alternate'],
                        'p': 0.05
                    },
                    'out': {'success': False, 'true_value': 5.9032943409869654e-06}
                }
        ]
        for t in T:
            out = self.D.expect_column_chisquare_test_p_value_greater_than(*t['args'], **t['kwargs'])
            self.assertEqual(out['success'],t['out']['success'])
            self.assertEqual(out['true_value'], t['out']['true_value'])


    def test_expect_column_chisquare_test_p_value_greater_than_new_categorical_val(self):
        categorical_list = (['A'] * 25) + (['B'] * 25) + (['C'] * 25) + (['D'] * 25)
        df = ge.dataset.PandasDataSet({'categorical': categorical_list})

        out = df.expect_column_chisquare_test_p_value_greater_than('categorical', self.test_partitions['categorical_fixed_alternate'])
        self.assertEqual(out['success'], False)

        out = df.expect_column_chisquare_test_p_value_greater_than('categorical', self.test_partitions['categorical_fixed_alternate'], tail_weight_holdout=0.25)
        self.assertEqual(out['success'], True)

    def test_expect_column_chisquare_test_p_value_greater_than_missing_categorical_val(self):
        categorical_list = (['A'] * 61) + (['B'] * 39)
        df = ge.dataset.PandasDataSet({'categorical': categorical_list})
        out = df.expect_column_chisquare_test_p_value_greater_than('categorical', self.test_partitions['categorical_fixed'])
        self.assertEqual(out['success'], False)


    def test_expect_column_kl_divergence_less_than_discrete(self):
        T = [
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed'],
                        'threshold': 0.1
                        },
                    'out': {'success': True, 'true_value': 0.}
                },
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed_alternate'],
                        'threshold': 0.1
                        },
                    'out': {'success': False, 'true_value': 0.12599700286677529}
                }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_less_than(*t['args'], **t['kwargs'])
            self.assertTrue(np.allclose(out['success'], t['out']['success']))
            self.assertTrue(np.allclose(out['true_value'], t['out']['true_value']))

    def test_execpt_column_kl_divergence_less_than_discrete_holdout(self):
        df = ge.dataset.PandasDataSet({'a': ['a', 'a', 'b', 'c']})
        out = df.expect_column_kl_divergence_less_than('a',
                                                       {'partition': ['a', 'b'], 'weights': [0.6, 0.4]},
                                                       threshold=0.1,
                                                       tail_weight_holdout=0.1)
        self.assertEqual(out['success'], True)
        self.assertTrue(np.allclose(out['true_value'], [0.099431384003497381]))

        out = df.expect_column_kl_divergence_less_than('a',
                                                       {'partition': ['a', 'b'], 'weights': [0.6, 0.4]},
                                                       threshold=0.1,
                                                       tail_weight_holdout=0.05)
        self.assertEqual(out['success'], False)
        self.assertTrue(np.isclose(out['true_value'], [0.23216776319077681]))

        out = df.expect_column_kl_divergence_less_than('a',
                                                       {'partition': ['a', 'b'], 'weights': [0.6, 0.4]},
                                                       threshold=0.1)
        self.assertEqual(out['success'], False)
        self.assertTrue(np.isclose(out['true_value'], [np.inf]))

    def test_expect_column_bootrapped_ks_test_p_value_greater_than(self):
        T = [
                {
                    'args': ['norm_0_1'],
                    'kwargs': {'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05},
                    'out': {'success': True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_10_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_10_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_10_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_10_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['bimodal_auto'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['bimodal_kde'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                }
            ]
        for t in T:
            out = self.D.expect_column_bootstrapped_ks_test_p_value_greater_than(*t['args'], **t['kwargs'])
            if out['success'] != t['out']['success']:
                print("Test case error:")
                print(t)
                print(out)
            self.assertEqual(out['success'], t['out']['success'])

    def test_expect_column_kl_divergence_less_than_continuous(self):
        T = [
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'true_value': 'NOTTESTED'}
                },
                ## Note higher threshold example for kde
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde'],
                              "threshold": 0.3,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 1e-5,
                              "internal_weight_holdout": 1e-5},
                    'out':{'success':False, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['bimodal_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':True, 'true_value': 'NOTTESTED'}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'true_value': "NOTTESTED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform'],
                              "threshold": 0.1,
                              "tail_weight_holdout": 0.01,
                              "internal_weight_holdout": 0.01},
                    'out':{'success':False, 'true_value': "NOTTESTED"}
                }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_less_than(*t['args'], **t['kwargs'])
            if t['out']['true_value'] != 'NOTTESTED':
                if not np.allclose(out['true_value'],t['out']['true_value']):
                    print("Test case error:")
                    print(t)
                    print(out)
                self.assertTrue(np.allclose(out['true_value'],t['out']['true_value']))
            self.assertEqual(out['success'],t['out']['success'])

    def test_expect_column_kl_divergence_less_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_less_than('norm_0_1', {}, threshold=0.1)
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_less_than('norm_0_1', self.test_partitions['norm_0_1_auto'])
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_less_than('norm_0_1', self.test_partitions['norm_0_1_auto'], threshold=0.1, tail_weight_holdout=2)
        with self.assertRaises(ValueError):
            self.D.expect_column_kl_divergence_less_than('categorical_fixed', self.test_partitions['categorical_fixed'], threshold=0.1, internal_weight_holdout=0.01)

    def test_expect_column_bootstrapped_ks_test_p_value_greater_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_bootstrapped_ks_test_p_value_greater_than('norm_0_1', self.test_partitions['categorical_fixed'])
        test_partition = ge.dataset.util.kde_partition_data(self.D['norm_0_1'], estimate_tails=False)
        with self.assertRaises(ValueError):
            self.D.expect_column_bootstrapped_ks_test_p_value_greater_than('norm_0_1', test_partition)

    def test_expect_column_chisquare_test_p_value_greater_than_bad_parameters(self):
        with self.assertRaises(ValueError):
            self.D.expect_column_chisquare_test_p_value_greater_than('categorical_fixed', self.test_partitions['norm_0_1_auto'])

if __name__ == "__main__":
    unittest.main()