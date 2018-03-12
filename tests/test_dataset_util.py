import json
import datetime
import numpy as np
import unittest
from functools import wraps

import great_expectations as ge

class TestUtilMethods(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestUtilMethods, self).__init__(*args, **kwargs)
        self.D = ge.read_csv('./tests/test_sets/distributional_expectations_data_base.csv')

        with open('./tests/test_sets/test_partitions.json', 'r') as file:
            self.test_partitions = json.loads(file.read())

    def test_DotDict(self):
        D = ge.util.DotDict({
            'x' : [1,2,4],
            'y' : [1,2,5],
            'z' : ['hello', 'jello', 'mello'],
        })
        self.assertEqual(D.x[0],D.y[0])
        self.assertNotEqual(D.x[0],D.z[0])

    def test_continuous_partition_data_error(self):
        with self.assertRaises(ValueError):
            test_partition = ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], bins=-1)
            self.assertFalse(ge.dataset.util.is_valid_continuous_partition_object(test_partition))
            test_partition = ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], n_bins=-1)
            self.assertFalse(ge.dataset.util.is_valid_continuous_partition_object(test_partition))

    def test_partition_data_norm_0_1(self):
        test_partition = ge.dataset.util.continuous_partition_data(self.D.norm_0_1)
        for key, val in self.test_partitions['norm_0_1_auto'].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))


    def test_partition_data_bimodal(self):
        test_partition = ge.dataset.util.continuous_partition_data(self.D.bimodal)
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
        test_partition = ge.dataset.util.categorical_partition_data(self.D.categorical_fixed)
        for k in self.test_partitions['categorical_fixed']['values']:
            # Iterate over each categorical value and check that the weights equal those computed originally.
            self.assertEqual(
                self.test_partitions['categorical_fixed']['weights'][self.test_partitions['categorical_fixed']['values'].index(k)],
                test_partition['weights'][test_partition['values'].index(k)])

    def test_categorical_data_na(self):
        df = ge.dataset.PandasDataSet({
            'my_column': ["A", "B", "A", "B", None]
        })
        partition = ge.dataset.util.categorical_partition_data(df['my_column'])
        self.assertTrue(ge.dataset.util.is_valid_categorical_partition_object(partition))
        self.assertTrue(len(partition['values']) == 2)

    def test_is_valid_partition_object_simple(self):
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(ge.dataset.util.continuous_partition_data(self.D['norm_0_1'])))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(ge.dataset.util.continuous_partition_data(self.D['bimodal'])))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], bins='auto')))
        self.assertTrue(ge.dataset.util.is_valid_continuous_partition_object(ge.dataset.util.continuous_partition_data(self.D['norm_0_1'], bins='uniform', n_bins=10)))

    def test_generated_partition_objects(self):
        for partition_name, partition_object in self.test_partitions.items():
            result = ge.dataset.util.is_valid_partition_object(partition_object)
            if not result:
                print("Partition object " + partition_name + " is invalid.")
            self.assertTrue(result)

    def test_is_valid_partition_object_fails_length(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object({'bins': [0,1], 'weights': [0,1,2]}))

    def test_is_valid_partition_object_fails_weights(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object({'bins': [0,1,2], 'weights': [0.5,0.6]}))

    def test_is_valid_partition_object_fails_structure(self):
        self.assertFalse(ge.dataset.util.is_valid_partition_object({'weights': [0.5,0.5]}))
        self.assertFalse(ge.dataset.util.is_valid_partition_object({'bins': [0,1,2]}))

    def test_recursively_convert_to_json_serializable(self):
        D = ge.dataset.PandasDataSet({
            'x' : [1,2,3,4,5,6,7,8,9,10],
        })
        D.expect_column_values_to_be_in_set("x", set([1,2,3,4,5,6,7,8,9]), mostly=.8)

        part = ge.dataset.util.partition_data(D.x)
        D.expect_column_kl_divergence_to_be_less_than("x", part, .6)

        #Dumping this JSON object verifies that everything is serializable        
        json.dumps(D.get_expectations_config(), indent=2)


        x = {
            'w': [
                "aaaa", "bbbb", 1.3, 5, 6, 7
            ],
            'x': np.array([1, 2, 3]),
            'y': {
                'alpha' : None,
                'beta' : np.nan,
                'delta': np.inf,
                'gamma' : -np.inf
            },
            'z': set([1,2,3,4,5]),
            'zz': (1,2,3),
            'zzz': [
                datetime.datetime(2017,1,1),
                datetime.date(2017,5,1),
            ]
        }
        x = ge.dataset.util.recursively_convert_to_json_serializable(x)
        self.assertEqual(type(x['x']), list)

        try:
            x = unicode("abcdefg")
            x = ge.dataset.util.recursively_convert_to_json_serializable(x)
            self.assertEqual(type(x), unicode)
        except NameError:
            pass

    def test_expect_file_hash_to_equal(self):
        test_file = './tests/test_sets/Titanic.csv'
        # Test for non-existent file
        try:
            ge.expect_file_hash_to_equal('abc', value='abc')
        except IOError:
            pass
        # Test for non-existent hash algorithm
        try:
            ge.expect_file_hash_to_equal(test_file,
                                         hash_alg='md51',
                                         value='abc')
        except ValueError:
            pass
        # Test non-matching hash value
        self.assertFalse(ge.expect_file_hash_to_equal(test_file,
                                                      value='abc'))
        # Test matching hash value with default algorithm
        self.assertTrue(ge.expect_file_hash_to_equal(test_file,
                                                     value='63188432302f3a6e8c9e9c500ff27c8a'))
        # Test matching hash value with specified algorithm
        self.assertTrue(ge.expect_file_hash_to_equal(test_file,
                                                     value='f89f46423b017a1fc6a4059d81bddb3ff64891e3c81250fafad6f3b3113ecc9b',
                                                     hash_alg='sha256'))


"""
The following Parent and Child classes are used for testing documentation inheritance.
"""
class Parent(object):
    """Parent class docstring
    """

    @classmethod
    def expectation(cls, func):
        """Manages configuration and running of expectation objects.
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            # wrapper logic
            func(*args, **kwargs)

        return wrapper


    def override_me(self):
        """Parent method docstring
        Returns:
            Unattainable abiding satisfaction.
        """
        raise NotImplementedError


class Child(Parent):
    """
    Child class docstring
    """

    @ge.dataset.util.DocInherit
    @Parent.expectation
    def override_me(self):
        """Child method docstring
        Returns:
            Real, instantiable, abiding satisfaction.
        """


class TestDocumentation(unittest.TestCase):

    def test_doc_inheritance(self):
        c = Child()

        self.assertEqual(
            c.__getattribute__('override_me').__doc__,
        """Child method docstring
        Returns:
            Real, instantiable, abiding satisfaction.
        """ + '\n' +
        """Parent method docstring
        Returns:
            Unattainable abiding satisfaction.
        """
        )

if __name__ == "__main__":
    unittest.main()