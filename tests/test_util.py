import json
import numpy as np
import unittest

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

    def test_ensure_json_serializable(self):
        D = ge.dataset.PandasDataSet({
            'x' : [1,2,3,4,5,6,7,8,9,10],
        })
        D.expect_column_values_to_be_in_set("x", set([1,2,3,4,5,6,7,8,9]), mostly=.8)

        part = ge.dataset.util.partition_data(D.x)
        D.expect_column_kl_divergence_less_than("x", part, .6)

        #Dumping this JSON object verifies that everything is serializable        
        json.dumps(D.get_expectations_config(), indent=2)

        x = {'x': np.array([1, 2, 3])}
        ge.dataset.util.ensure_json_serializable(x)
        self.assertEqual(type(x['x']), type([1, 2, 3]))

if __name__ == "__main__":
    unittest.main()