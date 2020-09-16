import json
import unittest

import numpy as np

import great_expectations as ge
from great_expectations.data_context.util import file_relative_path
from great_expectations.dataset.util import build_categorical_partition_object


def test_build_categorical_partition(non_numeric_high_card_dataset):

    # Verify that we can build expected categorical partition objects
    # Note that this relies on the underlying sort behavior of the system in question
    # For weights, that will be unambiguous, but for values, it could depend on locale

    partition = build_categorical_partition_object(
        non_numeric_high_card_dataset, "medcardnonnum", sort="count"
    )

    assert partition == {
        "values": [
            "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
            "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
            "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
            "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
            "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
            "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
            "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
        ],
        "weights": [0.18, 0.17, 0.16, 0.145, 0.125, 0.11, 0.085, 0.02, 0.005],
    }

    partition = build_categorical_partition_object(
        non_numeric_high_card_dataset, "medcardnonnum", sort="value"
    )

    try:
        assert partition == {
            "values": [
                "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
                "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
                "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
                "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
                "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
                "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
                "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
                "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
                "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
            ],
            "weights": [0.16, 0.02, 0.125, 0.17, 0.085, 0.18, 0.145, 0.005, 0.11],
        }
    except AssertionError:
        # Postgres uses a lexigraphical sort that differs from the one used in python natively
        # Since we *want* to preserve the underlying system's ability to do compute (and the user
        # can override if desired), we allow this explicitly.
        assert partition == {
            "values": [
                "2K8njWnvuq1u6tkzreNhxTEyO8PTeWer",
                "ajcLVizD2vwZlmmGKyXYki03SWn7fnt3",
                "hW0kFZ6ijfciJWN4vvgcFa6MWv8cTeVk",
                "k8B9KCXhaQb6Q82zFbAzOESAtDxK174J",
                "mS2AVcLFp6i36sX7yAUrdfM0g0RB2X4D",
                "NfX4KfEompMbbKloFq8NQpdXtk5PjaPe",
                "NhTsracusfp5V6zVeWqLZnychDl7jjO4",
                "oRnY5jDWFw2KZRYLh6ihFd021ggy4UxJ",
                "T7EUE54HUhyJ9Hnxv1pKY0Bmg42qiggP",
            ],
            "weights": [0.16, 0.085, 0.18, 0.145, 0.005, 0.02, 0.125, 0.11, 0.17],
        }


class TestUtilMethods(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.D = ge.read_csv(
            file_relative_path(
                __file__, "../test_sets/distributional_expectations_data_base.csv"
            )
        )

        with open(
            file_relative_path(__file__, "../test_sets/test_partitions.json")
        ) as file:
            self.test_partitions = json.loads(file.read())

    def test_continuous_partition_data_error(self):
        with self.assertRaises(ValueError):
            test_partition = ge.dataset.util.continuous_partition_data(
                self.D["norm_0_1"], bins=-1
            )
            self.assertFalse(
                ge.dataset.util.is_valid_continuous_partition_object(test_partition)
            )
            test_partition = ge.dataset.util.continuous_partition_data(
                self.D["norm_0_1"], n_bins=-1
            )
            self.assertFalse(
                ge.dataset.util.is_valid_continuous_partition_object(test_partition)
            )

    def test_partition_data_norm_0_1(self):
        test_partition = ge.dataset.util.continuous_partition_data(self.D.norm_0_1)
        for key, val in self.test_partitions["norm_0_1_auto"].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_partition_data_bimodal(self):
        test_partition = ge.dataset.util.continuous_partition_data(self.D.bimodal)
        for key, val in self.test_partitions["bimodal_auto"].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_kde_partition_data_norm_0_1(self):
        test_partition = ge.dataset.util.kde_partition_data(self.D.norm_0_1)
        for key, val in self.test_partitions["norm_0_1_kde"].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_kde_partition_data_bimodal(self):
        test_partition = ge.dataset.util.kde_partition_data(self.D.bimodal)
        for key, val in self.test_partitions["bimodal_kde"].items():
            self.assertEqual(len(val), len(test_partition[key]))
            self.assertTrue(np.allclose(test_partition[key], val))

    def test_categorical_data_fixed(self):
        test_partition = ge.dataset.util.categorical_partition_data(
            self.D.categorical_fixed
        )
        for k in self.test_partitions["categorical_fixed"]["values"]:
            # Iterate over each categorical value and check that the weights equal those computed originally.
            self.assertEqual(
                self.test_partitions["categorical_fixed"]["weights"][
                    self.test_partitions["categorical_fixed"]["values"].index(k)
                ],
                test_partition["weights"][test_partition["values"].index(k)],
            )

    def test_categorical_data_na(self):
        df = ge.dataset.PandasDataset({"my_column": ["A", "B", "A", "B", None]})
        partition = ge.dataset.util.categorical_partition_data(df["my_column"])
        self.assertTrue(
            ge.dataset.util.is_valid_categorical_partition_object(partition)
        )
        self.assertTrue(len(partition["values"]) == 2)

    def test_is_valid_partition_object_simple(self):
        self.assertTrue(
            ge.dataset.util.is_valid_continuous_partition_object(
                ge.dataset.util.continuous_partition_data(self.D["norm_0_1"])
            )
        )
        self.assertTrue(
            ge.dataset.util.is_valid_continuous_partition_object(
                ge.dataset.util.continuous_partition_data(self.D["bimodal"])
            )
        )
        self.assertTrue(
            ge.dataset.util.is_valid_continuous_partition_object(
                ge.dataset.util.continuous_partition_data(
                    self.D["norm_0_1"], bins="auto"
                )
            )
        )
        self.assertTrue(
            ge.dataset.util.is_valid_continuous_partition_object(
                ge.dataset.util.continuous_partition_data(
                    self.D["norm_0_1"], bins="uniform", n_bins=10
                )
            )
        )

    def test_generated_partition_objects(self):
        for partition_name, partition_object in self.test_partitions.items():
            result = ge.dataset.util.is_valid_partition_object(partition_object)
            if not result:
                print("Partition object " + partition_name + " is invalid.")
            self.assertTrue(result)

    def test_is_valid_partition_object_fails_length(self):
        self.assertFalse(
            ge.dataset.util.is_valid_partition_object(
                {"bins": [0, 1], "weights": [0, 1, 2]}
            )
        )

    def test_is_valid_partition_object_fails_weights(self):
        self.assertFalse(
            ge.dataset.util.is_valid_partition_object(
                {"bins": [0, 1, 2], "weights": [0.5, 0.6]}
            )
        )
        # weights don't add
        continuous_partition_object = {
            "weights": [0.3, 0.15, 0.0, 0.10, 0.16],
            "bins": [-3, -2, -1, 0, 1, 2],
            "tail_weights": [0.15, 0.15],
        }
        self.assertFalse(
            ge.dataset.util.is_valid_continuous_partition_object(
                continuous_partition_object
            )
        )

    def test_is_valid_partition_object_only_one_tail_weight(self):
        continuous_partition_object = {
            "weights": [0.3, 0.15, 0.0, 0.10, 0.30],
            "bins": [-3, -2, -1, 0, 1, 2],
            "tail_weights": [0.15],
        }
        self.assertFalse(
            ge.dataset.util.is_valid_continuous_partition_object(
                continuous_partition_object
            )
        )

    def test_is_valid_partition_object_fails_structure(self):
        self.assertFalse(
            ge.dataset.util.is_valid_partition_object({"weights": [0.5, 0.5]})
        )
        self.assertFalse(ge.dataset.util.is_valid_partition_object({"bins": [0, 1, 2]}))

    def test_validate_distribution_parameters(self):
        D = ge.read_csv(
            file_relative_path(
                __file__, "../test_sets/fixed_distributional_test_dataset.csv"
            )
        )

        # ------ p_value ------
        with self.assertRaises(ValueError):
            # p_value is 0
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 1], p_value=0
            )
        with self.assertRaises(ValueError):
            # p_value negative
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 1], p_value=-0.1
            )
        with self.assertRaises(ValueError):
            P_value = 1
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 1], p_value=1
            )

        with self.assertRaises(ValueError):
            # p_value greater than 1
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 1], p_value=1.1
            )
        with self.assertRaises(ValueError):
            # params is none
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=None
            )

        # ---- std_dev ------
        with self.assertRaises(ValueError):
            # std_dev is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params={"mean": 0, "std_dev": 0}
            )
        with self.assertRaises(ValueError):
            # std_dev is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params={"mean": 0, "std_dev": -1}
            )
        with self.assertRaises(ValueError):
            # std_dev is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 0]
            )
        with self.assertRaises(ValueError):
            # std_dev is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, -1]
            )

        # ------- beta ------
        with self.assertRaises(ValueError):
            # beta, alpha is 0, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"alpha": 0, "beta": 0.1}
            )
        with self.assertRaises(ValueError):
            # beta, alpha is negative, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"alpha": -1, "beta": 0.1}
            )
        with self.assertRaises(ValueError):
            # beta, beta is 0, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"alpha": 0.1, "beta": 0}
            )
        with self.assertRaises(ValueError):
            # beta, beta is negative, dict params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"alpha": 0, "beta": -1}
            )
        with self.assertRaises(ValueError):
            # beta, alpha is 0, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[0, 0.1]
            )
        with self.assertRaises(ValueError):
            # beta, alpha is negative, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[-1, 0.1]
            )
        with self.assertRaises(ValueError):
            # beta, beta is 0, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[0.1, 0]
            )
        with self.assertRaises(ValueError):
            # beta, beta is negative, list params
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[0.1, -1]
            )

        with self.assertRaises(ValueError):
            # beta, missing alpha, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"beta": 0.1}
            )
        with self.assertRaises(ValueError):
            # beta, missing beta, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params={"alpha": 0.1}
            )
        with self.assertRaises(ValueError):
            # beta, missing beta, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[1]
            )
        with self.assertRaises(ValueError):
            # beta, missing beta, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "beta", distribution="beta", params=[1, 1, 1, 1, 1]
            )

        # ------ Gamma -------
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params={"alpha": 0}
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params={"alpha": -1}
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params={"alpha": 0}
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is missing, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params={}
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is missing, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params=[]
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params=[0]
            )
        with self.assertRaises(ValueError):
            # gamma, alpha is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params=[-1]
            )
        with self.assertRaises(ValueError):
            # gamma, too many arguments, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "gamma", distribution="gamma", params=[1, 1, 1, 1]
            )

        # ----- chi2 --------
        with self.assertRaises(ValueError):
            # chi2, df is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params={"df": 0}
            )
        with self.assertRaises(ValueError):
            # chi2, df is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params={"df": -1}
            )
        with self.assertRaises(ValueError):
            # chi2, df is missing, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params={}
            )
        with self.assertRaises(ValueError):
            # chi2, df is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params=[0]
            )
        with self.assertRaises(ValueError):
            # chi2, df is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params=[-1]
            )
        with self.assertRaises(ValueError):
            # chi2, df is missing, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params=[]
            )
        with self.assertRaises(ValueError):
            # chi2, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "chi2", distribution="chi2", params=[1, 1, 1, 5]
            )
        # ----- norm ------
        with self.assertRaises(ValueError):
            # norm, too many arguments, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "norm", distribution="norm", params=[0, 1, 500]
            )

        # ----- uniform -----
        with self.assertRaises(ValueError):
            # uniform, scale is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "uniform", distribution="uniform", params=[0, 0]
            )
        with self.assertRaises(ValueError):
            # uniform, scale is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "uniform", distribution="uniform", params=[0, -1]
            )
        with self.assertRaises(ValueError):
            # uniform, scale is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "uniform", distribution="uniform", params={"loc": 0, "scale": -1}
            )
        with self.assertRaises(ValueError):
            # uniform, scale is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "uniform", distribution="uniform", params={"loc": 0, "scale": 0}
            )

        with self.assertRaises(ValueError):
            # uniform, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "uniform", distribution="uniform", params=[0, 1, 500]
            )

        # --- expon ---
        with self.assertRaises(ValueError):
            # expon, scale is 0, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="expon", params=[0, 0]
            )
        with self.assertRaises(ValueError):
            # expon, scale is negative, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="expon", params=[0, -1]
            )
        with self.assertRaises(ValueError):
            # expon, scale is 0, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="expon", params={"loc": 0, "scale": 0}
            )
        with self.assertRaises(ValueError):
            # expon, scale is negative, dict
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="expon", params={"loc": 0, "scale": -1}
            )
        with self.assertRaises(ValueError):
            # expon, too many parameters, list
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="expon", params=[0, 1, 500]
            )

        # --- misc ---
        with self.assertRaises(AttributeError):
            # non-supported distribution
            D.expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than(
                "exponential", distribution="fakedistribution", params=[0, 1]
            )

    def test_infer_distribution_parameters(self):
        D = ge.read_csv(
            file_relative_path(
                __file__, "../test_sets/fixed_distributional_test_dataset.csv"
            )
        )

        with self.assertRaises(TypeError):
            ge.dataset.util.infer_distribution_parameters(
                data=D.norm, distribution="norm", params=["wrong_param_format"]
            )
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.norm_std, distribution="norm", params=None
        )
        self.assertEqual(t["mean"], D.norm_std.mean())
        self.assertEqual(t["std_dev"], D.norm_std.std())
        self.assertEqual(t["loc"], 0)
        self.assertEqual(t["scale"], 1)

        # beta
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.beta, distribution="beta"
        )
        self.assertEqual(
            t["alpha"],
            (t["mean"] ** 2)
            * (((1 - t["mean"]) / t["std_dev"] ** 2) - (1 / t["mean"])),
            "beta dist, alpha infer",
        )
        self.assertEqual(
            t["beta"], t["alpha"] * ((1 / t["mean"]) - 1), "beta dist, beta infer"
        )

        # gamma
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.gamma, distribution="gamma"
        )
        self.assertEqual(t["alpha"], D.gamma.mean())

        # uniform distributions
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.uniform, distribution="uniform"
        )
        self.assertEqual(t["min"], min(D.uniform), "uniform, min infer")
        self.assertEqual(
            t["max"], max(D.uniform) - min(D.uniform), "uniform, max infer"
        )

        uni_loc = 5
        uni_scale = 10
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.uniform,
            distribution="uniform",
            params={"loc": uni_loc, "scale": uni_scale},
        )
        self.assertEqual(t["min"], uni_loc, "uniform, min infer")
        self.assertEqual(t["max"], uni_scale, "uniform, max infer")

        # expon distribution
        with self.assertRaises(AttributeError):
            ge.dataset.util.infer_distribution_parameters(
                data=D.norm, distribution="fakedistribution"
            )

        # chi2
        t = ge.dataset.util.infer_distribution_parameters(
            data=D.chi2, distribution="chi2"
        )
        self.assertEqual(t["df"], D.chi2.mean())

    def test_create_multiple_expectations(self):
        D = ge.dataset.PandasDataset(
            {
                "x": [1, 2, 3, 4, 5, 6],
                "y": [0, 2, 4, 6, 8, 10],
                "z": ["hi", "hello", "hey", "howdy", "hola", "holy smokes"],
                "zz": ["a", "b", "c", "hi", "howdy", "hola"],
            }
        )

        # Test kwarg
        results = ge.dataset.util.create_multiple_expectations(
            D,
            ["x", "y"],
            "expect_column_values_to_be_in_set",
            value_set=[1, 2, 3, 4, 5, 6],
        )
        self.assertTrue(results[0].success)
        self.assertFalse(results[1].success)

        # Test positional argument
        results = ge.dataset.util.create_multiple_expectations(
            D, ["x", "y"], "expect_column_values_to_be_in_set", [1, 2, 3, 4, 5, 6]
        )
        self.assertTrue(results[0].success)
        self.assertFalse(results[1].success)

        results = ge.dataset.util.create_multiple_expectations(
            D, ["z", "zz"], "expect_column_values_to_match_regex", "h"
        )
        self.assertTrue(results[0].success)
        self.assertFalse(results[1].success)

        # Non-argumentative expectation
        results = ge.dataset.util.create_multiple_expectations(
            D, ["z", "zz"], "expect_column_values_to_not_be_null"
        )
        self.assertTrue(results[0].success)
        self.assertTrue(results[1].success)

        # Key error when non-existant column is called
        with self.assertRaises(KeyError):
            ge.dataset.util.create_multiple_expectations(
                D, ["p"], "expect_column_values_to_be_in_set", ["hi"]
            )
        # Attribute error when non-existant expectation is called
        with self.assertRaises(AttributeError):
            ge.dataset.util.create_multiple_expectations(
                D, ["z"], "expect_column_values_to_be_fake_news"
            )


if __name__ == "__main__":
    unittest.main()
