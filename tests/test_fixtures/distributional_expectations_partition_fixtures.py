import copy
import json
import sys

import numpy as np
import pandas as pd
import scipy.stats as stats

import great_expectations as ge

"""
Use this file to generate random datasets for testing distributional expectations.

Tests expect two datasets: "distributional_expectations_data_base.csv" and "distributional_expectations_data_test.csv"
They also expect a set of partitions: "test_partitions.json"

The partitions should be built from distributional_expectations_data_base.csv. The tests will use distributional_expectations_data_test.csv

"""


def generate_new_data(seed):
    np.random.seed(seed=seed)

    norm_0_1 = stats.norm.rvs(0, 1, 1000)
    norm_1_1 = stats.norm.rvs(1, 1, 1000)
    norm_10_1 = stats.norm.rvs(10, 1, 1000)
    bimodal = np.concatenate((norm_0_1[:500], norm_10_1[500:]))
    categorical_fixed = (["A"] * 540) + (["B"] * 320) + (["C"] * 140)

    return pd.DataFrame(
        {
            "norm_0_1": norm_0_1,
            "norm_1_1": norm_1_1,
            "norm_10_1": norm_10_1,
            "bimodal": bimodal,
            "categorical_fixed": categorical_fixed,
        }
    )


def generate_new_partitions(df):
    test_partitions = {}
    for column in ["norm_0_1", "norm_1_1", "bimodal"]:
        partition_object = ge.dataset.util.kde_partition_data(df[column])
        # Print how close sum of weights is to one for a quick visual consistency check when data are generated
        # print(column + '_kde: '+ str(abs(1-np.sum(partition_object['weights']))))
        test_partitions[column + "_kde"] = partition_object

        for bin_type in ["uniform", "ntile", "auto"]:
            partition_object = ge.dataset.util.continuous_partition_data(
                df[column], bin_type
            )
            # Print how close sum of weights is to one for a quick visual consistency check when data are generated
            # print(column + '_' + bin_type + ': ' + str(abs(1 - np.sum(partition_object['weights']))))
            test_partitions[column + "_" + bin_type] = partition_object

        # Create infinite endpoint partitions:
        inf_partition = copy.deepcopy(test_partitions[column + "_auto"])
        inf_partition["weights"] = inf_partition["weights"] * (1 - 0.01)
        inf_partition["tail_weights"] = [0.005, 0.005]
        test_partitions[column + "_auto_inf"] = inf_partition

    partition_object = ge.dataset.util.categorical_partition_data(
        df["categorical_fixed"]
    )
    test_partitions["categorical_fixed"] = partition_object
    alt_partition = ge.dataset.util.categorical_partition_data(df["categorical_fixed"])
    # overwrite weights with uniform weights to give a testing dataset
    alt_partition["weights"] = [1.0 / len(alt_partition["values"])] * len(
        alt_partition["values"]
    )
    test_partitions["categorical_fixed_alternate"] = alt_partition

    return test_partitions


if __name__ == "__main__":
    df = generate_new_data(seed=42)
    d = df.to_dict(orient="list")
    json.dump(d, open("../test_sets/distributional_expectations_data_base.json", "w"))
    test_partitions = generate_new_partitions(df)

    test_partitions = ge.data_asset.util.recursively_convert_to_json_serializable(
        test_partitions
    )
    with open("../test_sets/test_partitions_definition_fixture.json", "w") as file:
        file.write(json.dumps(test_partitions))

    df = generate_new_data(seed=20190501)
    d = df.to_dict(orient="list")
    json.dump(d, open("../test_sets/distributional_expectations_data_test.json", "w"))
    print("Done generating new base data, partitions, and test data.")
