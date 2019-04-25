import pandas as pd
import numpy as np
import scipy.stats as stats
import great_expectations as ge
import json
import sys

"""
Use this file to generate random datasets for testing distributional expectations.

Tests expect two datasets: "distributional_expectations_data_base.csv" and "distributional_expectations_data_test.csv" 
They also expect a set of partitions: "test_partitions.json"

The partitions should be built from distributional_expectations_data_base.csv. The tests will use distributional_expectations_data_test.csv

"""


def generate_new_data():
    norm_0_1 = stats.norm.rvs(0, 1, 1000)
    norm_1_1 = stats.norm.rvs(1, 1, 1000)
    norm_10_1 = stats.norm.rvs(10, 1, 1000)
    bimodal = np.concatenate((norm_0_1[:500], norm_10_1[500:]))
    categorical_fixed = (['A'] * 540) + (['B'] * 320) + (['C'] * 140)

    return pd.DataFrame({
        'norm_0_1': norm_0_1,
        'norm_1_1': norm_1_1,
        'norm_10_1': norm_10_1,
        'bimodal': bimodal,
        'categorical_fixed': categorical_fixed
    })


def generate_new_partitions(df):
    test_partitions = {}
    for column in ['norm_0_1', 'norm_1_1', 'bimodal']:
        partition_object = ge.data_asset.util.kde_partition_data(df[column])
        # Print how close sum of weights is to one for a quick visual consistency check when data are generated
        #print(column + '_kde: '+ str(abs(1-np.sum(partition_object['weights']))))
        test_partitions[column + '_kde'] = partition_object

        for bin_type in ['uniform', 'ntile', 'auto']:
            partition_object = ge.data_asset.util.continuous_partition_data(
                df[column], bin_type)
            # Print how close sum of weights is to one for a quick visual consistency check when data are generated
            #print(column + '_' + bin_type + ': ' + str(abs(1 - np.sum(partition_object['weights']))))
            test_partitions[column + '_' + bin_type] = partition_object

    partition_object = ge.data_asset.util.categorical_partition_data(
        df['categorical_fixed'])
    test_partitions['categorical_fixed'] = partition_object
    alt_partition = ge.data_asset.util.categorical_partition_data(
        df['categorical_fixed'])
    # overwrite weights with uniform weights to give a testing dataset
    alt_partition['weights'] = [
        1./len(alt_partition['values'])] * len(alt_partition['values'])
    test_partitions['categorical_fixed_alternate'] = alt_partition

    return test_partitions


if __name__ == "__main__":
    # Set precision we'll use:
    precision = sys.float_info.dig
    print("Setting pandas float_format to use " +
          str(precision) + " digits of precision.")

    df = generate_new_data()
    df.to_csv('../test_sets/distributional_expectations_data_base.csv',
              float_format='%.' + str(precision) + 'g')
    test_partitions = generate_new_partitions(df)

    ge.data_asset.util.ensure_json_serializable(test_partitions)
    with open('../test_sets/test_partitions.json', 'w') as file:
        file.write(json.dumps(test_partitions))

    df = generate_new_data()
    df.to_csv('../test_sets/distributional_expectations_data_test.csv',
              float_format='%.' + str(precision) + 'g')
    print("Done generating new base data, partitions, and test data.")
