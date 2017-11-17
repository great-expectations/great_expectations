import pandas as pd
import numpy as np
import scipy.stats as stats
import great_expectations as ge
import json

generate_new_data = False
generate_new_partitions = True

if generate_new_data == True:
    norm_0_1 = stats.norm.rvs(0, 1, 100)
    norm_1_1 = stats.norm.rvs(1, 1, 100)
    norm_10_1 = stats.norm.rvs(10, 1, 100)
    bimodal = np.concatenate((norm_0_1[:50], norm_10_1[50:]))
    categorical_fixed = (['A'] * 54) + (['B'] * 32) + (['C'] * 14)

    df = pd.DataFrame( {
        'norm_0_1': norm_0_1,
        'norm_1_1': norm_1_1,
        'bimodal': bimodal,
        'categorical_fixed': categorical_fixed
    })

    df.to_csv('../test_sets/distributional_expectations_data_test.csv')
else:
    df = pd.read_csv('../test_sets/distributional_expectations_data_base.csv')

D = ge.dataset.PandasDataSet(df)

if generate_new_partitions == True:
    test_partitions = {}
    for column in ['norm_0_1', 'norm_1_1', 'bimodal']:
        for partition_type in ['no_holdout', 'tail_holdout', 'both_holdout']:
            if partition_type == 'no_holdout':
                internal_weight_holdout = 0.
                tail_weight_holdout = 0.
            elif partition_type == 'tail_holdout':
                internal_weight_holdout = 0.
                tail_weight_holdout = 1e-5
            elif partition_type == 'both_holdout':
                internal_weight_holdout = 1e-5
                tail_weight_holdout = 1e-5

            partition_object = ge.dataset.util.kde_partition_data(df[column],
                                                                  internal_weight_holdout=internal_weight_holdout,
                                                                  tail_weight_holdout=tail_weight_holdout)
            print(column + '_kde_' + partition_type + ': '+ str(abs(1-np.sum(partition_object['weights']))))
            test_partitions[column + '_kde_' + partition_type] = partition_object

            for bin_type in ['uniform', 'ntile', 'auto']:
                partition_object = ge.dataset.util.continuous_partition_data(df[column], bin_type,
                                                                             internal_weight_holdout=internal_weight_holdout,
                                                                             tail_weight_holdout=tail_weight_holdout)
                print(column + '_' + bin_type + '_' + partition_type + ': ' + str(abs(1 - np.sum(partition_object['weights']))))
                test_partitions[column + '_' + bin_type + '_' + partition_type] = partition_object

    partition_object = ge.dataset.util.categorical_partition_data(df['categorical_fixed'])
    test_partitions['categorical_fixed'] = partition_object
    alt_partition = ge.dataset.util.categorical_partition_data(df['categorical_fixed'])
    alt_partition['weights'] = [1./len(alt_partition['partition'])] * len(alt_partition['partition'])
    test_partitions['categorical_fixed_alternate'] = alt_partition
    with open('../test_sets/test_partitions.json', 'w') as file:
        file.write(json.dumps(test_partitions))
else:
    with open('../test_sets/test_partitions.json', 'r') as file:
        test_partitions = json.loads(file.read())