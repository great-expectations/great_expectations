import collections
from functools import reduce
import operator
from great_expectations.data_context.types.metrics import NamespaceAwareValidationMetric, NamespaceAwareExpectationDefinedValidationMetric
from great_expectations.profile.metrics_utils import (
set_nested_value_in_dict,
get_nested_value_from_dict
)

class MetricsStore(object):
    EXPECTATION_DEFINED_METRICS_LOOKUP_TABLE = {
        ('expect_column_values_to_not_be_null', 'unexpected_percent'): ('column'),
        ('expect_column_quantile_values_to_be_between', ('observed_value', 'values')): (
            'column', ('quantile_ranges', 'quantiles')),

    }

    @classmethod
    def add_expectation_defined_metric_for_result_key(cls, d, result, data_asset_name, batch_fingerprint, t=()):
        for key, value in d.items():
            if isinstance(value, collections.Mapping):
                cls.add_expectation_defined_metric_for_result_key(value, result, data_asset_name, batch_fingerprint, t + (key,))
            else:
                result_key_lookup_key = key if t==() else (t + (key,))
                full_lookup_key = (result['expectation_config']['expectation_type'], result_key_lookup_key)
                metric_kwargs_names = cls.EXPECTATION_DEFINED_METRICS_LOOKUP_TABLE.get(full_lookup_key)
                if metric_kwargs_names:
                    metric_kwargs = {}
                    for metric_kwarg_name in metric_kwargs_names:
                        if isinstance(metric_kwarg_name, tuple):
                            set_nested_value_in_dict(metric_kwargs, metric_kwarg_name, get_nested_value_from_dict(result['expectation_config']['kwargs'], metric_kwarg_name))

                    new_metric = NamespaceAwareExpectationDefinedValidationMetric(
                        data_asset_name=data_asset_name,
                        batch_fingerprint=batch_fingerprint,
                        expectation_type=result['expectation_config']['expectation_type'],
                        result_key=result_key_lookup_key,
                        metric_kwargs=metric_kwargs,
                        metric_value=value)

                    yield new_metric


    @classmethod
    def get_metrics_for_expectation(cls, result, data_asset_name, batch_fingerprint):
        """
        Extract metrics from a validation result of one expectation.
        Depending on the type of the expectation, this method chooses the key
        in the result dictionary that should be returned as a metric
        (e.g., "observed_value" or "unexpected_percent").

        :param result: a validation result dictionary of one expectation
        :param data_asset_name:
        :param batch_fingerprint:
        :return: a dict {metric_name -> metric_value}
        """
        expectation_metrics = {
            # 'expect_column_distinct_values_to_be_in_set'
            # 'expect_column_kl_divergence_to_be_less_than',
            'expect_column_max_to_be_between': {
                'observed_value': 'column_max'
            },
            'expect_column_mean_to_be_between': {
                'observed_value': 'column_mean'
            },
            'expect_column_median_to_be_between': {
                'observed_value': 'column_median'
            },
            'expect_column_min_to_be_between': {
                'observed_value': 'column_min'
            },
            'expect_column_proportion_of_unique_values_to_be_between': {
                'observed_value': 'column_proportion_of_unique_values'
            },
            # 'expect_column_quantile_values_to_be_between',
            'expect_column_stdev_to_be_between': {
                'observed_value': 'column_stdev'
            },
            'expect_column_unique_value_count_to_be_between': {
                'observed_value': 'column_unique_count'
            },
            # 'expect_column_values_to_be_between',
            # 'expect_column_values_to_be_in_set',
            # 'expect_column_values_to_be_in_type_list',
            'expect_column_values_to_be_unique': {

            },
            # 'expect_table_columns_to_match_ordered_list',
            'expect_table_row_count_to_be_between': {
                'observed_value': 'row_count'
            }

        }

        metrics = []
        if result.get('result'):
            entry = expectation_metrics.get(result['expectation_config']['expectation_type'])
            if entry:
                for key in result['result'].keys():
                    metric_name = entry.get(key)
                    if metric_name:
                        metric_kwargs = {"column": result['expectation_config']['kwargs']['column']} if result['expectation_config'][
                    'kwargs'].get('column') else {}

                        new_metric = NamespaceAwareValidationMetric(
                            data_asset_name=data_asset_name,
                            batch_fingerprint=batch_fingerprint,
                            metric_name=metric_name,
                            metric_kwargs=metric_kwargs,
                            metric_value=result['result'][key])
                        metrics.append(new_metric)
            else:
                for new_metric in cls.add_expectation_defined_metric_for_result_key(result['result'], result, data_asset_name, batch_fingerprint):
                    metrics.append(new_metric)

        return metrics

