import logging
from collections import defaultdict

import warnings
from great_expectations.datasource.types import BatchKwargs
from great_expectations.data_context.types.metrics import NamespaceAwareValidationMetric, \
    MultiBatchNamespaceAwareValidationMetric
from great_expectations.util import get_metrics_for_expectation

logger = logging.getLogger(__name__)


class MultiBatchValidationMetaAnalysis(object):
    """MultiBatchDatasetProfiler
    TODO: content
    """

    @classmethod
    def _get_metrics_dict_by_batch_id(cls, validation_results_list, data_context):
        """
        An auxiliary method that gets a list of validation results and returns
        a dictionary of metrics

        :param validation_results_list: a list validation results where each item is a
                result of validating a batch against the same expectation suite
        :return: a dict: {batch_id -> {metric_name -> metric_value}}
        """
        metrics_dict = {}

        for j, one_batch_validation_results in enumerate(validation_results_list):
            #             print(json.dumps(one_batch_validation_results['meta'], indent=2))
            batch_id = cls.get_batch_id(one_batch_validation_results['meta']['batch_kwargs'])

            # NOTE: Eugene 2019-08-25: when validation results be a typed object,
            # that object will have data_asset_name property method that will
            # return a NormalizedDataAssetName. Until then we are constructing
            # a NormalizedDataAssetName from the string that we fetch from the dictionary
            normalized_data_asset_name = data_context._normalize_data_asset_name(
                one_batch_validation_results['meta']['data_asset_name'])
            metrics_dict[batch_id] = {}
            for i, result in enumerate(one_batch_validation_results['results']):
                cur_exp_metrics = get_metrics_for_expectation(result,
                                                              normalized_data_asset_name,
                                                              batch_id)
                for metric in cur_exp_metrics:
                    metrics_dict[batch_id][metric.key] = metric['metric_value']

        return metrics_dict

    @classmethod
    def get_metrics(cls, validation_results_list, data_context):
        """
        Get multi-batch metrics from a list of validation results

        :param validation_results_list: a list validation results where each item is a
                result of validating a batch against the same expectation suite
        :return: a dict: {multi-batch metric urn -> multi-batch metric}
        """
        metrics_dict = cls._get_metrics_dict_by_batch_id(validation_results_list, data_context)

        # let's compute the union of all metrics names that come from all the batches.
        # this will help us fill with nulls if a particular metric is missing from a batch
        # (e.g., due to the column missing)
        # Not performing this steps would result in non-uniform lengths of lists and we would
        # not be able to convert this dict of lists into a dataframe.
        metric_names_union = set()
        for batch_id, batch_metrics in metrics_dict.items():
            metric_names_union = metric_names_union.union(batch_metrics.keys())

        metrics_dict_of_lists = defaultdict(list)

        batch_index = list(metrics_dict.keys())

        for batch_id, batch_metrics in metrics_dict.items():
            value_list = []
            # fill in the metrics that are present in the batch
            for metric_name, metric_value in batch_metrics.items():
                metrics_dict_of_lists[metric_name].append(metric_value)

            # fill in the metrics that are missing in the batch
            metrics_missing_in_batch = metric_names_union - set(batch_metrics.keys())
            for metric_name in metrics_missing_in_batch:
                metrics_dict_of_lists[metric_name].append(None)

        mb_metrics = {}
        for metric_key, value_list in metrics_dict_of_lists.items():
            mb_metric = MultiBatchNamespaceAwareValidationMetric.from_urn(metric_key)
            mb_metric['batch_fingerprints'] = batch_index
            mb_metric['batch_metric_values'] = value_list
            mb_metrics[mb_metric.urn] = mb_metric

        return mb_metrics

    @classmethod
    def get_batch_id(cls, batch_kwargs):
        return BatchKwargs.build_batch_id(batch_kwargs)
