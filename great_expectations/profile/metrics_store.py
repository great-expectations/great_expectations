import collections
from functools import reduce
import operator
from collections import defaultdict
from great_expectations.data_context.types.metrics import (
NamespaceAwareValidationMetric,
MultiBatchNamespaceAwareValidationMetric,
NamespaceAwareExpectationDefinedValidationMetric,
MultiBatchNamespaceAwareExpectationDefinedValidationMetric,
)


class MetricsStore(object):
    def __init__(self):
        self.single_batch_metrics = []
        self.dict_single_batch_metrics_by_multi_batch_key_by_batch = {}


    def add_single_batch_metric(
        self,
        data_asset_name,
        batch_fingerprint,
        metric_name,
        metric_kwargs,
        metric_value):

        new_metric = NamespaceAwareValidationMetric(
            data_asset_name=data_asset_name,
            batch_fingerprint=batch_fingerprint,
            metric_name=metric_name,
            metric_kwargs=metric_kwargs,
            metric_value=metric_value)

        self.single_batch_metrics.append(new_metric)

        batch_metrics = self.dict_single_batch_metrics_by_multi_batch_key_by_batch.get(batch_fingerprint.fingerprint)
        if not batch_metrics:
            batch_metrics = {}
            self.dict_single_batch_metrics_by_multi_batch_key_by_batch[batch_fingerprint.fingerprint] = batch_metrics
        self.dict_single_batch_metrics_by_multi_batch_key_by_batch[batch_fingerprint.fingerprint][new_metric.multi_batch_key] = new_metric

    def add_single_batch_expectation_defined_metric(
        self,
        data_asset_name,
        batch_fingerprint,
        expectation_type,
        result_key,
        metric_kwargs,
        metric_value):

        new_metric = NamespaceAwareExpectationDefinedValidationMetric(
            data_asset_name=data_asset_name,
            batch_fingerprint=batch_fingerprint,
            expectation_type=expectation_type,
            result_key=result_key,
            metric_kwargs=metric_kwargs,
            metric_value=metric_value)

        self.single_batch_metrics.append(new_metric)
        batch_metrics = self.dict_single_batch_metrics_by_multi_batch_key_by_batch.get(batch_fingerprint.fingerprint)
        if not batch_metrics:
            batch_metrics = {}
            self.dict_single_batch_metrics_by_multi_batch_key_by_batch[batch_fingerprint.fingerprint] = batch_metrics
        self.dict_single_batch_metrics_by_multi_batch_key_by_batch[batch_fingerprint.fingerprint][new_metric.multi_batch_key] = new_metric

    def get_multi_batch_metrics(self, batch_kwargs_list):
        """
        Return a list of multi batch metrics for a list of batches
        :param batch_fingerprints:
        :return: dict of multi batch metrics (by mb metric key).
                Values are MultiBatchNamespaceAwareValidationMetric or
                MultiBatchNamespaceAwareExpectationDefinedValidationMetric
        """

        dict_selected_batches = {}
        for batch_fingerprint, batch_metrics in self.dict_single_batch_metrics_by_multi_batch_key_by_batch.items():
            if batch_fingerprint in [bk.batch_fingerprint.fingerprint for bk in batch_kwargs_list]:
                dict_selected_batches[batch_fingerprint] = batch_metrics

        # let's compute the union of all metrics names that come from all the batches.
        # this will help us fill with nulls if a particular metric is missing from a batch
        # (e.g., due to the column missing)
        # Not performing this steps would result in non-uniform lengths of lists and we would
        # not be able to convert this dict of lists into a dataframe.
        metric_names_union = set()
        for batch_id, batch_metrics in dict_selected_batches.items():
            metric_names_union = metric_names_union.union(batch_metrics.keys())

        metrics_dict_of_lists = defaultdict(list)

        batch_index = list(self.dict_single_batch_metrics_by_multi_batch_key_by_batch.keys())

        for batch_id, batch_metrics in dict_selected_batches.items():
            # fill in the metrics that are present in the batch
            for metric_name, metric_value in batch_metrics.items():
                metrics_dict_of_lists[metric_name].append(metric_value)

            # fill in the metrics that are missing in the batch
            metrics_missing_in_batch = metric_names_union - set(batch_metrics.keys())
            for metric_name in metrics_missing_in_batch:
                metrics_dict_of_lists[metric_name].append(None)

        mb_metrics = {}
        for metric_key, single_batch_metric_list in metrics_dict_of_lists.items():
            mb_metric = self._make_multi_batch_metric_from_list_of_single_batch_metrics(metric_key[0], single_batch_metric_list,
                                                                                      batch_index)
            mb_metrics[mb_metric.key] = mb_metric

        return mb_metrics

    def _make_multi_batch_metric_from_list_of_single_batch_metrics(self, single_batch_metric_name, single_batch_metric_list, batch_index):
        """
        Utility method that gets a list of single batch metrics with the same multi-batch key (meaning that they are the same
        metric with the same kwargs, but obtained by validating different batches of the same data asset) and
        constructs a multi-batch metric for that key.

        :param single_batch_metric_name:
        :param single_batch_metric_list:
        :param batch_index:
        :return:
        """
        first_non_null_single_batch_metric = [item for item in single_batch_metric_list if item is not None][0]

        if 'NamespaceAwareValidationMetric' == single_batch_metric_name:
                mb_metric = MultiBatchNamespaceAwareValidationMetric(
                    data_asset_name=first_non_null_single_batch_metric.data_asset_name,
                    metric_name=first_non_null_single_batch_metric.metric_name,
                    metric_kwargs=first_non_null_single_batch_metric.metric_kwargs,
                    batch_fingerprints=batch_index,
                    batch_metric_values=[None if metric is None else metric.metric_value for metric in
                                         single_batch_metric_list]
                )
        elif 'NamespaceAwareExpectationDefinedValidationMetric' == single_batch_metric_name:
                mb_metric = MultiBatchNamespaceAwareExpectationDefinedValidationMetric(
                    data_asset_name = first_non_null_single_batch_metric.data_asset_name,
                    result_key = first_non_null_single_batch_metric.result_key,
                    expectation_type = first_non_null_single_batch_metric.expectation_type,
                    metric_kwargs = first_non_null_single_batch_metric.metric_kwargs,
                    batch_fingerprints = batch_index,
                    batch_metric_values = [None if metric is None else metric.metric_value for metric in single_batch_metric_list]
                )

        return mb_metric
