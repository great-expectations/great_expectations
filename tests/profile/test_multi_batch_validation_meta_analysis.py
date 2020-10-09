# import pytest
#
# from great_expectations.profile.basic_dataset_profiler import BasicDatasetProfiler
#
# from great_expectations.profile.multi_batch_validation_meta_analysis import MultiBatchValidationMetaAnalysis
# from great_expectations.datasource.types import BatchKwargs
# from great_expectations.data_context.types.metrics import (
#     ValidationMetric,
#     MultiBatchNamespaceAwareValidationMetric,
#     NamespaceAwareExpectationDefinedValidationMetric,
#     MultiBatchNamespaceAwareExpectationDefinedValidationMetric,
# )
#
#
# # noinspection PyPep8Naming
# def test_get_metrics_basic(titanic_multibatch_data_context):
#     """
#     We will call MultiBatchValidationMetaAnalysis.get_metrics on 2 validation results
#     and verify that the multi batch metrics that are returned are as expected.
#     """
#     context = titanic_multibatch_data_context
#     my_ds = context.get_datasource("mydatasource")
#     generator = my_ds.get_generator("mygenerator")
#     all_batch_kwargs = [x for x in generator.get_iterator(data_asset_name='titanic')]
#     all_batch_kwargs = sorted(all_batch_kwargs, key=lambda x: x['path'])
#     all_batch_kwargs
#
#     batch_profiling_results = []
#
#     profiler = BasicDatasetProfiler
#
#     for batch_kwargs_set in all_batch_kwargs:
#         context.create_expectation_suite("titanic", "foo", overwrite_existing=True)
#         batch = context.get_batch('titanic', "foo", batch_kwargs=batch_kwargs_set)
#         expectation_suite, validation_result = profiler.profile(batch, run_id="profiling_" + BatchKwargs.build_batch_fingerprint(
#             batch._batch_kwargs).fingerprint)
#         batch_profiling_results.append(validation_result)
#
#
#     mb_metrics = MultiBatchValidationMetaAnalysis.get_metrics(batch_profiling_results, context)
#
#
#     # total number of metrics
#     assert len(mb_metrics.values()) == 27
#
#     # since there are 2 batches in the input, each multi batch metric must have exactly 2 items in the
#     # lists of batch fingerprints and metric values
#     assert set([len(m.batch_fingerprints) for m in mb_metrics.values()]) == set([2])
#     assert set([len(m.batch_metric_values) for m in mb_metrics.values()]) == set([2])
#
#     # verify the counts of MultiBatchNamespaceAwareValidationMetric and MultiBatchNamespaceAwareExpectationDefinedValidationMetric
#     assert len([m for m in mb_metrics.values() if isinstance(m, MultiBatchNamespaceAwareValidationMetric)]) == 19
#     assert len([m for m in mb_metrics.values() if isinstance(m, MultiBatchNamespaceAwareExpectationDefinedValidationMetric)]) == 8
#
#     # verify the expectation types for MultiBatchNamespaceAwareExpectationDefinedValidationMetric
#     assert set([m.expectation_type for m in mb_metrics.values() if
#          isinstance(m, MultiBatchNamespaceAwareExpectationDefinedValidationMetric)]) == set(['expect_column_values_to_not_be_null', 'expect_column_quantile_values_to_be_between'])
