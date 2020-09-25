from great_expectations.expectations.expectation import ColumnMapDatasetExpectation


class ExpectColumnValueLengthsToBeBetween(ColumnMapDatasetExpectation):
    map_metric = "map.value_length_between"
    metric_dependencies = (
        "map.value_length_between.count",
        "map.nonnull.count",
    )
    success_keys = (
        "strictly",
        "mostly",
    )


#
#     default_kwarg_values = {
#         "row_condition": None,
#         "condition_parser": None,
#         "strictly": None,
#         "mostly": 1,
#         "result_format": "BASIC",
#         "include_config": True,
#         "catch_exceptions": False,
#     }
#
#     def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
#         return super().validate_configuration(configuration)
#
#     @PandasExecutionEngine.column_map_metric(
#         metric_name="map.value_length_between",
#         metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
#         metric_value_keys=("strictly",),
#         metric_dependencies=tuple(),
#     )
#     def _pandas_map_value_length_between(
#             self,
#             series: pd.Series,
#             metrics: dict,
#             metric_domain_kwargs: dict,
#             metric_value_kwargs: dict,
#             runtime_configuration: dict = None,
#               filter_column_isnull: bool = True,
#     ):
#         strictly = metric_value_kwargs["strictly"]
#         series_diff = series.diff()
#         # The first element is null, so it gets a bye and is always treated as True
#         series_diff[series_diff.isnull()] = 1
#
#         if strictly:
#             return series_diff > 0
#         else:
#             return series_diff >= 0
#
#     @Expectation.validates(metric_dependencies=metric_dependencies)
#     def _validates(
#             self,
#             configuration: ExpectationConfiguration,
#             metrics: dict,
#             runtime_configuration: dict = None,
#     ):
#         validation_dependencies = self.get_validation_dependencies(configuration)[
#             "metrics"
#         ]
#         metric_vals = extract_metrics(validation_dependencies, metrics, configuration)
#         mostly = configuration.get_success_kwargs().get(
#             "mostly", self.default_kwarg_values.get("mostly")
#         )
#         if runtime_configuration:
#             result_format = runtime_configuration.get(
#                 "result_format", self.default_kwarg_values.get("result_format")
#             )
#         else:
#             result_format = self.default_kwarg_values.get("result_format")
#         return _format_map_output(
#             result_format=parse_result_format(result_format),
#             success=(
#                             metric_vals.get("map.value_length_between.count")
#                             / metric_vals.get("map.nonnull.count")
#                     )
#                     >= mostly,
#             element_count=metric_vals.get("map.count"),
#             nonnull_count=metric_vals.get("map.nonnull.count"),
#             unexpected_count=metric_vals.get("map.nonnull.count")
#                              - metric_vals.get("map.value_length_between.count"),
#             unexpected_list=metric_vals.get("map.value_length_between.unexpected_values"),
#             unexpected_index_list=metric_vals.get("map.is_in.unexpected_index"),
#         )
