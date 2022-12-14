# from dataclasses import asdict, dataclass
# from typing import Any, Optional
#
# import logging
#
# from great_expectations.core.util import convert_to_json_serializable
# from great_expectations.execution_engine.execution_engine import MetricFunctionTypes, MetricPartialFunctionTypes
# from great_expectations.types import DictDot
# from great_expectations.validator.metric_configuration import MetricConfiguration
#
# logger = logging.getLogger(__name__)
#
#
# @dataclass(frozen=True)
# class MetricComputationConfiguration(DictDot):
#     """
#     MetricComputationConfiguration is a "dataclass" object, which holds components required for metric computation.
#     """
#
#     metric_configuration: MetricConfiguration
#     metric_fn: Any
#     metric_provider_kwargs: dict
#     compute_domain_kwargs: Optional[dict] = None
#     accessor_domain_kwargs: Optional[dict] = None
#
#     def __post_init__(self):
#         metric_fn_type: MetricFunctionTypes = getattr(self.metric_fn, "metric_fn_type", MetricFunctionTypes.VALUE)
#         if metric_fn_type not in [
#             MetricPartialFunctionTypes.MAP_FN,
#             MetricPartialFunctionTypes.MAP_SERIES,
#             MetricPartialFunctionTypes.MAP_CONDITION_FN,
#             MetricPartialFunctionTypes.MAP_CONDITION_SERIES,
#             MetricPartialFunctionTypes.WINDOW_FN,
#             MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
#             MetricPartialFunctionTypes.AGGREGATE_FN,
#             MetricFunctionTypes.VALUE,
#         ]:
#             logger.warning(
#                 f'Unrecognized metric function type while trying to resolve "{str(self.metric_configuration.id)}".'
#             )
#
#     def to_dict(self) -> dict:
#         """Returns: this BundledMetricConfiguration as a dictionary"""
#         return asdict(self)
#
#     def to_json_dict(self) -> dict:
#         """Returns: this BundledMetricConfiguration as a JSON dictionary"""
#         return convert_to_json_serializable(data=self.to_dict())
