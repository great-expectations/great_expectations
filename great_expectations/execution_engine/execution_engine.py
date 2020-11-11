import copy
import logging
from enum import Enum
from typing import Any, Dict, Iterable, Tuple, Union

from ruamel.yaml import YAML

from great_expectations.exceptions import GreatExpectationsError
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.default_flow_style = False


class NoOpDict:
    def __getitem__(self, item):
        return None

    def __setitem__(self, key, value):
        return None


class ExecutionEngine:
    recognized_batch_spec_defaults = set()

    def __init__(
        self,
        name=None,
        caching=True,
        batch_spec_defaults=None,
        batch_data_dict=None,
        validator=None,
    ):
        self.name = name
        self._validator = validator

        # NOTE: using caching makes the strong assumption that the user will not modify the core data store
        # (e.g. self.spark_df) over the lifetime of the dataset instance
        self._caching = caching
        # NOTE: 20200918 - this is a naive cache; update.
        if self._caching:
            self._metric_cache = {}
        else:
            self._metric_cache = NoOpDict()

        if batch_spec_defaults is None:
            batch_spec_defaults = {}
        batch_spec_defaults_keys = set(batch_spec_defaults.keys())
        if not batch_spec_defaults_keys <= self.recognized_batch_spec_defaults:
            logger.warning(
                "Unrecognized batch_spec_default(s): %s"
                % str(batch_spec_defaults_keys - self.recognized_batch_spec_defaults)
            )

        self._batch_spec_defaults = {
            key: value
            for key, value in batch_spec_defaults.items()
            if key in self.recognized_batch_spec_defaults
        }

        if batch_data_dict is None:
            batch_data_dict = dict()
        self._batch_data = batch_data_dict
        self._active_batch_data_id = None

    def configure_validator(self, validator):
        """Optionally configure the validator as appropriate for the execution engine."""
        pass

    @property
    def active_batch_data_id(self):
        """The batch id for the default batch data.

        When an execution engine is asked to process a compute domain that does
        not include a specific batch_id, then the data associated with the
        active_batch_data_id will be used as the default.
        """
        if self._active_batch_data_id is not None:
            return self._active_batch_data_id
        elif len(self.loaded_batch_data) == 1:
            return list(self.loaded_batch_data.keys())[0]
        else:
            return None

    @property
    def active_batch_data(self):
        """The data from the currently-active batch.
        """
        if self.active_batch_data_id is None:
            return None
        else:
            return self.loaded_batch_data.get(self.active_batch_data_id)

    @property
    def loaded_batch_data(self):
        """The current dictionary of batches."""
        return self._batch_data

    def load_batch_data(self, batch_id: str, batch_data: Any) -> None:
        """
        Loads the specified batch_data into the execution engine
        """
        self._batch_data[batch_id] = batch_data
        self._active_batch_data_id = batch_id

    def process_batch_request(self, batch_request, batch_spec):
        """Use ExecutionEngine-specific configuration to translate any batch_request keys into batch_spec keys

        Args:
            batch_request (dict): batch_request to process
            batch_spec (dict): batch_spec to map processed batch_request keys to

        Returns:
            batch_spec (dict)
        """
        raise NotImplementedError

    def resolve_metrics(
        self,
        metrics_to_resolve: Iterable[MetricConfiguration],
        metrics: Dict[Tuple, Any] = None,
        runtime_configuration: dict = None,
    ) -> dict:
        """resolve_metrics is the main entrypoint for an execution engine. The execution engine will compute the value
        of the provided metrics.

        Args:
            metrics_to_resolve: the metrics to evaluate
            metrics: already-computed metrics currently available to the engine
            runtime_configuration: runtime configuration information

        Returns:
            resolved_metrics (Dict): a dictionary with the values for the metrics that have just been resolved.
        """
        if metrics is None:
            metrics = dict()
        resolved_metrics = dict()

        metric_fn_bundle = []
        for metric_to_resolve in metrics_to_resolve:
            metric_class, metric_fn = get_metric_provider(
                metric_name=metric_to_resolve.metric_name, execution_engine=self
            )
            try:
                metric_dependencies = {
                    k: metrics[v.id]
                    for k, v in metric_to_resolve.metric_dependencies.items()
                }
            except KeyError as e:
                raise GreatExpectationsError(f"Missing metric dependency: {str(e)}")
            metric_provider_kwargs = {
                "cls": metric_class,
                "execution_engine": self,
                "metric_domain_kwargs": metric_to_resolve.metric_domain_kwargs,
                "metric_value_kwargs": metric_to_resolve.metric_value_kwargs,
                "metrics": metric_dependencies,
                "runtime_configuration": runtime_configuration,
            }
            if metric_fn is None:
                (
                    metric_fn,
                    compute_domain_kwargs,
                    accessor_domain_kwargs,
                ) = metric_dependencies.pop("metric_partial_fn")
                metric_fn_bundle.append(
                    (
                        metric_to_resolve,
                        metric_fn,
                        compute_domain_kwargs,
                        accessor_domain_kwargs,
                        metric_provider_kwargs,
                    )
                )
                continue
            metric_fn_type = getattr(
                metric_fn, "metric_fn_type", MetricFunctionTypes.VALUE
            )
            if metric_fn_type in [
                MetricPartialFunctionTypes.MAP_FN,
                MetricPartialFunctionTypes.MAP_CONDITION_FN,
                MetricPartialFunctionTypes.WINDOW_FN,
                MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
                MetricPartialFunctionTypes.AGGREGATE_FN,
            ]:
                # NOTE: 20201026 - JPC - we could use the fact that these metric functions return functions rather
                # than data to optimize compute in the future
                resolved_metrics[metric_to_resolve.id] = metric_fn(
                    **metric_provider_kwargs
                )
            elif metric_fn_type == MetricFunctionTypes.VALUE:
                resolved_metrics[metric_to_resolve.id] = metric_fn(
                    **metric_provider_kwargs
                )
            else:
                logger.warning(
                    f"Unrecognized metric function type while trying to resolve {str(metric_to_resolve.id)}"
                )
                resolved_metrics[metric_to_resolve.id] = metric_fn(
                    **metric_provider_kwargs
                )
        if len(metric_fn_bundle) > 0:
            resolved_metrics.update(self.resolve_metric_bundle(metric_fn_bundle))

        return resolved_metrics

    def resolve_metric_bundle(self, metric_fn_bundle):
        """Resolve a bundle of metrics with the same compute domain as part of a single trip to the compute engine."""
        raise NotImplementedError

    def get_compute_domain(
        self, domain_kwargs: dict, domain_type: Union[str, "MetricDomainTypes"],
    ) -> Tuple[Any, dict, dict]:
        """get_compute_domain computes the optimal domain_kwargs for computing metrics based on the given domain_kwargs
        and specific engine semantics.

        Returns:
            A tuple consisting of three elements:

            1. data correspondig to the compute domain;
            2. a modified copy of domain_kwargs describing the domain of the data returned in (1);
            3. a dictionary describing the access instructions for data elements included in the compute domain
                (e.g. specific column name).

            In general, the union of the compute_domain_kwargs and accessor_domain_kwargs will be the same as the domain_kwargs
            provided to this method.
        """

        raise NotImplementedError

    def add_column_row_condition(
        self, domain_kwargs, filter_null=True, filter_nan=False
    ):
        """EXPERIMENTAL

        Add a row condition for handling null filter.
        """
        if filter_null is False and filter_nan is False:
            logger.warning(
                "add_column_row_condition called with no filter condition requested"
            )
            return domain_kwargs

        if filter_nan:
            raise GreatExpectationsError(
                "Base ExecutionEngine does not support adding nan condition filters"
            )

        if "row_condition" in domain_kwargs and domain_kwargs["row_condition"]:
            raise GreatExpectationsError(
                "ExecutionEngine does not support updating existing row_conditions."
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert "column" in domain_kwargs
        column = domain_kwargs["column"]
        new_domain_kwargs["condition_parser"] = "great_expectations__experimental__"
        new_domain_kwargs["row_condition"] = f'col("{column}").notnull()'
        return new_domain_kwargs


class MetricPartialFunctionTypes(Enum):
    MAP_FN = "map_fn"
    MAP_SERIES = "map_series"
    MAP_CONDITION_FN = "map_condition_fn"
    MAP_CONDITION_SERIES = "map_condition_series"
    WINDOW_FN = "window_fn"
    WINDOW_CONDITION_FN = "window_condition_fn"
    AGGREGATE_FN = "aggregate_fn"


class MetricFunctionTypes(Enum):
    VALUE = "value"
    MAP_VALUES = "map_values"
    WINDOW_VALUES = "window_values"
    AGGREGATE_VALUE = "aggregate_value"


class MetricDomainTypes(Enum):
    IDENTITY = "identity"
    COLUMN = "column"
    COLUMN_PAIR = "column_pair"
    TABLE = "table"
    OTHER = "other"
