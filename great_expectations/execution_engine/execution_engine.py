import copy
import logging
from typing import Any, Dict, Iterable, Tuple

from ruamel.yaml import YAML

from great_expectations.core.batch import BatchSpec
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
        # only used for accessing data in S3
        data_connector=None,
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

        # only for accessing S3
        self._data_connector = data_connector


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

    @property
    def data_connector(self):
        return self._data_connector

    @data_connector.setter
    def data_connector(self, data_connector):
        self._data_connector = data_connector


    def get_batch_data(
        self,
        batch_spec: BatchSpec,
    ) -> Any:
        """Interprets batch_data and returns the appropriate data.

        This method is primarily useful for utility cases (e.g. testing) where
        data is being fetched without a DataConnector and metadata like
        batch_markers is unwanted

        Note: this method is currently a thin wrapper for get_batch_data_and_markers.
        It simply suppresses the batch_markers.
        """
        batch_data, _ = self.get_batch_data_and_markers(batch_spec)
        return batch_data

    def load_batch_data(self, batch_id: str, batch_data: Any) -> None:
        """
        Loads the specified batch_data into the execution engine
        """
        self._batch_data[batch_id] = batch_data
        self._active_batch_data_id = batch_id

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
            metric_fn_type = getattr(metric_fn, "metric_fn_type", "data")
            if metric_fn_type in ["aggregate_fn"]:
                metric_fn_bundle.append(
                    (metric_to_resolve, metric_fn, metric_provider_kwargs)
                )
            elif metric_fn_type in [
                "map_fn",
                "map_condition",
                "window_fn",
                "window_condition",
            ]:
                # NOTE: 20201026 - JPC - we could use the fact that these metric functions return functions rather
                # than data to optimize compute in the future
                resolved_metrics[metric_to_resolve.id] = metric_fn(
                    **metric_provider_kwargs
                )
            elif metric_fn_type == "data":
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

    def get_compute_domain(self, domain_kwargs: dict) -> Tuple[Any, dict, dict]:
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

    def add_column_null_filter_row_condition(self, domain_kwargs):
        """EXPERIMENTAL

        Add a row condition for handling null filter.
        """
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
