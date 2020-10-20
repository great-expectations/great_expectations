import copy
import logging
from typing import Any, Dict, Iterable, Tuple

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
        batches=None,
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

        if batches is None:
            batches = dict()
        self._batches = batches
        self._active_batch_data_id = None

    def configure_validator(self, validator):
        self._validator = validator

    @property
    def _active_validation(self):
        """Given that the active_validation property is present, returns it"""
        if not self.validator:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute `_active_validation` - please set "
                f"_validator attribute first."
            )
        else:
            return self.validator._active_validation

    @_active_validation.setter
    def _active_validation(self, active_validation):
        """A setter for the active_validation property"""
        if not self.validator:
            raise AttributeError(
                f"'{type(self).__name__}' object cannot set `_active_validation` attribute - please "
                f"set "
                f"_validator attribute first."
            )
        else:
            self.validator._active_validation = active_validation

    @property
    def active_batch_data_id(self):
        """The batch id for the default batch data.

        When an execution engine is asked to process a compute domain that does
        not include a specific batch_id, then the data associated with the
        active_batch_data_id will be used as the default.
        """
        return self._active_batch_data_id

    @property
    def active_batch_data(self):
        """The data from the currently-active batch.
        """
        if self.active_batch_data_id is None:
            return None
        else:
            return self.batches.get(self.active_batch_data_id)

    @property
    def batches(self):
        """The current dictionary of batches."""
        return self._batches

    def load_batch(self, batch_definition):
        """
        Load a Batch specified by the batch_definition.

        :param batch_definition:
        :return:
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
            metric_dependencies = {
                k: metrics[v.id]
                for k, v in metric_to_resolve.metric_dependencies.items()
            }
            metric_provider_kwargs = {
                "cls": metric_class,
                "execution_engine": self,
                "metric_domain_kwargs": metric_to_resolve.metric_domain_kwargs,
                "metric_value_kwargs": metric_to_resolve.metric_value_kwargs,
                "metrics": metric_dependencies,
                "runtime_configuration": runtime_configuration,
            }
            if getattr(metric_fn, "_bundle_metric", False) is True:
                metric_fn_bundle.append(
                    (metric_to_resolve, metric_fn, metric_provider_kwargs,)
                )
            else:
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

    def add_column_null_filter_row_condition(self, column, domain_kwargs):
        """EXPERIMENTAL

        Add a row condition for handling null filter.
        """
        if "row_condition" in domain_kwargs:
            raise GreatExpectationsError(
                "ExecutionEngine does not support updating existing row_conditions."
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        new_domain_kwargs["condition_engine"] = "great_expectations__experimental__"
        new_domain_kwargs["row_condition"] = f'col("{column}").notnull()'
        return new_domain_kwargs
