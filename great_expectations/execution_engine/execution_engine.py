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
        data_context=None,
        batch_spec_defaults=None,
        batches=None,
        validator=None,
    ):
        self.name = name
        self._data_context = data_context
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
        self._loaded_batch_id = None

    def configure_validator(self, validator):
        self._validator = validator

    @property
    def data_context(self):
        """Returns the internal Data Context (An object containing the data)"""
        return self._data_context

    @data_context.setter
    def data_context(self, data_context):
        """A setter for the Data Context"""
        self._data_context = data_context

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
    def loaded_batch_id(self):
        ### TODO: A good docstring, and rename to "active_batch_data_id"
        """A getter for the batch id"""
        return self._loaded_batch_id

    @property
    def loaded_batch(self):
        if self.loaded_batch_id is None:
            return None
        else:
            return self.batches.get(self.loaded_batch_id)

    @property
    def batches(self):
        """A getter for the Execution Engine's batches"""
        return self._batches

    # def process_batch_definition(self, batch_definition, batch_spec):
    #     """Use ExecutionEngine-specific configuration to translate any batch_definition keys into batch_spec keys
    #
    #     Args:
    #         batch_definition (dict): batch_definition to process
    #         batch_spec (dict): batch_spec to map processed batch_definition keys to
    #
    #     Returns:
    #         batch_spec (dict)
    #     """
    #     raise NotImplementedError

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
        """A method used to 'resolve', or configure, metrics whose full configuration has not been provided. Given a
        dictionary of metrics, any metric in the Iterable metrics_to_resolve has their configuration and provider
        function
        obtained so that it can be computed. The updated list of metrics is then returned

                Args:
                    batches (Dict[str, Batch): \
                        A Dictionary of batch names and corresponding batches
                    metrics_to_resolve (Iterable[MetricConfiguration]): \
                        A list/ other iterable of MetricEdgeKeys that represent the metrics whose configurations
                        need to be resolved
                    metrics (float or None): \
                        The list of the metrics and the corresponding configurations as registered within the registry
                    runtime_configuration (dict): \
                        A metric's runtime configuration, representing changes in result format/ other domains at
                        runtime


                Returns:
                    success (boolean), percent_success (float)
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
        raise NotImplementedError

    def get_compute_domain(self, domain_kwargs: dict) -> Tuple[Any, dict, dict]:
        raise NotImplementedError

    def add_column_null_filter_row_condition(self, column, domain_kwargs):
        if "row_condition" in domain_kwargs:
            raise GreatExpectationsError(
                "ExecutionEngine does not support updating existing row_conditions."
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        new_domain_kwargs["condition_engine"] = "great_expectations__experimental__"
        new_domain_kwargs["row_condition"] = f'col("{column}").notnull()'
        return new_domain_kwargs
