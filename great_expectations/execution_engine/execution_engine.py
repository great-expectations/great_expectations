from __future__ import annotations

import copy
import logging
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.batch_manager import BatchManager
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.expectations.registry import get_metric_provider
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
)
from great_expectations.types import DictDot
from great_expectations.util import filter_properties_dict
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,  # noqa: TCH001
)

if TYPE_CHECKING:
    from great_expectations.compatibility.pyspark import functions as F
    from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
    from great_expectations.core.batch import (
        BatchData,
        BatchDataType,
        BatchMarkers,
        BatchSpec,
    )
    from great_expectations.expectations.metrics.metric_provider import MetricProvider
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class NoOpDict:
    def __getitem__(self, item):
        return None

    def __setitem__(self, key, value):
        return None

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def update(self, value):
        return None


@dataclass(frozen=True)
class MetricComputationConfiguration(DictDot):
    """
    MetricComputationConfiguration is a "dataclass" object, which holds components required for metric computation.
    """

    metric_configuration: MetricConfiguration
    metric_fn: sa.func | F  # type: ignore[valid-type]
    metric_provider_kwargs: dict
    compute_domain_kwargs: Optional[dict] = None
    accessor_domain_kwargs: Optional[dict] = None

    @public_api
    def to_dict(self) -> dict:
        """Returns: this MetricComputationConfiguration as a Python dictionary

        Returns:
            (dict) representation of present object
        """
        return asdict(self)

    @public_api
    def to_json_dict(self) -> dict:
        """Returns: this MetricComputationConfiguration as a JSON dictionary

        Returns:
            (dict) representation of present object as JSON-compatible Python dictionary
        """
        return convert_to_json_serializable(data=self.to_dict())


@dataclass
class SplitDomainKwargs:
    """compute_domain_kwargs, accessor_domain_kwargs when split from domain_kwargs

    The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
    """

    compute: dict
    accessor: dict


@public_api
class ExecutionEngine(ABC):
    """ExecutionEngine defines interfaces and provides common methods for loading Batch of data and compute metrics.

    ExecutionEngine is the parent class of every backend-specific computational class, tasked with loading Batch of
    data and computing metrics.  Each subclass (corresponding to Pandas, Spark, SQLAlchemy, and any other computational
    components) utilizes generally-applicable methods defined herein, while implementing required backend-specific
    interfaces.  ExecutionEngine base class also performs the important task of translating cloud storage resource URLs
    to format and protocol compatible with given computational mechanism (e.g, Pandas, Spark).  Then ExecutionEngine
    subclasses access the referenced data according to their paritcular compatible protocol and return Batch of data.

    In order to obtain Batch of data, ExecutionEngine (through implementation of key interface methods by subclasses)
    gets data records and also provides access references so that different aspects of data can be loaded at once.
    ExecutionEngine uses BatchManager for Batch caching in order to reduce load on computational backends.

    Crucially, ExecutionEngine serves as focal point for resolving (i.e., computing) metrics.  Wherever opportunities
    arize to bundle multiple metric computations (e.g., SQLAlchemy, Spark), ExecutionEngine utilizes subclasses in order
    to provide specific functionality (bundling of computation is available only for "deferred execution" computational
    systems, such as SQLAlchemy and Spark; it is not available for Pandas, because Pandas computations are immediate).

    Finally, ExecutionEngine defines interfaces for Batch data sampling and splitting Batch of data along defined axes.

    Constructor builds an ExecutionEngine, using provided configuration options (instatiation is done by child classes).

    Args:
        name: (str) name of this ExecutionEngine
        caching: (Boolean) if True (default), then resolved (computed) metrics are added to local in-memory cache.
        batch_spec_defaults: dictionary of BatchSpec overrides (useful for amending configuration at runtime).
        batch_data_dict: dictionary of Batch objects with corresponding IDs as keys supplied at initialization time
        validator: Validator object (optional) -- not utilized in V3 and later versions
    """

    recognized_batch_spec_defaults: Set[str] = set()

    def __init__(  # noqa: PLR0913
        self,
        name: Optional[str] = None,
        caching: bool = True,
        batch_spec_defaults: Optional[dict] = None,
        batch_data_dict: Optional[dict] = None,
        validator: Optional[Validator] = None,
    ) -> None:
        self.name = name
        self._validator = validator

        # NOTE: using caching makes the strong assumption that the user will not modify the core data store
        # (e.g. self.spark_df) over the lifetime of the dataset instance
        self._caching = caching
        # NOTE: 20200918 - this is a naive cache; update.
        if self._caching:
            self._metric_cache: Union[Dict, NoOpDict] = {}
        else:
            self._metric_cache = NoOpDict()

        if batch_spec_defaults is None:
            batch_spec_defaults = {}

        batch_spec_defaults_keys = set(batch_spec_defaults.keys())
        if not batch_spec_defaults_keys <= self.recognized_batch_spec_defaults:
            logger.warning(
                f"""Unrecognized batch_spec_default(s): \
{str(batch_spec_defaults_keys - self.recognized_batch_spec_defaults)}
"""
            )

        self._batch_spec_defaults = {
            key: value
            for key, value in batch_spec_defaults.items()
            if key in self.recognized_batch_spec_defaults
        }

        self._batch_manager = BatchManager(execution_engine=self)

        if batch_data_dict is None:
            batch_data_dict = {}

        self._load_batch_data_from_dict(batch_data_dict=batch_data_dict)

        # Gather the call arguments of the present function (and add the "class_name"), filter out the Falsy values, and
        # set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "name": name,
            "caching": caching,
            "batch_spec_defaults": batch_spec_defaults,
            "batch_data_dict": batch_data_dict,
            "validator": validator,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

    def configure_validator(  # noqa: B027 # empty-method-without-abstract-decorator
        self, validator
    ) -> None:
        """Optionally configure the validator as appropriate for the execution engine."""
        pass

    @property
    def config(self) -> dict:
        return self._config

    @property
    def dialect(self):
        return None

    @property
    def batch_manager(self) -> BatchManager:
        """Getter for batch_manager"""
        return self._batch_manager

    def _load_batch_data_from_dict(
        self, batch_data_dict: Dict[str, BatchDataType]
    ) -> None:
        """
        Loads all data in batch_data_dict using cache_batch_data
        """
        batch_id: str
        batch_data: BatchDataType
        for batch_id, batch_data in batch_data_dict.items():
            self.load_batch_data(batch_id=batch_id, batch_data=batch_data)

    def load_batch_data(self, batch_id: str, batch_data: BatchDataType) -> None:
        self._batch_manager.save_batch_data(batch_id=batch_id, batch_data=batch_data)

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

    @abstractmethod
    def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:
        raise NotImplementedError

    def resolve_metrics(
        self,
        metrics_to_resolve: Iterable[MetricConfiguration],
        metrics: Optional[Dict[Tuple[str, str, str], MetricValue]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """resolve_metrics is the main entrypoint for an execution engine. The execution engine will compute the value
        of the provided metrics.

        Args:
            metrics_to_resolve: the metrics to evaluate
            metrics: already-computed metrics currently available to the engine
            runtime_configuration: runtime configuration information

        Returns:
            resolved_metrics (Dict): a dictionary with the values for the metrics that have just been resolved.
        """
        if not metrics_to_resolve:
            return metrics or {}

        metric_fn_direct_configurations: List[MetricComputationConfiguration]
        metric_fn_bundle_configurations: List[MetricComputationConfiguration]
        (
            metric_fn_direct_configurations,
            metric_fn_bundle_configurations,
        ) = self._build_direct_and_bundled_metric_computation_configurations(
            metrics_to_resolve=metrics_to_resolve,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
        )
        return self._process_direct_and_bundled_metric_computation_configurations(
            metric_fn_direct_configurations=metric_fn_direct_configurations,
            metric_fn_bundle_configurations=metric_fn_bundle_configurations,
        )

    def resolve_metric_bundle(
        self, metric_fn_bundle
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """Resolve a bundle of metrics with the same compute Domain as part of a single trip to the compute engine."""
        raise NotImplementedError

    @public_api
    def get_domain_records(
        self,
        domain_kwargs: dict,
    ) -> Any:
        """get_domain_records() is an interface method, which computes the full-access data (dataframe or selectable) for computing metrics based on the given domain_kwargs and specific engine semantics.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the Domain kwargs specifying which data to obtain

        Returns:
            data corresponding to the compute domain
        """

        raise NotImplementedError

    @public_api
    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[Any, dict, dict]:
        """get_compute_domain() is an interface method, which computes the optimal domain_kwargs for computing metrics based on the given domain_kwargs and specific engine semantics.

        Args:
            domain_kwargs (dict): a dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type (str or MetricDomainTypes): an Enum value indicating which metric Domain the user would like \
            to be using, or a corresponding string value representing it.  String types include "column", \
            "column_pair", "table", and "other".  Enum types include capitalized versions of these from the class \
            MetricDomainTypes.
            accessor_keys (str iterable): keys that are part of the compute Domain but should be ignored when \
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            A tuple consisting of three elements:

            1. data corresponding to the compute domain;
            2. a modified copy of domain_kwargs describing the Domain of the data returned in (1);
            3. a dictionary describing the access instructions for data elements included in the compute domain \
                (e.g. specific column name).

            In general, the union of the compute_domain_kwargs and accessor_domain_kwargs will be the same as the
            domain_kwargs provided to this method.
        """

        raise NotImplementedError

    def add_column_row_condition(
        self, domain_kwargs, column_name=None, filter_null=True, filter_nan=False
    ):
        """EXPERIMENTAL

        Add a row condition for handling null filter.

        Args:
            domain_kwargs: the Domain kwargs to use as the base and to which to add the condition
            column_name: if provided, use this name to add the condition; otherwise, will use "column" key from
                table_domain_kwargs
            filter_null: if true, add a filter for null values
            filter_nan: if true, add a filter for nan values
        """
        if filter_null is False and filter_nan is False:
            logger.warning(
                "add_column_row_condition called with no filter condition requested"
            )
            return domain_kwargs

        if filter_nan:
            raise gx_exceptions.GreatExpectationsError(
                "Base ExecutionEngine does not support adding nan condition filters"
            )

        new_domain_kwargs = copy.deepcopy(domain_kwargs)
        assert (
            "column" in domain_kwargs or column_name is not None
        ), "No column provided: A column must be provided in domain_kwargs or in the column_name parameter"
        if column_name is not None:
            column = column_name
        else:
            column = domain_kwargs["column"]

        row_condition = RowCondition(
            condition=f'col("{column}").notnull()',
            condition_type=RowConditionParserType.GE,
        )
        new_domain_kwargs.setdefault("filter_conditions", []).append(row_condition)
        return new_domain_kwargs

    def _build_direct_and_bundled_metric_computation_configurations(
        self,
        metrics_to_resolve: Iterable[MetricConfiguration],
        metrics: Optional[Dict[Tuple[str, str, str], MetricValue]] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Tuple[
        List[MetricComputationConfiguration],
        List[MetricComputationConfiguration],
    ]:
        """
        This method organizes "metrics_to_resolve" ("MetricConfiguration" objects) into two lists: direct and bundled.
        Directly-computable "MetricConfiguration" must have non-NULL metric function ("metric_fn").  Aggregate metrics
        have NULL metric function, but non-NULL partial metric function ("metric_partial_fn"); aggregates are bundled.

        See documentation in "MetricProvider._register_metric_functions()" for in-depth description of this mechanism.

        Args:
            metrics_to_resolve: the metrics to evaluate
            metrics: already-computed metrics currently available to the engine
            runtime_configuration: runtime configuration information

        Returns:
            Tuple with two elements: directly-computable and bundled "MetricComputationConfiguration" objects
        """
        metric_fn_direct_configurations: List[MetricComputationConfiguration] = []
        metric_fn_bundle_configurations: List[MetricComputationConfiguration] = []

        if not metrics_to_resolve:
            return (
                metric_fn_direct_configurations,
                metric_fn_bundle_configurations,
            )

        if metrics is None:
            metrics = {}

        resolved_metric_dependencies_by_metric_name: Dict[
            str, Union[MetricValue, Tuple[Any, dict, dict]]
        ]
        metric_class: MetricProvider
        metric_fn: Union[Callable, None]
        metric_aggregate_fn: sa.func | F  # type: ignore[valid-type]
        metric_provider_kwargs: dict
        compute_domain_kwargs: dict
        accessor_domain_kwargs: dict
        metric_to_resolve: MetricConfiguration
        for metric_to_resolve in metrics_to_resolve:
            resolved_metric_dependencies_by_metric_name = (
                self._get_computed_metric_evaluation_dependencies_by_metric_name(
                    metric_to_resolve=metric_to_resolve,
                    metrics=metrics,
                )
            )
            metric_class, metric_fn = get_metric_provider(
                metric_name=metric_to_resolve.metric_name, execution_engine=self
            )
            metric_provider_kwargs = {
                "cls": metric_class,
                "execution_engine": self,
                "metric_domain_kwargs": metric_to_resolve.metric_domain_kwargs,
                "metric_value_kwargs": metric_to_resolve.metric_value_kwargs,
                "metrics": resolved_metric_dependencies_by_metric_name,
                "runtime_configuration": runtime_configuration,
            }
            if metric_fn is None:
                try:
                    (
                        metric_aggregate_fn,
                        compute_domain_kwargs,
                        accessor_domain_kwargs,
                    ) = resolved_metric_dependencies_by_metric_name.pop(
                        "metric_partial_fn"
                    )
                except KeyError as e:
                    raise gx_exceptions.MetricError(
                        message=f'Missing metric dependency: {str(e)} for metric "{metric_to_resolve.metric_name}".'
                    )

                metric_fn_bundle_configurations.append(
                    MetricComputationConfiguration(
                        metric_configuration=metric_to_resolve,
                        metric_fn=metric_aggregate_fn,
                        metric_provider_kwargs=metric_provider_kwargs,
                        compute_domain_kwargs=compute_domain_kwargs,
                        accessor_domain_kwargs=accessor_domain_kwargs,
                    )
                )
            else:
                metric_fn_direct_configurations.append(
                    MetricComputationConfiguration(
                        metric_configuration=metric_to_resolve,
                        metric_fn=metric_fn,
                        metric_provider_kwargs=metric_provider_kwargs,
                    )
                )

        return (
            metric_fn_direct_configurations,
            metric_fn_bundle_configurations,
        )

    def _get_computed_metric_evaluation_dependencies_by_metric_name(
        self,
        metric_to_resolve: MetricConfiguration,
        metrics: Dict[Tuple[str, str, str], MetricValue],
    ) -> Dict[str, Union[MetricValue, Tuple[Any, dict, dict]]]:
        """
        Gathers resolved (already computed) evaluation dependencies of metric-to-resolve (not yet computed)
        "MetricConfiguration" object by "metric_name" property of resolved "MetricConfiguration" objects.

        Args:
            metric_to_resolve: dependent (not yet resolved) "MetricConfiguration" object
            metrics: resolved (already computed) "MetricConfiguration" objects keyd by ID of that object

        Returns:
            Dictionary keyed by "metric_name" with values as computed metric or partial bundling information tuple
        """
        metric_dependencies_by_metric_name: Dict[
            str, Union[MetricValue, Tuple[Any, dict, dict]]
        ] = {}

        metric_name: str
        metric_configuration: MetricConfiguration
        for (
            metric_name,
            metric_configuration,
        ) in metric_to_resolve.metric_dependencies.items():
            if metric_configuration.id in metrics:
                metric_dependencies_by_metric_name[metric_name] = metrics[
                    metric_configuration.id
                ]
            elif self._caching and metric_configuration.id in self._metric_cache:  # type: ignore[operator] # TODO: update NoOpDict
                metric_dependencies_by_metric_name[metric_name] = self._metric_cache[
                    metric_configuration.id
                ]
            else:
                raise gx_exceptions.MetricError(
                    message=f'Missing metric dependency: "{metric_name}" for metric "{metric_to_resolve.metric_name}".'
                )

        return metric_dependencies_by_metric_name

    def _process_direct_and_bundled_metric_computation_configurations(
        self,
        metric_fn_direct_configurations: List[MetricComputationConfiguration],
        metric_fn_bundle_configurations: List[MetricComputationConfiguration],
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """
        This method processes directly-computable and bundled "MetricComputationConfiguration" objects.

        Args:
            metric_fn_direct_configurations: directly-computable "MetricComputationConfiguration" objects
            metric_fn_bundle_configurations: bundled "MetricComputationConfiguration" objects (column aggregates)

        Returns:
            resolved_metrics (Dict): a dictionary with the values for the metrics that have just been resolved.
        """
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue] = {}

        metric_computation_configuration: MetricComputationConfiguration

        for metric_computation_configuration in metric_fn_direct_configurations:
            try:
                resolved_metrics[
                    metric_computation_configuration.metric_configuration.id
                ] = metric_computation_configuration.metric_fn(  # type: ignore[misc] # F not callable
                    **metric_computation_configuration.metric_provider_kwargs
                )
            except Exception as e:
                raise gx_exceptions.MetricResolutionError(
                    message=str(e),
                    failed_metrics=(
                        metric_computation_configuration.metric_configuration,
                    ),
                ) from e

        try:
            # an engine-specific way of computing metrics together
            resolved_metric_bundle: Dict[
                Tuple[str, str, str], MetricValue
            ] = self.resolve_metric_bundle(
                metric_fn_bundle=metric_fn_bundle_configurations
            )
            resolved_metrics.update(resolved_metric_bundle)
        except Exception as e:
            raise gx_exceptions.MetricResolutionError(
                message=str(e),
                failed_metrics=[
                    metric_computation_configuration.metric_configuration
                    for metric_computation_configuration in metric_fn_bundle_configurations
                ],
            ) from e

        if self._caching:
            self._metric_cache.update(resolved_metrics)

        return resolved_metrics

    def _split_domain_kwargs(
        self,
        domain_kwargs: Dict[str, Any],
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> SplitDomainKwargs:
        """Split domain_kwargs for all Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using, or a corresponding string value representing it. String types include "identity",
            "column", "column_pair", "table" and "other". Enum types include capitalized versions of these from the
            class MetricDomainTypes.
            accessor_keys: keys that are part of the compute Domain but should be ignored when
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """
        # Extracting value from enum if it is given for future computation
        domain_type = MetricDomainTypes(domain_type)

        # Warning user if accessor keys are in any Domain that is not of type table, will be ignored
        if (
            domain_type != MetricDomainTypes.TABLE
            and accessor_keys is not None
            and len(list(accessor_keys)) > 0
        ):
            logger.warning(
                'Accessor keys ignored since Metric Domain Type is not "table"'
            )

        split_domain_kwargs: SplitDomainKwargs
        if domain_type == MetricDomainTypes.TABLE:
            split_domain_kwargs = self._split_table_metric_domain_kwargs(
                domain_kwargs, domain_type, accessor_keys
            )

        elif domain_type == MetricDomainTypes.COLUMN:
            split_domain_kwargs = self._split_column_metric_domain_kwargs(
                domain_kwargs,
                domain_type,
            )

        elif domain_type == MetricDomainTypes.COLUMN_PAIR:
            split_domain_kwargs = self._split_column_pair_metric_domain_kwargs(
                domain_kwargs,
                domain_type,
            )

        elif domain_type == MetricDomainTypes.MULTICOLUMN:
            split_domain_kwargs = self._split_multi_column_metric_domain_kwargs(
                domain_kwargs,
                domain_type,
            )
        else:
            compute_domain_kwargs = copy.deepcopy(domain_kwargs)
            accessor_domain_kwargs: Dict[str, Any] = {}
            split_domain_kwargs = SplitDomainKwargs(
                compute_domain_kwargs, accessor_domain_kwargs
            )

        return split_domain_kwargs

    @staticmethod
    def _split_table_metric_domain_kwargs(
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> SplitDomainKwargs:
        """Split domain_kwargs for table Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.
            accessor_keys: keys that are part of the compute Domain but should be ignored when
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """
        assert (
            domain_type == MetricDomainTypes.TABLE
        ), "This method only supports MetricDomainTypes.TABLE"

        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}

        if accessor_keys is not None and len(list(accessor_keys)) > 0:
            for key in accessor_keys:
                accessor_domain_kwargs[key] = compute_domain_kwargs.pop(key)
        if len(domain_kwargs.keys()) > 0:
            # Warn user if kwarg not "normal".
            unexpected_keys: set = set(compute_domain_kwargs.keys()).difference(
                {
                    "batch_id",
                    "table",
                    "row_condition",
                    "condition_parser",
                }
            )
            if len(unexpected_keys) > 0:
                unexpected_keys_str: str = ", ".join(
                    map(lambda element: f'"{element}"', unexpected_keys)
                )
                logger.warning(
                    f"""Unexpected key(s) {unexpected_keys_str} found in domain_kwargs for Domain type "{domain_type.value}"."""
                )

        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_column_metric_domain_kwargs(
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> SplitDomainKwargs:
        """Split domain_kwargs for column Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """
        assert (
            domain_type == MetricDomainTypes.COLUMN
        ), "This method only supports MetricDomainTypes.COLUMN"

        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}

        if "column" not in compute_domain_kwargs:
            raise gx_exceptions.GreatExpectationsError(
                "Column not provided in compute_domain_kwargs"
            )

        accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_column_pair_metric_domain_kwargs(
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> SplitDomainKwargs:
        """Split domain_kwargs for column pair Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """
        assert (
            domain_type == MetricDomainTypes.COLUMN_PAIR
        ), "This method only supports MetricDomainTypes.COLUMN_PAIR"

        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}

        if not ("column_A" in domain_kwargs and "column_B" in domain_kwargs):
            raise gx_exceptions.GreatExpectationsError(
                "column_A or column_B not found within domain_kwargs"
            )

        accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop("column_A")
        accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop("column_B")

        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @staticmethod
    def _split_multi_column_metric_domain_kwargs(
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> SplitDomainKwargs:
        """Split domain_kwargs for multicolumn Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """
        assert (
            domain_type == MetricDomainTypes.MULTICOLUMN
        ), "This method only supports MetricDomainTypes.MULTICOLUMN"

        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}

        if "column_list" not in domain_kwargs:
            raise gx_exceptions.GreatExpectationsError(
                "column_list not found within domain_kwargs"
            )

        column_list = compute_domain_kwargs.pop("column_list")

        if len(column_list) < 2:  # noqa: PLR2004
            raise gx_exceptions.GreatExpectationsError(
                "column_list must contain at least 2 columns"
            )

        accessor_domain_kwargs["column_list"] = column_list

        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)
