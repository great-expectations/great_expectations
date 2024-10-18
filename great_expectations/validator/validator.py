from __future__ import annotations

import copy
import datetime
import inspect
import itertools
import json
import logging
import traceback
import warnings
from collections import defaultdict
from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import pandas as pd
from marshmallow import ValidationError

from great_expectations import __version__ as ge_version
from great_expectations._docs_decorators import deprecated_argument
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.data_context.context_factory import project_manager
from great_expectations.data_context.types.base import CheckpointValidationDefinition
from great_expectations.exceptions import (
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from great_expectations.util import convert_to_json_serializable  # noqa: TID251
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metrics_calculator import (
    MetricsCalculator,
    _AbortedMetricsInfoDict,
    _MetricsDict,
)
from great_expectations.validator.util import recursively_convert_to_json_serializable
from great_expectations.validator.validation_graph import (
    ExpectationValidationGraph,
    MetricEdge,
    ValidationGraph,
)
from great_expectations.validator.validation_statistics import (
    calc_validation_statistics,
)

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


if TYPE_CHECKING:
    from great_expectations.core.batch import (
        AnyBatch,
        Batch,
        BatchDataUnion,
        BatchMarkers,
        LegacyBatchDefinition,
    )
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.data_context.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import Batch as FluentBatch
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.validator.metric_configuration import MetricConfiguration


@dataclass
class ValidationDependencies:
    # Note: Dependent "metric_name" (key) is different from "metric_name" in dependency "MetricConfiguration" (value).  # noqa: E501
    metric_configurations: Dict[str, MetricConfiguration] = field(default_factory=dict)
    result_format: Dict[str, Any] = field(default_factory=dict)

    def set_metric_configuration(
        self, metric_name: str, metric_configuration: MetricConfiguration
    ) -> None:
        """
        Sets specified "MetricConfiguration" for "metric_name" to "metric_configurations" dependencies dictionary.
        """  # noqa: E501
        self.metric_configurations[metric_name] = metric_configuration

    def get_metric_configuration(self, metric_name: str) -> Optional[MetricConfiguration]:
        """
        Obtains "MetricConfiguration" for specified "metric_name" from "metric_configurations" dependencies dictionary.
        """  # noqa: E501
        return self.metric_configurations.get(metric_name)

    def remove_metric_configuration(self, metric_name: str) -> None:
        """
        Removes "MetricConfiguration" for specified "metric_name" from "metric_configurations" dependencies dictionary.
        """  # noqa: E501
        del self.metric_configurations[metric_name]

    def get_metric_names(self) -> List[str]:
        """
        Returns "metric_name" keys, for which "MetricConfiguration" dependency objects have been specified.
        """  # noqa: E501
        return list(self.metric_configurations.keys())

    def get_metric_configurations(self) -> List[MetricConfiguration]:
        """
        Returns "MetricConfiguration" dependency objects specified.
        """
        return list(self.metric_configurations.values())


class Validator:
    """Validator is the key object used to create Expectations, validate Expectations, and get Metrics for Expectations.

    Validators are used by Checkpoints to validate Expectations.

    Args:
        execution_engine: The Execution Engine to be used to perform validation.
        interactive_evaluation: If True, the Validator will perform evaluation when Expectations are added.
        expectation_suite: The Expectation Suite to validate.
        expectation_suite_name: The name of the Expectation Suite to validate.
        data_context: The Data Context associated with this Validator.
        batches: The Batches for which to validate.
    """  # noqa: E501

    DEFAULT_RUNTIME_CONFIGURATION = {
        "catch_exceptions": False,
        "result_format": "BASIC",
    }
    RUNTIME_KEYS = DEFAULT_RUNTIME_CONFIGURATION.keys()

    # noinspection PyUnusedLocal
    def __init__(  # noqa: PLR0913
        self,
        execution_engine: ExecutionEngine,
        interactive_evaluation: bool = True,
        expectation_suite: ExpectationSuite | None = None,
        expectation_suite_name: Optional[str] = None,
        data_context: Optional[AbstractDataContext] = None,
        batches: List[Batch] | Sequence[Batch | FluentBatch] = tuple(),
        **kwargs,
    ) -> None:
        self._data_context: Optional[AbstractDataContext] = data_context

        self._metrics_calculator: MetricsCalculator = MetricsCalculator(
            execution_engine=execution_engine,
            show_progress_bars=self._determine_progress_bars(),
        )
        execution_engine.batch_manager.reset_batch_cache()
        self._execution_engine: ExecutionEngine = execution_engine

        if batches:
            self.load_batch_list(batch_list=batches)

        self._expose_dataframe_methods: bool = False

        self.interactive_evaluation: bool = interactive_evaluation
        self._initialize_expectations(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
        )
        self._default_expectation_args: Dict[str, Union[bool, str]] = copy.deepcopy(
            Validator.DEFAULT_RUNTIME_CONFIGURATION  # type: ignore[arg-type]
        )

        # This special state variable tracks whether a validation run is going on, which will disable  # noqa: E501
        # saving expectation config objects
        self._active_validation: bool = False

    @property
    def _include_rendered_content(self) -> bool:
        return project_manager.is_using_cloud()

    @property
    def execution_engine(self) -> ExecutionEngine:
        """Returns the execution engine being used by the validator at the given time"""
        return self._execution_engine

    @property
    def metrics_calculator(self) -> MetricsCalculator:
        """Returns the "MetricsCalculator" object being used by the Validator to handle metrics computations."""  # noqa: E501
        return self._metrics_calculator

    @property
    def data_context(self) -> Optional[AbstractDataContext]:
        """Reference to DataContext object handle."""
        return self._data_context

    @property
    def expose_dataframe_methods(self) -> bool:
        """The "expose_dataframe_methods" getter property."""
        return self._expose_dataframe_methods

    @expose_dataframe_methods.setter
    def expose_dataframe_methods(self, value: bool) -> None:
        """The "expose_dataframe_methods" setter property."""
        self._expose_dataframe_methods = value

    @property
    def loaded_batch_ids(self) -> List[str]:
        """Getter for IDs of loaded Batch objects (convenience property)"""
        return self._execution_engine.batch_manager.loaded_batch_ids

    @property
    def active_batch_data(self) -> Optional[BatchDataUnion]:
        """Getter for BatchData object from the currently-active Batch object (convenience property)."""  # noqa: E501
        return self._execution_engine.batch_manager.active_batch_data

    @property
    def batch_cache(self) -> Dict[str, AnyBatch]:
        """Getter for dictionary of Batch objects (convenience property)"""
        return self._execution_engine.batch_manager.batch_cache

    @property
    def batches(self) -> Dict[str, AnyBatch]:
        """Getter for dictionary of Batch objects (alias convenience property, to be deprecated)"""
        return self.batch_cache

    @property
    def active_batch_id(self) -> Optional[str]:
        """Getter for batch_id of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_id

    @property
    def active_batch(self) -> Optional[AnyBatch]:
        """Getter for active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch

    @property
    def active_batch_spec(self) -> Optional[BatchSpec]:
        """Getter for batch_spec of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_spec

    @property
    def active_batch_markers(self) -> Optional[BatchMarkers]:
        """Getter for batch_markers of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_markers

    @property
    def active_batch_definition(self) -> Optional[LegacyBatchDefinition]:
        """Getter for batch_definition of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_definition

    @property
    def expectation_suite(self) -> ExpectationSuite:
        return self._expectation_suite

    @expectation_suite.setter
    def expectation_suite(self, value: ExpectationSuite) -> None:
        self._initialize_expectations(
            expectation_suite=value,
            expectation_suite_name=value.name,
        )

    @property
    def expectation_suite_name(self) -> str:
        """Gets the current expectation_suite name of this data_asset as stored in the expectations configuration."""  # noqa: E501
        return self._expectation_suite.name

    @expectation_suite_name.setter
    def expectation_suite_name(self, name: str) -> None:
        """Sets the expectation_suite name of this data_asset as stored in the expectations configuration."""  # noqa: E501
        self._expectation_suite.name = name

    def load_batch_list(self, batch_list: Sequence[Batch | FluentBatch]) -> None:
        self._execution_engine.batch_manager.load_batch_list(batch_list=batch_list)

    def get_metric(
        self,
        metric: MetricConfiguration,
    ) -> Any:
        """Convenience method, return the value of the requested metric.

        Args:
            metric: MetricConfiguration

        Returns:
            The value of the requested metric.
        """
        return self._metrics_calculator.get_metric(metric=metric)

    def get_metrics(
        self,
        metrics: Dict[str, MetricConfiguration],
    ) -> Dict[str, Any]:
        """
        Convenience method that resolves requested metrics (specified as dictionary, keyed by MetricConfiguration ID).

        Args:
            metrics: Dictionary of desired metrics to be resolved; metric_name is key and MetricConfiguration is value.

        Returns:
            Return Dictionary with requested metrics resolved, with metric_name as key and computed metric as value.
        """  # noqa: E501
        return self._metrics_calculator.get_metrics(metrics=metrics)

    def compute_metrics(
        self,
        metric_configurations: List[MetricConfiguration],
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,
        # Set to low number (e.g., 3) to suppress progress bar for small graphs.
    ) -> tuple[_MetricsDict, _AbortedMetricsInfoDict]:
        """
        Convenience method that computes requested metrics (specified as elements of "MetricConfiguration" list).

        Args:
            metric_configurations: List of desired MetricConfiguration objects to be resolved.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            min_graph_edges_pbar_enable: Minumum number of graph edges to warrant showing progress bars.

        Returns:
            Tuple of two elements, the first is a dictionary with requested metrics resolved,
            with unique metric ID as key and computed metric as value. The second is a dictionary of the
            aborted metrics information, with metric ID as key if any metrics were aborted.
        """  # noqa: E501
        return self._metrics_calculator.compute_metrics(
            metric_configurations=metric_configurations,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=min_graph_edges_pbar_enable,
        )

    def columns(self, domain_kwargs: Optional[Dict[str, Any]] = None) -> List[str]:
        """Convenience method to obtain Batch columns.

        Arguments:
            domain_kwargs: Optional dictionary of domain kwargs (e.g., containing "batch_id").

        Returns:
            The list of Batch columns.
        """
        return self._metrics_calculator.columns(domain_kwargs=domain_kwargs)

    def head(
        self,
        n_rows: int = 5,
        domain_kwargs: Optional[Dict[str, Any]] = None,
        fetch_all: bool = False,
    ) -> pd.DataFrame:
        """Convenience method to return the first several rows or records from a Batch of data.

        Args:
            n_rows: The number of rows to return.
            domain_kwargs: If provided, the domain for which to return records.
            fetch_all: If True, ignore n_rows and return the entire batch.

        Returns:
            A Pandas DataFrame containing the records' data.
        """
        return self._metrics_calculator.head(
            n_rows=n_rows, domain_kwargs=domain_kwargs, fetch_all=fetch_all
        )

    @override
    def __dir__(self) -> List[str]:
        """
        This custom magic method is used to enable expectation tab completion on Validator objects.
        It also allows users to call Pandas.DataFrame methods on Validator objects
        """
        validator_attrs = set(super().__dir__())
        class_expectation_impls = set(list_registered_expectation_implementations())
        # execution_engine_expectation_impls = (
        #     {
        #         attr_name
        #         for attr_name in self.execution_engine.__dir__()
        #         if attr_name.startswith("expect_")
        #     }
        #     if self.execution_engine
        #     else set()
        # )

        combined_dir = (
            validator_attrs | class_expectation_impls
            # | execution_engine_expectation_impls
        )

        if self._expose_dataframe_methods:
            combined_dir | set(dir(pd.DataFrame))

        return list(combined_dir)

    def _determine_progress_bars(self) -> bool:
        enable: bool = True
        if self._data_context:
            progress_bars = self._data_context.progress_bars
            # If progress_bars are not present, assume we want them enabled
            if progress_bars is not None:
                if "globally" in progress_bars:
                    enable = bool(progress_bars["globally"])
                if "metric_calculations" in progress_bars:
                    enable = bool(progress_bars["metric_calculations"])

        return enable

    def __getattr__(self, name):
        if self.active_batch is None:
            raise TypeError("active_batch cannot be None")  # noqa: TRY003
        name = name.lower()
        if (
            name.startswith("expect_") or name == "unexpected_rows_expectation"
        ) and get_expectation_impl(name):
            return self.validate_expectation(name)
        elif (
            self._expose_dataframe_methods
            and isinstance(self.active_batch.data, PandasBatchData)
            and hasattr(pd.DataFrame, name)
        ):
            return getattr(self.active_batch.data.dataframe, name)
        else:
            raise AttributeError(f"'{type(self).__name__}'  object has no attribute '{name}'")  # noqa: TRY003

    def validate_expectation(self, name: str) -> Callable:  # noqa: C901, PLR0915
        """
        Given the name of an Expectation, obtains the Class-first Expectation implementation and utilizes the
                expectation's validate method to obtain a validation result. Also adds in the runtime configuration

                        Args:
                            name (str): The name of the Expectation being validated

                        Returns:
                            The Expectation's validation result
        """  # noqa: E501
        expectation_impl = get_expectation_impl(name)

        def inst_expectation(*args: dict, **kwargs):  # noqa: C901, PLR0912
            # this is used so that exceptions are caught appropriately when they occur in expectation config  # noqa: E501

            # TODO: JPC - THIS LOGIC DOES NOT RESPECT DEFAULTS SET BY USERS IN THE VALIDATOR VS IN THE EXPECTATION  # noqa: E501
            # DEVREL has action to develop a new plan in coordination with MarioPod

            expectation_kwargs = recursively_convert_to_json_serializable(kwargs)

            meta: Optional[dict] = expectation_kwargs.pop("meta", None)

            basic_default_expectation_args: dict = {
                k: v
                for k, v in self.default_expectation_args.items()
                if k in Validator.RUNTIME_KEYS
            }
            basic_runtime_configuration: dict = copy.deepcopy(basic_default_expectation_args)
            basic_runtime_configuration.update(
                {k: v for k, v in kwargs.items() if k in Validator.RUNTIME_KEYS}
            )

            allowed_config_keys: Tuple[str, ...] = expectation_impl.get_allowed_config_keys()

            args_keys: Tuple[str, ...] = expectation_impl.args_keys or tuple()

            arg_name: str

            idx: int
            arg: dict
            for idx, arg in enumerate(args):
                try:
                    arg_name = args_keys[idx]
                    if arg_name in allowed_config_keys:
                        expectation_kwargs[arg_name] = arg
                    if arg_name == "meta":
                        logger.warning(
                            "Setting meta via args could be ambiguous; please use a kwarg instead."
                        )
                        meta = arg
                except IndexError:
                    raise InvalidExpectationConfigurationError(  # noqa: TRY003
                        f"Invalid positional argument: {arg}"
                    )

            configuration: ExpectationConfiguration | None = None

            try:
                expectation = expectation_impl(**expectation_kwargs, meta=meta)
                configuration = expectation.configuration

                if self.interactive_evaluation:
                    configuration.process_suite_parameters(
                        self._expectation_suite.suite_parameters,
                        True,
                        self._data_context,
                    )

                """Given an implementation and a configuration for any Expectation, returns its validation result"""  # noqa: E501

                if not self.interactive_evaluation and not self._active_validation:
                    validation_result = ExpectationValidationResult(
                        expectation_config=copy.deepcopy(configuration)
                    )
                else:
                    validation_result = expectation.validate_(
                        validator=self,
                        suite_parameters=self._expectation_suite.suite_parameters,
                        data_context=self._data_context,
                        runtime_configuration=basic_runtime_configuration,
                    )

                # If validate has set active_validation to true, then we do not save the config to avoid  # noqa: E501
                # saving updating expectation configs to the same suite during validation runs
                if self._active_validation is True:
                    stored_config = configuration.get_raw_configuration()
                else:
                    # Append the expectation to the config.
                    stored_config = self._expectation_suite._add_expectation(
                        expectation_configuration=configuration.get_raw_configuration(),
                    )

                # If there was no interactive evaluation, success will not have been computed.
                if validation_result.success is not None:
                    # Add a "success" object to the config
                    stored_config.success_on_last_run = validation_result.success

            except Exception as err:
                if basic_runtime_configuration.get("catch_exceptions"):
                    exception_traceback = traceback.format_exc()
                    exception_message = f"{type(err).__name__}: {err!s}"
                    exception_info = ExceptionInfo(
                        exception_traceback=exception_traceback,
                        exception_message=exception_message,
                    )

                    if not configuration:
                        configuration = ExpectationConfiguration(
                            type=name, kwargs=expectation_kwargs, meta=meta
                        )

                    validation_result = ExpectationValidationResult(
                        success=False,
                        exception_info=exception_info,
                        expectation_config=configuration,
                    )
                else:
                    raise err  # noqa: TRY201

            if self._include_rendered_content:
                validation_result.render()

            return validation_result

        inst_expectation.__name__ = name
        inst_expectation.__doc__ = expectation_impl.__doc__

        return inst_expectation

    def list_available_expectation_types(self) -> List[str]:
        """Returns a list of all expectations available to the validator"""
        keys = dir(self)
        return [expectation for expectation in keys if expectation.startswith("expect_")]

    def graph_validate(
        self,
        configurations: List[ExpectationConfiguration],
        runtime_configuration: Optional[dict] = None,
    ) -> List[ExpectationValidationResult]:
        """Obtains validation dependencies for each metric using the implementation of their
        associated expectation, then proceeds to add these dependencies to the validation graph,
        supply readily available metric implementations to fulfill current metric requirements,
        and validate these metrics.

        Args:
            configurations(List[ExpectationConfiguration]): A list of needed Expectation
            Configurations that will be used to supply domain and values for metrics.
            runtime_configuration (dict): A dictionary of runtime keyword arguments, controlling
            semantics, such as the result_format.

        Returns:
            A list of Validations, validating that all necessary metrics are available.
        """
        if runtime_configuration is None:
            runtime_configuration = {}

        if runtime_configuration.get("catch_exceptions", True):
            catch_exceptions = True
        else:
            catch_exceptions = False

        expectation_validation_graphs: List[ExpectationValidationGraph]

        evrs: List[ExpectationValidationResult]

        processed_configurations: List[ExpectationConfiguration] = []

        (
            expectation_validation_graphs,
            evrs,
            processed_configurations,
        ) = self._generate_metric_dependency_subgraphs_for_each_expectation_configuration(
            expectation_configurations=configurations,
            processed_configurations=processed_configurations,
            catch_exceptions=catch_exceptions,
            runtime_configuration=runtime_configuration,
        )

        graph: ValidationGraph = self._generate_suite_level_graph_from_expectation_level_sub_graphs(
            expectation_validation_graphs=expectation_validation_graphs
        )

        resolved_metrics: _MetricsDict

        try:
            (
                resolved_metrics,
                evrs,
                processed_configurations,
            ) = self._resolve_suite_level_graph_and_process_metric_evaluation_errors(
                graph=graph,
                runtime_configuration=runtime_configuration,
                expectation_validation_graphs=expectation_validation_graphs,
                evrs=evrs,
                processed_configurations=processed_configurations,
                show_progress_bars=self._determine_progress_bars(),
            )
        except Exception as err:
            # If a general Exception occurs during the execution of "ValidationGraph.resolve()", then  # noqa: E501
            # all expectations in the suite are impacted, because it is impossible to attribute the failure to a metric.  # noqa: E501
            if catch_exceptions:
                exception_traceback: str = traceback.format_exc()
                evrs = self._catch_exceptions_in_failing_expectation_validations(
                    exception_traceback=exception_traceback,
                    exception=err,
                    failing_expectation_configurations=processed_configurations,
                    evrs=evrs,
                )
                return evrs
            else:
                raise err  # noqa: TRY201

        configuration: ExpectationConfiguration
        result: ExpectationValidationResult
        for configuration in processed_configurations:
            try:
                runtime_configuration_default = copy.deepcopy(runtime_configuration)

                expectation = configuration.to_domain_obj()
                result = expectation.metrics_validate(
                    metrics=resolved_metrics,
                    execution_engine=self._execution_engine,
                    runtime_configuration=runtime_configuration_default,
                )
                evrs.append(result)
            except Exception as err:
                if catch_exceptions:
                    exception_traceback = traceback.format_exc()
                    evrs = self._catch_exceptions_in_failing_expectation_validations(
                        exception_traceback=exception_traceback,
                        exception=err,
                        failing_expectation_configurations=[configuration],
                        evrs=evrs,
                    )
                else:
                    raise err  # noqa: TRY201

        return evrs

    def _generate_metric_dependency_subgraphs_for_each_expectation_configuration(
        self,
        expectation_configurations: List[ExpectationConfiguration],
        processed_configurations: List[ExpectationConfiguration],
        catch_exceptions: bool,
        runtime_configuration: Optional[dict] = None,
    ) -> Tuple[
        List[ExpectationValidationGraph],
        List[ExpectationValidationResult],
        List[ExpectationConfiguration],
    ]:
        # While evaluating expectation configurations, create sub-graph for every metric dependency and incorporate  # noqa: E501
        # these sub-graphs under corresponding expectation-level sub-graph (state of ExpectationValidationGraph object).  # noqa: E501
        expectation_validation_graphs: List[ExpectationValidationGraph] = []
        evrs: List[ExpectationValidationResult] = []
        configuration: ExpectationConfiguration
        evaluated_config: ExpectationConfiguration
        metric_configuration: MetricConfiguration
        graph: ValidationGraph
        for configuration in expectation_configurations:
            # Validating
            try:
                assert (
                    configuration.type is not None
                ), "Given configuration should include expectation type"
            except AssertionError as e:
                raise InvalidExpectationConfigurationError(str(e))

            evaluated_config = copy.deepcopy(configuration)

            if self.active_batch_id:
                evaluated_config.kwargs.update({"batch_id": self.active_batch_id})

            expectation = evaluated_config.to_domain_obj()
            validation_dependencies: ValidationDependencies = (
                expectation.get_validation_dependencies(
                    execution_engine=self._execution_engine,
                    runtime_configuration=runtime_configuration,
                )
            )

            try:
                expectation_validation_graph: ExpectationValidationGraph = ExpectationValidationGraph(  # noqa: E501
                    configuration=evaluated_config,
                    graph=self._metrics_calculator.build_metric_dependency_graph(
                        metric_configurations=validation_dependencies.get_metric_configurations(),
                        runtime_configuration=runtime_configuration,
                    ),
                )
                expectation_validation_graphs.append(expectation_validation_graph)
                processed_configurations.append(evaluated_config)
            except Exception as err:
                if catch_exceptions:
                    exception_traceback: str = traceback.format_exc()
                    exception_message: str = str(err)
                    exception_info = ExceptionInfo(
                        exception_traceback=exception_traceback,
                        exception_message=exception_message,
                    )
                    result = ExpectationValidationResult(
                        success=False,
                        exception_info=exception_info,
                        expectation_config=evaluated_config,
                    )
                    evrs.append(result)
                else:
                    raise err  # noqa: TRY201

        return expectation_validation_graphs, evrs, processed_configurations

    def _generate_suite_level_graph_from_expectation_level_sub_graphs(
        self,
        expectation_validation_graphs: List[ExpectationValidationGraph],
    ) -> ValidationGraph:
        # Collect edges from all expectation-level sub-graphs and incorporate them under common suite-level graph.  # noqa: E501
        expectation_validation_graph: ExpectationValidationGraph
        edges: List[MetricEdge] = list(
            itertools.chain.from_iterable(
                [
                    expectation_validation_graph.graph.edges
                    for expectation_validation_graph in expectation_validation_graphs
                ]
            )
        )
        validation_graph = ValidationGraph(execution_engine=self._execution_engine, edges=edges)
        return validation_graph

    def _resolve_suite_level_graph_and_process_metric_evaluation_errors(  # noqa: PLR0913
        self,
        graph: ValidationGraph,
        runtime_configuration: dict,
        expectation_validation_graphs: List[ExpectationValidationGraph],
        evrs: List[ExpectationValidationResult],
        processed_configurations: List[ExpectationConfiguration],
        show_progress_bars: bool,
    ) -> Tuple[
        _MetricsDict,
        List[ExpectationValidationResult],
        List[ExpectationConfiguration],
    ]:
        # Resolve overall suite-level graph and process any MetricResolutionError type exceptions that might occur.  # noqa: E501
        resolved_metrics: _MetricsDict
        aborted_metrics_info: _AbortedMetricsInfoDict
        (
            resolved_metrics,
            aborted_metrics_info,
        ) = self._metrics_calculator.resolve_validation_graph(
            graph=graph,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=0,
        )

        # Trace MetricResolutionError occurrences to expectations relying on corresponding malfunctioning metrics.  # noqa: E501
        rejected_configurations: List[ExpectationConfiguration] = []
        for expectation_validation_graph in expectation_validation_graphs:
            metric_exception_info: Dict[str, Union[MetricConfiguration, ExceptionInfo, int]] = (
                expectation_validation_graph.get_exception_info(metric_info=aborted_metrics_info)
            )
            # Report all MetricResolutionError occurrences impacting expectation and append it to rejected list.  # noqa: E501
            if len(metric_exception_info) > 0:
                configuration = expectation_validation_graph.configuration
                result = ExpectationValidationResult(
                    success=False,
                    exception_info=metric_exception_info,
                    expectation_config=configuration,
                )
                evrs.append(result)

                if configuration not in rejected_configurations:
                    rejected_configurations.append(configuration)

        # Exclude all rejected expectations from list of expectations cleared for validation.
        for configuration in rejected_configurations:
            processed_configurations.remove(configuration)

        return resolved_metrics, evrs, processed_configurations

    @staticmethod
    def _catch_exceptions_in_failing_expectation_validations(
        exception_traceback: str,
        exception: Exception,
        failing_expectation_configurations: List[ExpectationConfiguration],
        evrs: List[ExpectationValidationResult],
    ) -> List[ExpectationValidationResult]:
        """
        Catch exceptions in failing Expectation validations and convert to unsuccessful ExpectationValidationResult
        Args:
            exception_traceback: Traceback related to raised Exception
            exception: Exception raised
            failing_expectation_configurations: ExpectationConfigurations that failed
            evrs: List of ExpectationValidationResult objects to append failures to

        Returns:
            List of ExpectationValidationResult objects with unsuccessful ExpectationValidationResult objects appended
        """  # noqa: E501
        exception_message: str = str(exception)
        exception_info = ExceptionInfo(
            exception_traceback=exception_traceback,
            exception_message=exception_message,
        )

        configuration: ExpectationConfiguration
        result: ExpectationValidationResult
        for configuration in failing_expectation_configurations:
            result = ExpectationValidationResult(
                success=False,
                exception_info=exception_info,
                expectation_config=configuration,
            )
            evrs.append(result)

        return evrs

    def remove_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
        id: Optional[str] = None,
    ) -> List[ExpectationConfiguration]:
        """Remove an ExpectationConfiguration from the ExpectationSuite associated with the Validator.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against.
            match_type: This determines what kwargs to use when matching. Options are:
                - 'domain' to match based on the data evaluated by that expectation
                - 'success' to match based on all configuration parameters that influence whether an expectation succeeds on a given batch of data
                - 'runtime' to match based on all configuration parameters.
            remove_multiple_matches: If True, will remove multiple matching expectations.
            id: Great Expectations Cloud id for an Expectation.

        Returns:
            The list of deleted ExpectationConfigurations.

        Raises:
            TypeError: Must provide either expectation_configuration or id.
            ValueError: No match or multiple matches found (and remove_multiple_matches=False).
        """  # noqa: E501

        return self._expectation_suite.remove_expectation(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            remove_multiple_matches=remove_multiple_matches,
            id=id,
        )

    def discard_failing_expectations(self) -> None:
        """Removes any expectations from the validator where the validation has failed"""
        res = self.validate(only_return_failures=True).results  # type: ignore[union-attr] # ExpectationValidationResult has no `.results` attr
        if any(res):
            for item in res:
                config = item.expectation_config
                if not config:
                    raise ValueError(  # noqa: TRY003
                        "ExpectationValidationResult does not have an expectation_config"
                    )
                self.remove_expectation(
                    expectation_configuration=config,
                    match_type="runtime",
                )
            warnings.warn(f"Removed {len(res)} expectations that were 'False'")

    def get_default_expectation_arguments(self) -> dict:
        """Fetch default expectation arguments for this data_asset

        Returns:
            A dictionary containing all the current default expectation arguments for a data_asset

            Ex::

                {
                    "catch_exceptions" : False,
                    "result_format" : 'BASIC'
                }

        See also:
            set_default_expectation_arguments
        """
        return self.default_expectation_args

    @property
    def cloud_mode(self) -> bool:
        """
        Wrapper around cloud_mode property of associated Data Context
        """
        from great_expectations.data_context.data_context.cloud_data_context import (
            CloudDataContext,
        )

        return isinstance(self._data_context, CloudDataContext)

    @property
    def ge_cloud_mode(self) -> bool:
        # <GE_RENAME> Deprecated 0.15.37
        return self.cloud_mode

    @property
    def default_expectation_args(self) -> dict:
        """A getter for default Expectation arguments"""
        return self._default_expectation_args

    def set_default_expectation_argument(self, argument: str, value) -> None:
        """
        Set a default expectation argument for this data_asset

        Args:
            argument (string): The argument to be replaced
            value : The New argument to use for replacement

        Returns:
            None

        See also:
            get_default_expectation_arguments
        """

        self._default_expectation_args[argument] = value

    def get_expectation_suite(  # noqa: C901, PLR0912, PLR0913
        self,
        discard_failed_expectations: bool = True,
        discard_result_format_kwargs: bool = True,
        discard_include_config_kwargs: bool = True,
        discard_catch_exceptions_kwargs: bool = True,
        suppress_warnings: bool = False,
        suppress_logging: bool = False,
    ) -> ExpectationSuite:
        """Get a copy of the Expectation Suite from the Validator object.

        Args:
            discard_failed_expectations: Omit Expectations which failed on their last run.
            discard_result_format_kwargs: Omit `result_format` from each Expectation.
            discard_include_config_kwargs: Omit `include_config` from each Expectation.
            discard_catch_exceptions_kwargs: Omit `catch_exceptions` from each Expectation.
            suppress_warnings: Do not log warnings.
            suppress_logging: Do not log anything.

        Returns:
            ExpectationSuite object.
        """

        expectation_suite = copy.deepcopy(self.expectation_suite)
        expectations = expectation_suite.expectation_configurations

        discards: defaultdict[str, int] = defaultdict(int)

        if discard_failed_expectations:
            new_expectations = []

            for expectation in expectations:
                # Note: This is conservative logic.
                # Instead of retaining expectations IFF success==True, it discard expectations IFF success==False.  # noqa: E501
                # In cases where expectation.success is missing or None, expectations are *retained*.  # noqa: E501
                # Such a case could occur if expectations were loaded from a config file and never run.  # noqa: E501
                if expectation.success_on_last_run is False:
                    discards["failed_expectations"] += 1
                else:
                    new_expectations.append(expectation)

            expectations = new_expectations

        message = f"\t{len(expectations)} expectation(s) included in expectation_suite."

        if discards["failed_expectations"] > 0 and not suppress_warnings:
            message += (
                f" Omitting {discards['failed_expectations']} expectation(s) that failed when last run; set "  # noqa: E501
                "discard_failed_expectations=False to include them."
            )

        for expectation in expectations:
            # FIXME: Factor this out into a new function. The logic is duplicated in remove_expectation,  # noqa: E501
            #  which calls _copy_and_clean_up_expectation
            expectation.success_on_last_run = None

            if discard_result_format_kwargs:
                if "result_format" in expectation.kwargs:
                    del expectation.kwargs["result_format"]
                    discards["result_format"] += 1

            if discard_include_config_kwargs:
                if "include_config" in expectation.kwargs:
                    del expectation.kwargs["include_config"]
                    discards["include_config"] += 1

            if discard_catch_exceptions_kwargs:
                if "catch_exceptions" in expectation.kwargs:
                    del expectation.kwargs["catch_exceptions"]
                    discards["catch_exceptions"] += 1

        settings_message = ""

        if discards["result_format"] > 0 and not suppress_warnings:
            settings_message += " result_format"

        if discards["include_config"] > 0 and not suppress_warnings:
            settings_message += " include_config"

        if discards["catch_exceptions"] > 0 and not suppress_warnings:
            settings_message += " catch_exceptions"

        if len(settings_message) > 1:  # Only add this if we added one of the settings above.
            settings_message += " settings filtered."

        expectation_suite.expectations = []
        expectation_suite.add_expectation_configurations(expectation_configurations=expectations)
        if not suppress_logging:
            logger.info(message + settings_message)
        return expectation_suite

    def save_expectation_suite(  # noqa: PLR0913
        self,
        filepath: Optional[str] = None,
        discard_failed_expectations: bool = True,
        discard_result_format_kwargs: bool = True,
        discard_include_config_kwargs: bool = True,
        discard_catch_exceptions_kwargs: bool = True,
        suppress_warnings: bool = False,
    ) -> None:
        """Write the Expectation Suite (e.g. from interactive evaluation) to the Expectation Store associated with the Validator's Data Context.

        If `filepath` is provided, the Data Context configuration will be ignored and the configuration will be written, as JSON, to the specified file.

        Args:
            filepath: The location and name to write the JSON config file to. This parameter overrides the Data Context configuration.
            discard_failed_expectations: If True, excludes expectations that do not return `success = True`. If False, all expectations are saved.
            discard_result_format_kwargs: If True, the `result_format` attribute for each expectation is not included in the saved configuration.
            discard_include_config_kwargs: If True, the `include_config` attribute for each expectation is not included in the saved configuration.
            discard_catch_exceptions_kwargs: If True, the `catch_exceptions` attribute for each expectation is not included in the saved configuration.
            suppress_warnings: If True, all warnings raised by Great Expectations, as a result of dropped expectations, are suppressed.

        Raises:
            ValueError: Must configure a Data Context when instantiating the Validator or pass in `filepath`.
        """  # noqa: E501
        expectation_suite: ExpectationSuite = self.get_expectation_suite(
            discard_failed_expectations,
            discard_result_format_kwargs,
            discard_include_config_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings,
        )
        if filepath is None and self._data_context is not None:
            self._data_context.suites.add(expectation_suite)
            if self.cloud_mode:
                updated_suite = self._data_context.suites.get(expectation_suite.name)
                self._initialize_expectations(expectation_suite=updated_suite)
        elif filepath is not None:
            with open(filepath, "w") as outfile:
                json.dump(
                    expectationSuiteSchema.dump(expectation_suite),
                    outfile,
                    indent=2,
                    sort_keys=True,
                )
        else:
            raise ValueError("Unable to save config: filepath or data_context must be available.")  # noqa: TRY003

    @deprecated_argument(
        argument_name="run_id",
        message="Only the str version of this argument is deprecated. run_id should be a RunIdentifier or dict. Support will be removed in 0.16.0.",  # noqa: E501
        version="0.13.0",
    )
    def validate(  # noqa: C901, PLR0912, PLR0913
        self,
        expectation_suite: str | ExpectationSuite | None = None,
        run_id: str | RunIdentifier | Dict[str, str] | None = None,
        data_context: Optional[Any] = None,  # Cannot type DataContext due to circular import
        suite_parameters: Optional[dict] = None,
        catch_exceptions: bool = True,
        result_format: Optional[str] = None,
        only_return_failures: bool = False,
        run_name: Optional[str] = None,
        run_time: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
    ) -> Union[ExpectationValidationResult, ExpectationSuiteValidationResult]:
        # noinspection SpellCheckingInspection
        """Run all expectations and return the outcome of the run.

        Args:
            expectation_suite: If None, uses the Expectation Suite configuration generated during the current Validator session. If an `ExpectationSuite` object, uses it as the configuration. If a string, assumes it is a path to a JSON file, and loads it as the Expectation Sutie configuration.
            run_id: Used to identify this validation result as part of a collection of validations.
            run_name: Used to identify this validation result as part of a collection of validations. Only used if a `run_id` is not passed. See DataContext for more information.
            run_time: Used to identify this validation result as part of a collection of validations. Only used if a `run_id` is not passed. See DataContext for more information.
            data_context: A datacontext object to use as part of validation for binding suite parameters and registering validation results. Overrides the Data Context configured when the Validator is instantiated.
            suite_parameters: If None, uses the evaluation_paramters from the Expectation Suite provided or as part of the Data Asset. If a dict, uses the suite parameters in the dictionary.
            catch_exceptions: If True, exceptions raised by tests will not end validation and will be described in the returned report.
            result_format: If None, uses the default value ('BASIC' or as specified). If string, the returned expectation output follows the specified format ('BOOLEAN_ONLY','BASIC', etc.).
            only_return_failures: If True, expectation results are only returned when `success = False`.
            checkpoint_name: Name of the Checkpoint which invoked this Validator.validate() call against an Expectation Suite. It will be added to `meta` field of the returned ExpectationSuiteValidationResult.

        Returns:
            Object containg the results.

        Raises:
            Exception: Depending on the Data Context configuration and arguments, there are numerous possible exceptions that may be raised.
            GreatExpectationsError: If `expectation_suite` is a string it must point to an existing and readable file.
            ValidationError: If `expectation_suite` is a string, the file it points to must be valid JSON.

        """  # noqa: E501
        # noinspection PyUnusedLocal
        try:
            validation_time = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )
            assert not (run_id and run_name) and not (
                run_id and run_time
            ), "Please provide either a run_id or run_name and/or run_time."
            if isinstance(run_id, dict):
                run_id = RunIdentifier(**run_id)
            elif not isinstance(run_id, RunIdentifier):
                run_id = RunIdentifier(run_name=run_name, run_time=run_time)

            self._active_validation = True

            # If a different validation data context was provided, override
            validation_data_context = self._data_context
            if data_context is None and self._data_context is not None:
                data_context = self._data_context
            elif data_context is not None:
                # temporarily set self._data_context so it is used inside the expectation decorator
                self._data_context = data_context

            if expectation_suite is None:
                expectation_suite = self.get_expectation_suite(
                    discard_failed_expectations=False,
                    discard_result_format_kwargs=False,
                    discard_include_config_kwargs=False,
                    discard_catch_exceptions_kwargs=False,
                )
            elif isinstance(expectation_suite, str):
                try:
                    with open(expectation_suite) as infile:
                        expectation_suite = expectationSuiteSchema.loads(infile.read())
                except ValidationError:
                    raise
                except OSError:
                    raise GreatExpectationsError(  # noqa: TRY003
                        f"Unable to load expectation suite: IO error while reading {expectation_suite}"  # noqa: E501
                    )

            if not isinstance(expectation_suite, ExpectationSuite):
                logger.error(
                    "Unable to validate using the provided value for expectation suite; does it need to be "  # noqa: E501
                    "loaded from a dictionary?"
                )
                return ExpectationValidationResult(success=False)

            # Suite parameter priority is
            # 1. from provided parameters
            # 2. from expectation configuration
            # 3. from data context
            # So, we load them in reverse order

            runtime_suite_parameters: dict = {}

            if expectation_suite.suite_parameters:
                runtime_suite_parameters.update(expectation_suite.suite_parameters)

            if suite_parameters is not None:
                runtime_suite_parameters.update(suite_parameters)

            # Convert suite parameters to be json-serializable
            runtime_suite_parameters = recursively_convert_to_json_serializable(
                runtime_suite_parameters
            )

            # Warn if our version is different from the version in the configuration
            # TODO: Deprecate "great_expectations.__version__"

            expectations_to_evaluate = self.process_expectations_for_validation(
                expectation_suite.expectation_configurations,
                runtime_suite_parameters,
            )

            runtime_configuration = self._get_runtime_configuration(
                catch_exceptions=catch_exceptions, result_format=result_format
            )

            results = self.graph_validate(
                configurations=expectations_to_evaluate,
                runtime_configuration=runtime_configuration,
            )

            if self._include_rendered_content:
                for validation_result in results:
                    validation_result.render()

            statistics = calc_validation_statistics(results)

            if only_return_failures:
                abbrev_results = []
                for exp in results:
                    if not exp.success:
                        abbrev_results.append(exp)
                results = abbrev_results

            expectation_suite_name = expectation_suite.name

            result = ExpectationSuiteValidationResult(
                results=results,
                success=statistics.success,
                suite_name=expectation_suite_name,
                statistics={
                    "evaluated_expectations": statistics.evaluated_expectations,
                    "successful_expectations": statistics.successful_expectations,
                    "unsuccessful_expectations": statistics.unsuccessful_expectations,
                    "success_percent": statistics.success_percent,
                },
                suite_parameters=runtime_suite_parameters,
                meta={
                    "great_expectations_version": ge_version,
                    "expectation_suite_name": expectation_suite_name,
                    "run_id": run_id,
                    "batch_spec": convert_to_json_serializable(self.active_batch_spec),
                    "batch_markers": self.active_batch_markers,
                    "active_batch_definition": self.active_batch_definition,
                    "validation_time": validation_time,
                    "checkpoint_name": checkpoint_name,
                },
                batch_id=self.active_batch_id,
            )

            self._data_context = validation_data_context
        finally:
            self._active_validation = False

        return result

    def process_expectations_for_validation(
        self,
        expectation_configurations: list[ExpectationConfiguration],
        suite_parameters: Optional[dict[str, Any]] = None,
    ) -> list[ExpectationConfiguration]:
        """Substitute suite parameters into the provided expectations and sort by column."""
        NO_COLUMN = "_nocolumn"  # just used to group expectations that don't specify a column
        columns: dict[str, list[ExpectationConfiguration]] = {}

        for expectation in expectation_configurations:
            expectation.process_suite_parameters(
                suite_parameters=suite_parameters,
                interactive_evaluation=self.interactive_evaluation,
                data_context=self._data_context,
            )
            if "column" in expectation.kwargs and isinstance(expectation.kwargs["column"], str):
                column = expectation.kwargs["column"]
            else:
                column = NO_COLUMN
            if column not in columns:
                columns[column] = []
            columns[column].append(expectation)

        expectations_to_evaluate = []
        for col in columns:
            expectations_to_evaluate.extend(columns[col])

        return expectations_to_evaluate

    def get_suite_parameter(self, parameter_name, default_value=None):
        """
        Get an suite parameter value that has been stored in meta.

        Args:
            parameter_name (string): The name of the parameter to store.
            default_value (any): The default value to be returned if the parameter is not found.

        Returns:
            The current value of the suite parameter.
        """
        if parameter_name in self._expectation_suite.suite_parameters:
            return self._expectation_suite.suite_parameters[parameter_name]
        else:
            return default_value

    def set_suite_parameter(self, parameter_name, parameter_value) -> None:
        """
        Provide a value to be stored in the data_asset suite_parameters object and used to evaluate
        parameterized expectations.

        Args:
            parameter_name (string): The name of the kwarg to be replaced at evaluation time
            parameter_value (any): The value to be used
        """
        self._expectation_suite.suite_parameters.update(
            {parameter_name: convert_to_json_serializable(parameter_value)}
        )

    def test_expectation_function(self, function: Callable, *args, **kwargs) -> Callable:
        """Test a generic expectation function

        Args:
            function (func): The function to be tested. (Must be a valid expectation function.)
            *args          : Positional arguments to be passed the function
            **kwargs       : Keyword arguments to be passed the function

        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to \
            define custom classes, etc. To use developed expectations from the command-line tool, you will still need \
            to define custom classes, etc.

            Check out :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations` for
            more information.
        """  # noqa: E501

        # noinspection SpellCheckingInspection
        argspec = inspect.getfullargspec(function)[0][1:]

        new_function = self.expectation(argspec)(function)
        return new_function(self, *args, **kwargs)

    @staticmethod
    def _parse_validation_graph(
        validation_graph: ValidationGraph,
        metrics: _MetricsDict,
    ) -> Tuple[Set[MetricConfiguration], Set[MetricConfiguration]]:
        """Given validation graph, returns the ready and needed metrics necessary for validation using a traversal of
        validation graph (a graph structure of metric ids) edges"""  # noqa: E501
        unmet_dependency_ids = set()
        unmet_dependency = set()
        maybe_ready_ids = set()
        maybe_ready = set()

        for edge in validation_graph.edges:
            if edge.left.id not in metrics:
                if edge.right is None or edge.right.id in metrics:
                    if edge.left.id not in maybe_ready_ids:
                        maybe_ready_ids.add(edge.left.id)
                        maybe_ready.add(edge.left)
                else:  # noqa: PLR5501
                    if edge.left.id not in unmet_dependency_ids:
                        unmet_dependency_ids.add(edge.left.id)
                        unmet_dependency.add(edge.left)

        return maybe_ready - unmet_dependency, unmet_dependency

    def _initialize_expectations(
        self,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
    ) -> None:
        """Instantiates `_expectation_suite` as empty by default or with a specified expectation `config`.
        In addition, this always sets the `default_expectation_args` to:
            `include_config`: False,
            `catch_exceptions`: False,
            `output_format`: 'BASIC'

        By default, initializes data_asset_type to the name of the implementing class, but subclasses
        that have interoperable semantics (e.g. Dataset) may override that parameter to clarify their
        interoperability.

        Args:
            expectation_suite (json): \
                A json-serializable expectation config. \
                If None, creates default `_expectation_suite` with an empty list of expectations and \
                key value `data_asset_name` as `data_asset_name`.

            expectation_suite_name (string): \
                The name to assign to the `expectation_suite.name`

        Returns:
            None
        """  # noqa: E501
        # Checking type of expectation_suite.
        # Check for expectation_suite_name is already done by ExpectationSuiteIdentifier
        if expectation_suite and not isinstance(expectation_suite, ExpectationSuite):
            raise TypeError(  # noqa: TRY003
                f"expectation_suite must be of type ExpectationSuite, not {type(expectation_suite)}"
            )
        if expectation_suite is not None:
            if isinstance(expectation_suite, dict):
                expectation_suite_dict: dict = expectationSuiteSchema.load(expectation_suite)
                expectation_suite = ExpectationSuite(**expectation_suite_dict)
            else:
                expectation_suite = copy.deepcopy(expectation_suite)
            self._expectation_suite: ExpectationSuite = expectation_suite

            if expectation_suite_name is not None:
                if self._expectation_suite.name != expectation_suite_name:
                    logger.warning(
                        f"Overriding existing expectation_suite_name {self._expectation_suite.name} with new name {expectation_suite_name}"  # noqa: E501
                    )
                self._expectation_suite.name = expectation_suite_name

        else:
            if expectation_suite_name is None:
                expectation_suite_name = "default"
            self._expectation_suite = ExpectationSuite(name=expectation_suite_name)

    def _get_runtime_configuration(
        self,
        catch_exceptions: Optional[bool] = None,
        result_format: Optional[Union[dict, str]] = None,
    ) -> dict:
        runtime_configuration = copy.deepcopy(self.default_expectation_args)

        if catch_exceptions is not None:
            runtime_configuration.update({"catch_exceptions": catch_exceptions})

        if (
            self.default_expectation_args["result_format"]
            == Validator.DEFAULT_RUNTIME_CONFIGURATION["result_format"]
        ):
            if result_format is None:
                runtime_configuration.pop("result_format")
            else:
                runtime_configuration.update({"result_format": result_format})
        else:  # noqa: PLR5501
            if result_format is not None:
                runtime_configuration.update({"result_format": result_format})

        return runtime_configuration

    def convert_to_checkpoint_validations_list(
        self,
    ) -> list[CheckpointValidationDefinition]:
        """
        Generates a list of validations to be used in the construction of a Checkpoint.

        Returns:
            A list of CheckpointValidationDefinitions (one for each batch in the Validator).
        """
        validations = []
        for batch in self.batch_cache.values():
            validation = CheckpointValidationDefinition(
                expectation_suite_name=self.expectation_suite_name,
                expectation_suite_id=self.expectation_suite.id,
                batch_request=batch.batch_request,
            )
            validations.append(validation)

        return validations
