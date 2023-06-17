from __future__ import annotations

import copy
import datetime
import inspect
import itertools
import json
import logging
import traceback
import warnings
from collections import defaultdict, namedtuple
from collections.abc import Hashable
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
from great_expectations.core._docs_decorators import deprecated_argument, public_api
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_asset.util import recursively_convert_to_json_serializable
from great_expectations.data_context.types.base import CheckpointValidationConfig
from great_expectations.dataset.pandas_dataset import PandasDataset
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset
from great_expectations.exceptions import (
    GreatExpectationsError,
    InvalidExpectationConfigurationError,
)
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.expectations.registry import (
    get_expectation_impl,
    list_registered_expectation_implementations,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.helpers.configuration_reconciliation import (
    DEFAULT_RECONCILATION_DIRECTIVES,
    ReconciliationDirectives,
    ReconciliationStrategy,
)
from great_expectations.rule_based_profiler.rule_based_profiler import (
    BaseRuleBasedProfiler,
)
from great_expectations.types import ClassConfig
from great_expectations.util import load_class, verify_dynamic_loading_support
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metrics_calculator import MetricsCalculator
from great_expectations.validator.validation_graph import (
    ExpectationValidationGraph,
    MetricEdge,
    ValidationGraph,
)

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


if TYPE_CHECKING:
    from great_expectations.core.batch import (
        Batch,
        BatchData,
        BatchDefinition,
        BatchMarkers,
    )
    from great_expectations.core.id_dict import BatchSpec
    from great_expectations.data_context.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.interfaces import Batch as FluentBatch
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation import Expectation
    from great_expectations.rule_based_profiler import RuleBasedProfilerResult
    from great_expectations.rule_based_profiler.expectation_configuration_builder import (
        ExpectationConfigurationBuilder,
    )
    from great_expectations.rule_based_profiler.parameter_builder import (
        ParameterBuilder,
    )
    from great_expectations.rule_based_profiler.parameter_container import (
        ParameterContainer,
    )
    from great_expectations.rule_based_profiler.rule import Rule
    from great_expectations.validator.computed_metric import MetricValue
    from great_expectations.validator.metric_configuration import MetricConfiguration


@dataclass
class ValidationDependencies:
    # Note: Dependent "metric_name" (key) is different from "metric_name" in dependency "MetricConfiguration" (value).
    metric_configurations: Dict[str, MetricConfiguration] = field(default_factory=dict)
    result_format: Dict[str, Any] = field(default_factory=dict)

    def set_metric_configuration(
        self, metric_name: str, metric_configuration: MetricConfiguration
    ) -> None:
        """
        Sets specified "MetricConfiguration" for "metric_name" to "metric_configurations" dependencies dictionary.
        """
        self.metric_configurations[metric_name] = metric_configuration

    def get_metric_configuration(
        self, metric_name: str
    ) -> Optional[MetricConfiguration]:
        """
        Obtains "MetricConfiguration" for specified "metric_name" from "metric_configurations" dependencies dictionary.
        """
        return self.metric_configurations.get(metric_name)

    def remove_metric_configuration(self, metric_name: str) -> None:
        """
        Removes "MetricConfiguration" for specified "metric_name" from "metric_configurations" dependencies dictionary.
        """
        del self.metric_configurations[metric_name]

    def get_metric_names(self) -> List[str]:
        """
        Returns "metric_name" keys, for which "MetricConfiguration" dependency objects have been specified.
        """
        return list(self.metric_configurations.keys())

    def get_metric_configurations(self) -> List[MetricConfiguration]:
        """
        Returns "MetricConfiguration" dependency objects specified.
        """
        return list(self.metric_configurations.values())


ValidationStatistics = namedtuple(
    "ValidationStatistics",
    [
        "evaluated_expectations",
        "successful_expectations",
        "unsuccessful_expectations",
        "success_percent",
        "success",
    ],
)


@public_api
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
        include_rendered_content: If True, the Rendered Content will be included in the ExpectationValidationResult.
    """

    DEFAULT_RUNTIME_CONFIGURATION = {
        "include_config": True,
        "catch_exceptions": False,
        "result_format": "BASIC",
    }
    RUNTIME_KEYS = DEFAULT_RUNTIME_CONFIGURATION.keys()

    # noinspection PyUnusedLocal
    def __init__(  # noqa: PLR0913
        self,
        execution_engine: ExecutionEngine,
        interactive_evaluation: bool = True,
        expectation_suite: Optional[ExpectationSuite] = None,
        expectation_suite_name: Optional[str] = None,
        data_context: Optional[AbstractDataContext] = None,
        batches: Optional[
            Union[List[Batch], Sequence[Union[Batch, FluentBatch]]]
        ] = None,
        include_rendered_content: Optional[bool] = None,
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
            Validator.DEFAULT_RUNTIME_CONFIGURATION
        )

        # This special state variable tracks whether a validation run is going on, which will disable
        # saving expectation config objects
        self._active_validation: bool = False
        if self._data_context and hasattr(
            self._data_context, "_expectation_explorer_manager"
        ):
            # TODO: verify flow of default expectation arguments
            self.set_default_expectation_argument("include_config", True)

        self._include_rendered_content: Optional[bool] = include_rendered_content

    @property
    def execution_engine(self) -> ExecutionEngine:
        """Returns the execution engine being used by the validator at the given time"""
        return self._execution_engine

    @property
    def metrics_calculator(self) -> MetricsCalculator:
        """Returns the "MetricsCalculator" object being used by the Validator to handle metrics computations."""
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
    def active_batch_data(self) -> Optional[BatchData]:
        """Getter for BatchData object from the currently-active Batch object (convenience property)."""
        return self._execution_engine.batch_manager.active_batch_data

    @property
    def batch_cache(self) -> Dict[str, Batch]:
        """Getter for dictionary of Batch objects (convenience property)"""
        return self._execution_engine.batch_manager.batch_cache

    @property
    def batches(self) -> Dict[str, Union[Batch, FluentBatch]]:
        """Getter for dictionary of Batch objects (alias convenience property, to be deprecated)"""
        return self.batch_cache

    @property
    def active_batch_id(self) -> Optional[str]:
        """Getter for batch_id of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_id

    @property
    def active_batch(self) -> Optional[Batch]:
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
    def active_batch_definition(self) -> Optional[BatchDefinition]:
        """Getter for batch_definition of active Batch (convenience property)"""
        return self._execution_engine.batch_manager.active_batch_definition

    @property
    def expectation_suite(self) -> ExpectationSuite:
        return self._expectation_suite

    @expectation_suite.setter
    def expectation_suite(self, value: ExpectationSuite) -> None:
        self._initialize_expectations(
            expectation_suite=value,
            expectation_suite_name=value.expectation_suite_name,
        )

    @property
    def expectation_suite_name(self) -> str:
        """Gets the current expectation_suite name of this data_asset as stored in the expectations configuration."""
        return self._expectation_suite.expectation_suite_name

    @expectation_suite_name.setter
    def expectation_suite_name(self, expectation_suite_name: str) -> None:
        """Sets the expectation_suite name of this data_asset as stored in the expectations configuration."""
        self._expectation_suite.expectation_suite_name = expectation_suite_name

    def load_batch_list(self, batch_list: List[Batch]) -> None:
        self._execution_engine.batch_manager.load_batch_list(batch_list=batch_list)

    @public_api
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
        """
        return self._metrics_calculator.get_metrics(metrics=metrics)

    def compute_metrics(
        self,
        metric_configurations: List[MetricConfiguration],
        runtime_configuration: Optional[dict] = None,
        min_graph_edges_pbar_enable: int = 0,
        # Set to low number (e.g., 3) to suppress progress bar for small graphs.
    ) -> Dict[Tuple[str, str, str], MetricValue]:
        """
        Convenience method that computes requested metrics (specified as elements of "MetricConfiguration" list).

        Args:
            metric_configurations: List of desired MetricConfiguration objects to be resolved.
            runtime_configuration: Additional run-time settings (see "Validator.DEFAULT_RUNTIME_CONFIGURATION").
            min_graph_edges_pbar_enable: Minumum number of graph edges to warrant showing progress bars.

        Returns:
            Dictionary with requested metrics resolved, with unique metric ID as key and computed metric as value.
        """
        return self._metrics_calculator.compute_metrics(
            metric_configurations=metric_configurations,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=min_graph_edges_pbar_enable,
        )

    @public_api
    def columns(self, domain_kwargs: Optional[Dict[str, Any]] = None) -> List[str]:
        """Convenience method to obtain Batch columns.

        Arguments:
            domain_kwargs: Optional dictionary of domain kwargs (e.g., containing "batch_id").

        Returns:
            The list of Batch columns.
        """
        return self._metrics_calculator.columns(domain_kwargs=domain_kwargs)

    @public_api
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
            validator_attrs
            | class_expectation_impls
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
                    enable = progress_bars["globally"]
                if "metric_calculations" in progress_bars:
                    enable = progress_bars["metric_calculations"]

        return enable

    def __getattr__(self, name):
        name = name.lower()
        if name.startswith("expect_") and get_expectation_impl(name):
            return self.validate_expectation(name)
        elif (
            self._expose_dataframe_methods
            and isinstance(self.active_batch.data, PandasBatchData)
            and hasattr(pd.DataFrame, name)
        ):
            return getattr(self.active_batch.data.dataframe, name)
        else:
            raise AttributeError(
                f"'{type(self).__name__}'  object has no attribute '{name}'"
            )

    def validate_expectation(self, name: str) -> Callable:  # noqa: PLR0915
        """
        Given the name of an Expectation, obtains the Class-first Expectation implementation and utilizes the
                expectation's validate method to obtain a validation result. Also adds in the runtime configuration

                        Args:
                            name (str): The name of the Expectation being validated

                        Returns:
                            The Expectation's validation result
        """
        expectation_impl = get_expectation_impl(name)

        def inst_expectation(*args, **kwargs):  # noqa: PLR0912
            # this is used so that exceptions are caught appropriately when they occur in expectation config

            # TODO: JPC - THIS LOGIC DOES NOT RESPECT DEFAULTS SET BY USERS IN THE VALIDATOR VS IN THE EXPECTATION
            # DEVREL has action to develop a new plan in coordination with MarioPod

            expectation_kwargs: dict = recursively_convert_to_json_serializable(kwargs)

            meta: dict = expectation_kwargs.pop("meta", None)

            basic_default_expectation_args: dict = {
                k: v
                for k, v in self.default_expectation_args.items()
                if k in Validator.RUNTIME_KEYS
            }
            basic_runtime_configuration: dict = copy.deepcopy(
                basic_default_expectation_args
            )
            basic_runtime_configuration.update(
                {k: v for k, v in kwargs.items() if k in Validator.RUNTIME_KEYS}
            )

            allowed_config_keys: Tuple[str] = expectation_impl.get_allowed_config_keys()

            args_keys: Tuple[str] = expectation_impl.args_keys or tuple()

            arg_name: str

            idx: int
            arg: Any
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
                    raise InvalidExpectationConfigurationError(
                        f"Invalid positional argument: {arg}"
                    )

            configuration: ExpectationConfiguration = (
                self._build_expectation_configuration(
                    expectation_type=name,
                    expectation_kwargs=expectation_kwargs,
                    meta=meta,
                    expectation_impl=expectation_impl,
                    runtime_configuration=basic_runtime_configuration,
                )
            )

            exception_info: ExceptionInfo

            if self.interactive_evaluation:
                configuration.process_evaluation_parameters(
                    self._expectation_suite.evaluation_parameters,
                    True,
                    self._data_context,
                )

            try:
                expectation = expectation_impl(configuration)
                """Given an implementation and a configuration for any Expectation, returns its validation result"""

                if not self.interactive_evaluation and not self._active_validation:
                    validation_result = ExpectationValidationResult(
                        expectation_config=copy.deepcopy(expectation.configuration)
                    )
                else:
                    validation_result = expectation.validate(
                        validator=self,
                        evaluation_parameters=self._expectation_suite.evaluation_parameters,
                        data_context=self._data_context,
                        runtime_configuration=basic_runtime_configuration,
                    )

                # If validate has set active_validation to true, then we do not save the config to avoid
                # saving updating expectation configs to the same suite during validation runs
                if self._active_validation is True:
                    stored_config = configuration.get_raw_configuration()
                else:
                    # Append the expectation to the config.
                    stored_config = self._expectation_suite._add_expectation(
                        expectation_configuration=configuration.get_raw_configuration(),
                        send_usage_event=False,
                    )

                # If there was no interactive evaluation, success will not have been computed.
                if validation_result.success is not None:
                    # Add a "success" object to the config
                    stored_config.success_on_last_run = validation_result.success

            except Exception as err:
                if basic_runtime_configuration.get("catch_exceptions"):
                    exception_traceback = traceback.format_exc()
                    exception_message = f"{type(err).__name__}: {str(err)}"
                    exception_info = ExceptionInfo(
                        exception_traceback=exception_traceback,
                        exception_message=exception_message,
                    )
                    validation_result = ExpectationValidationResult(
                        success=False,
                        exception_info=exception_info,
                        expectation_config=configuration,
                    )
                else:
                    raise err

            if self._include_rendered_content:
                validation_result.render()

            return validation_result

        inst_expectation.__name__ = name
        inst_expectation.__doc__ = expectation_impl.__doc__

        return inst_expectation

    def _build_expectation_configuration(  # noqa: PLR0913
        self,
        expectation_type: str,
        expectation_kwargs: dict,
        meta: dict,
        expectation_impl: Expectation,
        runtime_configuration: Optional[dict] = None,
    ) -> ExpectationConfiguration:
        auto: bool = expectation_kwargs.get("auto", False)
        profiler_config: Optional[RuleBasedProfilerConfig] = expectation_kwargs.get(
            "profiler_config"
        )
        default_profiler_config: Optional[
            RuleBasedProfilerConfig
        ] = expectation_impl.default_kwarg_values.get("profiler_config")

        if auto and profiler_config is None and default_profiler_config is None:
            raise ValueError(
                "Automatic Expectation argument estimation requires a Rule-Based Profiler to be provided."
            )

        configuration: ExpectationConfiguration

        profiler: Optional[
            BaseRuleBasedProfiler
        ] = self.build_rule_based_profiler_for_expectation(
            expectation_type=expectation_type
        )(
            *(), **expectation_kwargs
        )
        if profiler is not None:
            profiler_result: RuleBasedProfilerResult = profiler.run(
                variables=None,
                rules=None,
                batch_list=list(self.batch_cache.values()),
                batch_request=None,
                runtime_configuration=runtime_configuration,
                reconciliation_directives=DEFAULT_RECONCILATION_DIRECTIVES,
            )
            expectation_configurations: List[
                ExpectationConfiguration
            ] = profiler_result.expectation_configurations
            configuration = expectation_configurations[0]

            # Reconcile explicitly provided "ExpectationConfiguration" success_kwargs as overrides to generated values.
            success_keys: Tuple[str] = (
                expectation_impl.success_keys
                if hasattr(expectation_impl, "success_keys")
                else tuple()
            )
            arg_keys: Tuple[str] = (
                expectation_impl.arg_keys
                if hasattr(expectation_impl, "arg_keys")
                else tuple()
            )
            runtime_keys: Tuple[str] = (
                expectation_impl.runtime_keys
                if hasattr(expectation_impl, "runtime_keys")
                else None
            ) or tuple()
            # noinspection PyTypeChecker
            override_keys: Tuple[str] = success_keys + arg_keys + runtime_keys

            key: str
            value: Any
            expectation_kwargs_overrides: dict = {
                key: value
                for key, value in expectation_kwargs.items()
                if key in override_keys
            }
            expectation_kwargs_overrides = convert_to_json_serializable(
                data=expectation_kwargs_overrides
            )

            expectation_kwargs = configuration.kwargs
            expectation_kwargs.update(expectation_kwargs_overrides)

            if meta is None:
                meta = {}

            meta["profiler_config"] = profiler.to_json_dict()

        configuration = ExpectationConfiguration(
            expectation_type=expectation_type,
            kwargs=expectation_kwargs,
            meta=meta,
        )

        return configuration

    def build_rule_based_profiler_for_expectation(
        self, expectation_type: str
    ) -> Callable:
        """
        Given name of Expectation ("expectation_type"), builds effective RuleBasedProfiler object from configuration.
        Args:
            expectation_type (str): Name of Expectation for which Rule-Based Profiler may be configured.

        Returns:
            Function that builds effective RuleBasedProfiler object (for specified "expectation_type").
        """
        expectation_impl = get_expectation_impl(expectation_type)

        def inst_rule_based_profiler(
            *args, **kwargs
        ) -> Optional[BaseRuleBasedProfiler]:
            if args is None:
                args = tuple()

            if kwargs is None:
                kwargs = {}

            expectation_kwargs: dict = recursively_convert_to_json_serializable(kwargs)

            allowed_config_keys: Tuple[str] = expectation_impl.get_allowed_config_keys()

            args_keys: Tuple[str] = expectation_impl.args_keys or tuple()

            arg_name: str

            idx: int
            arg: Any
            for idx, arg in enumerate(args):
                try:
                    arg_name = args_keys[idx]
                    if arg_name in allowed_config_keys:
                        expectation_kwargs[arg_name] = arg
                    if arg_name == "meta":
                        logger.warning(
                            "Setting meta via args could be ambiguous; please use a kwarg instead."
                        )
                except IndexError:
                    raise InvalidExpectationConfigurationError(
                        f"Invalid positional argument: {arg}"
                    )

            success_keys: Tuple[str] = (
                expectation_impl.success_keys
                if hasattr(expectation_impl, "success_keys")
                else tuple()
            )

            auto: Optional[bool] = expectation_kwargs.get("auto")
            profiler_config: Optional[RuleBasedProfilerConfig] = expectation_kwargs.get(
                "profiler_config"
            )
            default_profiler_config: Optional[
                RuleBasedProfilerConfig
            ] = expectation_impl.default_kwarg_values.get("profiler_config")

            if auto and profiler_config is None and default_profiler_config is None:
                raise ValueError(
                    "Automatic Expectation argument estimation requires a Rule-Based Profiler to be provided."
                )

            profiler: Optional[BaseRuleBasedProfiler]

            if auto:
                # Save custom Rule-Based Profiler configuration for reconciling it with optionally-specified default
                # Rule-Based Profiler configuration as an override argument to "BaseRuleBasedProfiler.run()" method.
                override_profiler_config: Optional[RuleBasedProfilerConfig]
                if default_profiler_config:
                    override_profiler_config = copy.deepcopy(profiler_config)
                else:
                    override_profiler_config = None

                """
                If default Rule-Based Profiler configuration exists, use it as base with custom Rule-Based Profiler
                configuration as override; otherwise, use custom Rule-Based Profiler configuration with no override.
                """
                profiler_config = default_profiler_config or profiler_config

                profiler = self._build_rule_based_profiler_from_config_and_runtime_args(
                    expectation_type=expectation_type,
                    expectation_kwargs=expectation_kwargs,
                    success_keys=success_keys,
                    profiler_config=profiler_config,
                    override_profiler_config=override_profiler_config,
                )
            else:
                profiler = None

            return profiler

        return inst_rule_based_profiler

    def _build_rule_based_profiler_from_config_and_runtime_args(  # noqa: PLR0913
        self,
        expectation_type: str,
        expectation_kwargs: dict,
        success_keys: Tuple[str],
        profiler_config: RuleBasedProfilerConfig,
        override_profiler_config: Optional[RuleBasedProfilerConfig] = None,
    ) -> BaseRuleBasedProfiler:
        assert (
            profiler_config.name == expectation_type
        ), "The name of profiler used to build an ExpectationConfiguration must equal to expectation_type of the expectation being invoked."

        profiler = BaseRuleBasedProfiler(
            profiler_config=profiler_config,
            data_context=self.data_context,
        )

        rules: List[Rule] = profiler.rules

        assert (
            len(rules) == 1
        ), "A Rule-Based Profiler for an Expectation can have exactly one rule."

        domain_type: MetricDomainTypes

        if override_profiler_config is None:
            override_profiler_config = {}

        if isinstance(override_profiler_config, RuleBasedProfilerConfig):
            override_profiler_config = override_profiler_config.to_json_dict()

        override_profiler_config.pop("name", None)
        override_profiler_config.pop("config_version", None)

        override_variables: Dict[str, Any] = override_profiler_config.get(
            "variables", {}
        )
        effective_variables: Optional[
            ParameterContainer
        ] = profiler.reconcile_profiler_variables(
            variables=override_variables,
            reconciliation_strategy=ReconciliationStrategy.UPDATE,
        )
        profiler.variables = effective_variables

        override_rules: Dict[str, Dict[str, Any]] = override_profiler_config.get(
            "rules", {}
        )

        assert (
            len(override_rules) <= 1
        ), "An override Rule-Based Profiler for an Expectation can have exactly one rule."

        if override_rules:
            profiler.rules[0].name = list(override_rules.keys())[0]
            effective_rules: List[Rule] = profiler.reconcile_profiler_rules(
                rules=override_rules,
                reconciliation_directives=ReconciliationDirectives(
                    domain_builder=ReconciliationStrategy.UPDATE,
                    parameter_builder=ReconciliationStrategy.REPLACE,
                    expectation_configuration_builder=ReconciliationStrategy.REPLACE,
                ),
            )
            profiler.rules = effective_rules

        self._validate_profiler_and_update_rules_properties(
            profiler=profiler,
            expectation_type=expectation_type,
            expectation_kwargs=expectation_kwargs,
            success_keys=success_keys,
        )

        return profiler

    def _validate_profiler_and_update_rules_properties(
        self,
        profiler: BaseRuleBasedProfiler,
        expectation_type: str,
        expectation_kwargs: dict,
        success_keys: Tuple[str],
    ) -> None:
        rule: Rule = profiler.rules[0]
        assert (
            rule.expectation_configuration_builders[0].expectation_type
            == expectation_type
        ), "ExpectationConfigurationBuilder in profiler used to build an ExpectationConfiguration must have the same expectation_type as the expectation being invoked."

        # TODO: <Alex>Add "metric_domain_kwargs_override" when "Expectation" defines "domain_keys" separately.</Alex>
        key: str
        value: Any
        metric_value_kwargs_override: dict = {
            key: value
            for key, value in expectation_kwargs.items()
            if key in success_keys
            and key not in BaseRuleBasedProfiler.EXPECTATION_SUCCESS_KEYS
        }

        domain_type: MetricDomainTypes = rule.domain_builder.domain_type
        if domain_type not in MetricDomainTypes:
            raise ValueError(
                f'Domain type declaration "{domain_type}" in "MetricDomainTypes" does not exist.'
            )

        # TODO: <Alex>Handle future domain_type cases as they are defined.</Alex>
        if domain_type == MetricDomainTypes.COLUMN:
            column_name = expectation_kwargs.get("column")
            rule.domain_builder.include_column_names = (
                [column_name] if column_name else None
            )

        parameter_builders: List[ParameterBuilder] = rule.parameter_builders or []

        parameter_builder: ParameterBuilder

        for parameter_builder in parameter_builders:
            self._update_metric_value_kwargs_for_success_keys(
                parameter_builder=parameter_builder,
                metric_value_kwargs=metric_value_kwargs_override,
            )

        expectation_configuration_builders: List[ExpectationConfigurationBuilder] = (
            rule.expectation_configuration_builders or []
        )

        expectation_configuration_builder: ExpectationConfigurationBuilder
        for expectation_configuration_builder in expectation_configuration_builders:
            validation_parameter_builders: List[ParameterBuilder] = (
                expectation_configuration_builder.validation_parameter_builders or []
            )
            for parameter_builder in validation_parameter_builders:
                self._update_metric_value_kwargs_for_success_keys(
                    parameter_builder=parameter_builder,
                    metric_value_kwargs=metric_value_kwargs_override,
                )

    def _update_metric_value_kwargs_for_success_keys(
        self,
        parameter_builder: ParameterBuilder,
        metric_value_kwargs: Optional[dict] = None,
    ) -> None:
        if metric_value_kwargs is None:
            metric_value_kwargs = {}

        if hasattr(parameter_builder, "metric_name") and hasattr(
            parameter_builder, "metric_value_kwargs"
        ):
            parameter_builder_metric_value_kwargs: dict = (
                parameter_builder.metric_value_kwargs or {}
            )

            parameter_builder_metric_value_kwargs = {
                key: metric_value_kwargs.get(key) or value
                for key, value in parameter_builder_metric_value_kwargs.items()
            }
            parameter_builder.metric_value_kwargs = (
                parameter_builder_metric_value_kwargs
            )

        evaluation_parameter_builders: List[ParameterBuilder] = (
            parameter_builder.evaluation_parameter_builders or []
        )

        evaluation_parameter_builder: ParameterBuilder
        for evaluation_parameter_builder in evaluation_parameter_builders:
            self._update_metric_value_kwargs_for_success_keys(
                parameter_builder=evaluation_parameter_builder,
                metric_value_kwargs=metric_value_kwargs,
            )

    def list_available_expectation_types(self) -> List[str]:
        """Returns a list of all expectations available to the validator"""
        keys = dir(self)
        return [
            expectation for expectation in keys if expectation.startswith("expect_")
        ]

    def graph_validate(
        self,
        configurations: List[ExpectationConfiguration],
        runtime_configuration: Optional[dict] = None,
    ) -> List[ExpectationValidationResult]:
        """Obtains validation dependencies for each metric using the implementation of their associated expectation,
        then proceeds to add these dependencies to the validation graph, supply readily available metric implementations
        to fulfill current metric requirements, and validate these metrics.

        Args:
            configurations(List[ExpectationConfiguration]): A list of needed Expectation Configurations that will be
            used to supply domain and values for metrics.
            runtime_configuration (dict): A dictionary of runtime keyword arguments, controlling semantics, such as the
            result_format.

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

        graph: ValidationGraph = (
            self._generate_suite_level_graph_from_expectation_level_sub_graphs(
                expectation_validation_graphs=expectation_validation_graphs
            )
        )

        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]

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
            # If a general Exception occurs during the execution of "ValidationGraph.resolve()", then
            # all expectations in the suite are impacted, because it is impossible to attribute the failure to a metric.
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
                raise err

        configuration: ExpectationConfiguration
        result: ExpectationValidationResult
        for configuration in processed_configurations:
            try:
                runtime_configuration_default = copy.deepcopy(runtime_configuration)

                result = configuration.metrics_validate(
                    metrics=resolved_metrics,
                    execution_engine=self._execution_engine,
                    runtime_configuration=runtime_configuration_default,
                )
                evrs.append(result)
            except Exception as err:
                if catch_exceptions:
                    exception_traceback: str = traceback.format_exc()
                    evrs = self._catch_exceptions_in_failing_expectation_validations(
                        exception_traceback=exception_traceback,
                        exception=err,
                        failing_expectation_configurations=[configuration],
                        evrs=evrs,
                    )
                else:
                    raise err

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
        # While evaluating expectation configurations, create sub-graph for every metric dependency and incorporate
        # these sub-graphs under corresponding expectation-level sub-graph (state of ExpectationValidationGraph object).
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
                    configuration.expectation_type is not None
                ), "Given configuration should include expectation type"
            except AssertionError as e:
                raise InvalidExpectationConfigurationError(str(e))

            evaluated_config = copy.deepcopy(configuration)
            evaluated_config.kwargs.update({"batch_id": self.active_batch_id})

            expectation_impl = get_expectation_impl(evaluated_config.expectation_type)
            validation_dependencies: ValidationDependencies = (
                expectation_impl().get_validation_dependencies(
                    configuration=evaluated_config,
                    execution_engine=self._execution_engine,
                    runtime_configuration=runtime_configuration,
                )
            )

            try:
                expectation_validation_graph: ExpectationValidationGraph = ExpectationValidationGraph(
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
                    raise err

        return expectation_validation_graphs, evrs, processed_configurations

    def _generate_suite_level_graph_from_expectation_level_sub_graphs(
        self,
        expectation_validation_graphs: List[ExpectationValidationGraph],
    ) -> ValidationGraph:
        # Collect edges from all expectation-level sub-graphs and incorporate them under common suite-level graph.
        expectation_validation_graph: ExpectationValidationGraph
        edges: List[MetricEdge] = list(
            itertools.chain.from_iterable(
                [
                    expectation_validation_graph.graph.edges
                    for expectation_validation_graph in expectation_validation_graphs
                ]
            )
        )
        validation_graph = ValidationGraph(
            execution_engine=self._execution_engine, edges=edges
        )
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
        Dict[Tuple[str, str, str], MetricValue],
        List[ExpectationValidationResult],
        List[ExpectationConfiguration],
    ]:
        # Resolve overall suite-level graph and process any MetricResolutionError type exceptions that might occur.
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue]
        aborted_metrics_info: Dict[
            Tuple[str, str, str],
            Dict[str, Union[MetricConfiguration, Set[ExceptionInfo], int]],
        ]
        (
            resolved_metrics,
            aborted_metrics_info,
        ) = self._metrics_calculator.resolve_validation_graph(
            graph=graph,
            runtime_configuration=runtime_configuration,
            min_graph_edges_pbar_enable=0,
        )

        # Trace MetricResolutionError occurrences to expectations relying on corresponding malfunctioning metrics.
        rejected_configurations: List[ExpectationConfiguration] = []
        for expectation_validation_graph in expectation_validation_graphs:
            metric_exception_info: Set[
                ExceptionInfo
            ] = expectation_validation_graph.get_exception_info(
                metric_info=aborted_metrics_info
            )
            # Report all MetricResolutionError occurrences impacting expectation and append it to rejected list.
            if len(metric_exception_info) > 0:
                configuration = expectation_validation_graph.configuration
                for exception_info in metric_exception_info:
                    result = ExpectationValidationResult(
                        success=False,
                        exception_info=exception_info,
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
        """
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

    @public_api
    def remove_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
        ge_cloud_id: Optional[str] = None,
    ) -> List[ExpectationConfiguration]:
        """Remove an ExpectationConfiguration from the ExpectationSuite associated with the Validator.

        Args:
            expectation_configuration: A potentially incomplete (partial) Expectation Configuration to match against.
            match_type: This determines what kwargs to use when matching. Options are:
                - 'domain' to match based on the data evaluated by that expectation
                - 'success' to match based on all configuration parameters that influence whether an expectation succeeds on a given batch of data
                - 'runtime' to match based on all configuration parameters.
            remove_multiple_matches: If True, will remove multiple matching expectations.
            ge_cloud_id: Great Expectations Cloud id for an Expectation.

        Returns:
            The list of deleted ExpectationConfigurations.

        Raises:
            TypeError: Must provide either expectation_configuration or ge_cloud_id.
            ValueError: No match or multiple matches found (and remove_multiple_matches=False).
        """

        return self._expectation_suite.remove_expectation(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            remove_multiple_matches=remove_multiple_matches,
            ge_cloud_id=ge_cloud_id,
        )

    def discard_failing_expectations(self) -> None:
        """Removes any expectations from the validator where the validation has failed"""
        res = self.validate(only_return_failures=True).results
        if any(res):
            for item in res:
                self.remove_expectation(
                    expectation_configuration=item.expectation_config,
                    match_type="runtime",
                )
            warnings.warn(f"Removed {len(res)} expectations that were 'False'")

    def get_default_expectation_arguments(self) -> dict:
        """Fetch default expectation arguments for this data_asset

        Returns:
            A dictionary containing all the current default expectation arguments for a data_asset

            Ex::

                {
                    "include_config" : True,
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

    @public_api
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
        expectations = expectation_suite.expectations

        discards = defaultdict(int)

        if discard_failed_expectations:
            new_expectations = []

            for expectation in expectations:
                # Note: This is conservative logic.
                # Instead of retaining expectations IFF success==True, it discard expectations IFF success==False.
                # In cases where expectation.success is missing or None, expectations are *retained*.
                # Such a case could occur if expectations were loaded from a config file and never run.
                if expectation.success_on_last_run is False:
                    discards["failed_expectations"] += 1
                else:
                    new_expectations.append(expectation)

            expectations = new_expectations

        message = f"\t{len(expectations)} expectation(s) included in expectation_suite."

        if discards["failed_expectations"] > 0 and not suppress_warnings:
            message += (
                f" Omitting {discards['failed_expectations']} expectation(s) that failed when last run; set "
                "discard_failed_expectations=False to include them."
            )

        for expectation in expectations:
            # FIXME: Factor this out into a new function. The logic is duplicated in remove_expectation,
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

        if (
            len(settings_message) > 1
        ):  # Only add this if we added one of the settings above.
            settings_message += " settings filtered."

        expectation_suite.expectations = expectations
        if not suppress_logging:
            logger.info(message + settings_message)
        return expectation_suite

    @public_api
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
        """
        expectation_suite = self.get_expectation_suite(
            discard_failed_expectations,
            discard_result_format_kwargs,
            discard_include_config_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings,
        )
        if filepath is None and self._data_context is not None:
            self._data_context.add_or_update_expectation_suite(
                expectation_suite=expectation_suite
            )
            if self.cloud_mode:
                updated_suite = self._data_context.get_expectation_suite(
                    ge_cloud_id=expectation_suite.ge_cloud_id
                )
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
            raise ValueError(
                "Unable to save config: filepath or data_context must be available."
            )

    # TODO: <Alex>Should "include_config" also be an argument of this method?</Alex>
    @public_api
    @deprecated_argument(
        argument_name="run_id",
        message="Only the str version of this argument is deprecated. run_id should be a RunIdentifier or dict. Support will be removed in 0.16.0.",
        version="0.13.0",
    )
    def validate(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        expectation_suite: str | ExpectationSuite | None = None,
        run_id: str | RunIdentifier | Dict[str, str] | None = None,
        data_context: Optional[
            Any
        ] = None,  # Cannot type DataContext due to circular import
        evaluation_parameters: Optional[dict] = None,
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
            data_context: A datacontext object to use as part of validation for binding evaluation parameters and registering validation results. Overrides the Data Context configured when the Validator is instantiated.
            evaluation_parameters: If None, uses the evaluation_paramters from the Expectation Suite provided or as part of the Data Asset. If a dict, uses the evaluation parameters in the dictionary.
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

        """
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
                    raise GreatExpectationsError(
                        f"Unable to load expectation suite: IO error while reading {expectation_suite}"
                    )
            elif not isinstance(expectation_suite, ExpectationSuite):
                logger.error(
                    "Unable to validate using the provided value for expectation suite; does it need to be "
                    "loaded from a dictionary?"
                )
                if getattr(data_context, "_usage_statistics_handler", None):
                    # noinspection PyProtectedMember
                    handler = data_context._usage_statistics_handler
                    # noinspection PyProtectedMember
                    handler.send_usage_message(
                        event="data_asset.validate",
                        event_payload=handler.anonymizer.anonymize(obj=self),
                        success=False,
                    )
                return ExpectationValidationResult(success=False)

            # Evaluation parameter priority is
            # 1. from provided parameters
            # 2. from expectation configuration
            # 3. from data context
            # So, we load them in reverse order

            if data_context is not None:
                runtime_evaluation_parameters = (
                    data_context.evaluation_parameter_store.get_bind_params(run_id)
                )
            else:
                runtime_evaluation_parameters = {}

            if expectation_suite.evaluation_parameters:
                runtime_evaluation_parameters.update(
                    expectation_suite.evaluation_parameters
                )

            if evaluation_parameters is not None:
                runtime_evaluation_parameters.update(evaluation_parameters)

            # Convert evaluation parameters to be json-serializable
            runtime_evaluation_parameters = recursively_convert_to_json_serializable(
                runtime_evaluation_parameters
            )

            # Warn if our version is different from the version in the configuration
            # TODO: Deprecate "great_expectations.__version__"

            # Group expectations by column
            columns = {}

            for expectation in expectation_suite.expectations:
                expectation.process_evaluation_parameters(
                    evaluation_parameters=runtime_evaluation_parameters,
                    interactive_evaluation=self.interactive_evaluation,
                    data_context=self._data_context,
                )
                if "column" in expectation.kwargs and isinstance(
                    expectation.kwargs["column"], Hashable
                ):
                    column = expectation.kwargs["column"]
                else:
                    # noinspection SpellCheckingInspection
                    column = "_nocolumn"
                if column not in columns:
                    columns[column] = []
                columns[column].append(expectation)

            expectations_to_evaluate = []
            for col in columns:
                expectations_to_evaluate.extend(columns[col])

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
            statistics = self._calc_validation_statistics(results)

            if only_return_failures:
                abbrev_results = []
                for exp in results:
                    if not exp.success:
                        abbrev_results.append(exp)
                results = abbrev_results

            expectation_suite_name = expectation_suite.expectation_suite_name

            result = ExpectationSuiteValidationResult(
                results=results,
                success=statistics.success,
                statistics={
                    "evaluated_expectations": statistics.evaluated_expectations,
                    "successful_expectations": statistics.successful_expectations,
                    "unsuccessful_expectations": statistics.unsuccessful_expectations,
                    "success_percent": statistics.success_percent,
                },
                evaluation_parameters=runtime_evaluation_parameters,
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
            )

            self._data_context = validation_data_context
        except Exception:
            if getattr(data_context, "_usage_statistics_handler", None):
                # noinspection PyProtectedMember
                handler = data_context._usage_statistics_handler
                # noinspection PyProtectedMember
                handler.send_usage_message(
                    event="data_asset.validate",
                    event_payload=handler.anonymizer.anonymize(obj=self),
                    success=False,
                )
            raise
        finally:
            self._active_validation = False

        if getattr(data_context, "_usage_statistics_handler", None):
            # noinspection PyProtectedMember
            handler = data_context._usage_statistics_handler
            # noinspection PyProtectedMember
            handler.send_usage_message(
                event="data_asset.validate",
                event_payload=handler.anonymizer.anonymize(obj=self),
                success=True,
            )
        return result

    def get_evaluation_parameter(self, parameter_name, default_value=None):
        """
        Get an evaluation parameter value that has been stored in meta.

        Args:
            parameter_name (string): The name of the parameter to store.
            default_value (any): The default value to be returned if the parameter is not found.

        Returns:
            The current value of the evaluation parameter.
        """
        if parameter_name in self._expectation_suite.evaluation_parameters:
            return self._expectation_suite.evaluation_parameters[parameter_name]
        else:
            return default_value

    def set_evaluation_parameter(self, parameter_name, parameter_value) -> None:
        """
        Provide a value to be stored in the data_asset evaluation_parameters object and used to evaluate
        parameterized expectations.

        Args:
            parameter_name (string): The name of the kwarg to be replaced at evaluation time
            parameter_value (any): The value to be used
        """
        self._expectation_suite.evaluation_parameters.update(
            {parameter_name: convert_to_json_serializable(parameter_value)}
        )

    def add_citation(  # noqa: PLR0913
        self,
        comment: str,
        batch_spec: Optional[dict] = None,
        batch_markers: Optional[dict] = None,
        batch_definition: Optional[dict] = None,
        citation_date: Optional[str] = None,
    ) -> None:
        """Adds a citation to an existing Expectation Suite within the validator"""
        if batch_spec is None:
            batch_spec = self.batch_spec
        if batch_markers is None:
            batch_markers = self.active_batch_markers
        if batch_definition is None:
            batch_definition = self.active_batch_definition
        self._expectation_suite.add_citation(
            comment,
            batch_spec=batch_spec,
            batch_markers=batch_markers,
            batch_definition=batch_definition,
            citation_date=citation_date,
        )

    def test_expectation_function(
        self, function: Callable, *args, **kwargs
    ) -> Callable:
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
        """

        # noinspection SpellCheckingInspection
        argspec = inspect.getfullargspec(function)[0][1:]

        new_function = self.expectation(argspec)(function)
        return new_function(self, *args, **kwargs)

    @staticmethod
    def _parse_validation_graph(
        validation_graph: ValidationGraph,
        metrics: Dict[Tuple[str, str, str], MetricValue],
    ) -> Tuple[Set[MetricConfiguration], Set[MetricConfiguration]]:
        """Given validation graph, returns the ready and needed metrics necessary for validation using a traversal of
        validation graph (a graph structure of metric ids) edges"""
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
                else:
                    if edge.left.id not in unmet_dependency_ids:  # noqa: PLR5501
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
                The name to assign to the `expectation_suite.expectation_suite_name`

        Returns:
            None
        """
        # Checking type of expectation_suite.
        # Check for expectation_suite_name is already done by ExpectationSuiteIdentifier
        if expectation_suite and not isinstance(expectation_suite, ExpectationSuite):
            raise TypeError(
                "expectation_suite must be of type ExpectationSuite, not {}".format(
                    type(expectation_suite)
                )
            )
        if expectation_suite is not None:
            if isinstance(expectation_suite, dict):
                expectation_suite_dict: dict = expectationSuiteSchema.load(
                    expectation_suite
                )
                expectation_suite = ExpectationSuite(
                    **expectation_suite_dict, data_context=self._data_context
                )
            else:
                expectation_suite = copy.deepcopy(expectation_suite)
            self._expectation_suite = expectation_suite

            if expectation_suite_name is not None:
                if (
                    self._expectation_suite.expectation_suite_name
                    != expectation_suite_name
                ):
                    logger.warning(
                        "Overriding existing expectation_suite_name {n1} with new name {n2}".format(
                            n1=self._expectation_suite.expectation_suite_name,
                            n2=expectation_suite_name,
                        )
                    )
                self._expectation_suite.expectation_suite_name = expectation_suite_name

        else:
            if expectation_suite_name is None:
                expectation_suite_name = "default"
            self._expectation_suite = ExpectationSuite(
                expectation_suite_name=expectation_suite_name,
                data_context=self._data_context,
            )

        self._expectation_suite.execution_engine_type = type(
            self._execution_engine
        ).__name__

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
        else:
            if result_format is not None:  # noqa: PLR5501
                runtime_configuration.update({"result_format": result_format})

        return runtime_configuration

    @staticmethod
    def _calc_validation_statistics(
        validation_results: List[ExpectationValidationResult],
    ) -> ValidationStatistics:
        """
        Calculate summary statistics for the validation results and
        return ``ExpectationStatistics``.
        """
        # calc stats
        evaluated_expectations = len(validation_results)
        successful_expectations = sum(exp.success for exp in validation_results)
        unsuccessful_expectations = evaluated_expectations - successful_expectations
        success = successful_expectations == evaluated_expectations
        try:
            success_percent = successful_expectations / evaluated_expectations * 100
        except ZeroDivisionError:
            success_percent = None

        return ValidationStatistics(
            successful_expectations=successful_expectations,
            evaluated_expectations=evaluated_expectations,
            unsuccessful_expectations=unsuccessful_expectations,
            success=success,
            success_percent=success_percent,
        )

    def convert_to_checkpoint_validations_list(
        self,
    ) -> list[CheckpointValidationConfig]:
        """
        Generates a list of validations to be used in the construction of a Checkpoint.

        Returns:
            A list of CheckpointValidationConfigs (one for each batch in the Validator).
        """
        validations = []
        for batch in self.batch_cache.values():
            validation = CheckpointValidationConfig(
                expectation_suite_name=self.expectation_suite_name,
                expectation_suite_ge_cloud_id=self.expectation_suite.ge_cloud_id,
                batch_request=batch.batch_request,
            )
            validations.append(validation)

        return validations


class BridgeValidator:
    """This is currently helping bridge APIs"""

    def __init__(
        self, batch, expectation_suite, expectation_engine=None, **kwargs
    ) -> None:
        """Builds an expectation_engine object using an expectation suite and a batch, with the expectation engine being
        determined either by the user or by the type of batch data (pandas dataframe, SqlAlchemy table, etc.)

        Args:
            batch (Batch): A Batch in Pandas, Spark, or SQL format
            expectation_suite (ExpectationSuite): The Expectation Suite available to the validator within the current
            Data Context
            expectation_engine (ExecutionEngine): The current Execution Engine being utilized. If this is not set, it is
            determined by the type of data within the given batch
        """
        self.batch = batch
        self._expectation_suite = expectation_suite

        if isinstance(expectation_engine, dict):
            expectation_engine = ClassConfig(**expectation_engine)

        if isinstance(expectation_engine, ClassConfig):
            module_name = expectation_engine.module_name or "great_expectations.dataset"
            verify_dynamic_loading_support(module_name=module_name)
            expectation_engine = load_class(
                class_name=expectation_engine.class_name, module_name=module_name
            )

        self.expectation_engine = expectation_engine

        if self.expectation_engine is None:
            # Guess the engine
            if pd is not None:
                if isinstance(batch.data, pd.DataFrame):
                    self.expectation_engine = PandasDataset

        if self.expectation_engine is None:
            from great_expectations.compatibility import pyspark

            if pyspark.DataFrame and isinstance(batch.data, pyspark.DataFrame):
                self.expectation_engine = SparkDFDataset

        if self.expectation_engine is None:
            raise ValueError(
                "Unable to identify expectation_engine. It must be a subclass of DataAsset."
            )

        self.init_kwargs = kwargs

    def get_dataset(self):
        """
        Bridges between Execution Engines in providing access to the batch data. Validates that Dataset classes
        contain proper type of data (i.e. a Pandas Dataset does not contain SqlAlchemy data)
        """
        if issubclass(self.expectation_engine, PandasDataset):
            if not isinstance(self.batch["data"], pd.DataFrame):
                raise ValueError(
                    "PandasDataset expectation_engine requires a Pandas Dataframe for its batch"
                )

            return self.expectation_engine(
                self.batch.data,
                expectation_suite=self._expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )

        elif issubclass(self.expectation_engine, SparkDFDataset):
            from great_expectations.compatibility import pyspark

            if not (
                pyspark.DataFrame and isinstance(self.batch.data, pyspark.DataFrame)
            ):
                raise ValueError(
                    "SparkDFDataset expectation_engine requires a spark DataFrame for its batch"
                )

            return self.expectation_engine(
                spark_df=self.batch.data,
                expectation_suite=self._expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )
