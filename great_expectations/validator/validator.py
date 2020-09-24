import copy
import datetime
import inspect
import json
import logging
import traceback
import warnings
from collections import defaultdict, namedtuple
from collections.abc import Hashable
from functools import wraps
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

import pandas as pd
from dateutil.parser import parse

from great_expectations import __version__ as ge_version
from great_expectations.core.batch import Batch
from great_expectations.core.evaluation_parameters import build_evaluation_parameters
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import (
    ExpectationSuite,
    expectationSuiteSchema,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
    ExpectationValidationResult,
)
from great_expectations.core.id_dict import IDDict
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_asset.util import recursively_convert_to_json_serializable
from great_expectations.dataset import PandasDataset, SparkDFDataset, SqlAlchemyDataset
from great_expectations.dataset.sqlalchemy_dataset import SqlAlchemyBatchReference
from great_expectations.exceptions import GreatExpectationsError
from great_expectations.exceptions.metric_exceptions import MetricError
from great_expectations.expectations.registry import (
    get_expectation_impl,
    get_metric_dependencies,
    get_metric_kwargs,
)
from great_expectations.marshmallow__shade import ValidationError
from great_expectations.types import ClassConfig
from great_expectations.util import load_class, verify_dynamic_loading_support
from great_expectations.validator.validation_graph import (
    MetricEdge,
    MetricEdgeKey,
    ValidationGraph,
)

logger = logging.getLogger(__name__)
logging.captureWarnings(True)


class Validator:
    def __init__(
        self,
        interactive_evaluation=True,
        profiler=None,
        expectation_suite=None,
        expectation_suite_name=None,
        execution_engine=None,
        batch=None,
        **kwargs,
    ):
        """
        Initialize the DataAsset.

        :param profiler (profiler class) = None: The profiler that should be run on the data_asset to
            build a baseline expectation suite.

        Note: DataAsset is designed to support multiple inheritance (e.g. PandasDataset inherits from both a
        Pandas DataFrame and Dataset which inherits from DataAsset), so it accepts generic *args and **kwargs arguments
        so that they can also be passed to other parent classes. In python 2, there isn't a clean way to include all of
        *args, **kwargs, and a named kwarg...so we use the inelegant solution of popping from kwargs, leaving the
        support for the profiler parameter not obvious from the signature.

        """

        self._batch = batch
        self._execution_engine = execution_engine

        if execution_engine:
            self._data_context = execution_engine.data_context
            self._execution_engine._validator = self

            if batch:
                if not execution_engine.batches.get(batch.to_id()):
                    execution_engine.batches[batch.to_id()] = batch
                execution_engine._loaded_batch_id = batch.to_id()

        else:
            self._data_context = None
        self._validator_config = {"interactive_evaluation": interactive_evaluation}
        self._initialize_expectations(
            expectation_suite=expectation_suite,
            expectation_suite_name=expectation_suite_name,
        )

        # This special state variable tracks whether a validation run is going on, which will disable
        # saving expectation config objects
        self._active_validation = False
        if profiler is not None:
            profiler.profile(self)
        if self._data_context and hasattr(
            self._data_context, "_expectation_explorer_manager"
        ):
            self.set_default_expectation_argument("include_config", True)

    def __getattr__(self, name):
        if name.startswith("expect_") and hasattr(self.execution_engine, name):
            return getattr(self.execution_engine, name)
        elif type(
            self.execution_engine
        ).__name__ == "PandasExecutionEngine" and hasattr(pd.DataFrame, name):
            return getattr(self.batch.data, name)
        else:
            raise AttributeError(
                f"'{type(self).__name__}'  object has no attribute '{name}'"
            )

    @property
    def execution_engine(self):
        return self._execution_engine

    def list_available_expectation_types(self):
        keys = dir(self)
        return [
            expectation for expectation in keys if expectation.startswith("expect_")
        ]

    def _populate_dependencies(
        self,
        graph: ValidationGraph,
        metric_name: str,
        configuration: ExpectationConfiguration,
        parent_node: Union[MetricEdgeKey, None] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> None:
        metric_kwargs = get_metric_kwargs(metric_name)
        configuration_kwargs = configuration.get_runtime_kwargs(
            runtime_configuration=runtime_configuration
        )
        try:
            if len(metric_kwargs["metric_domain_keys"]) > 0:
                metric_domain_kwargs = IDDict(
                    {
                        k: configuration_kwargs.get(k)
                        for k in metric_kwargs["metric_domain_keys"]
                    }
                )
            else:
                metric_domain_kwargs = IDDict()
            if len(metric_kwargs["metric_value_keys"]) > 0:
                metric_value_kwargs = IDDict(
                    {
                        k: configuration_kwargs.get(k)
                        for k in metric_kwargs["metric_value_keys"]
                    }
                )
            else:
                metric_value_kwargs = IDDict()
        except KeyError:
            raise MetricError(
                f"missing kwarg value while trying to identify dependency graph for metric {metric_name}"
            )
        # domain_id = metric_domain_kwargs.to_id()
        # if domain_id not in graph_family:
        #     graph_family[domain_id] = ValidationGraph(metric_domain_kwargs)

        # graph = graph_family[domain_id]
        metric_dependencies = get_metric_dependencies(metric_name)

        if parent_node:
            graph.add(
                MetricEdge(
                    parent_node,
                    MetricEdgeKey(
                        metric_name, metric_domain_kwargs, metric_value_kwargs
                    ),
                )
            )

        if len(metric_dependencies) == 0:
            graph.add(
                MetricEdge(
                    MetricEdgeKey(
                        metric_name, metric_domain_kwargs, metric_value_kwargs
                    ),
                    None,
                )
            )

        else:
            for dependent_metric in metric_dependencies:
                self._populate_dependencies(
                    graph,
                    dependent_metric,
                    configuration,
                    MetricEdgeKey(
                        metric_name, metric_domain_kwargs, metric_value_kwargs
                    ),
                    runtime_configuration=runtime_configuration,
                )

    def graph_validate(
        self,
        batches: Dict[str, Batch],
        configurations: List[ExpectationConfiguration],
        execution_engine: "ExecutionEngine" = None,
        metrics: dict = None,
        runtime_configuration: dict = None,
    ) -> List[ExpectationValidationResult]:
        if execution_engine is None:
            if self._execution_engine:
                execution_engine = self._execution_engine
            else:
                raise ValueError("Execution Engine is required for validation")
        graph = ValidationGraph()
        for configuration in configurations:
            expectation_impl = get_expectation_impl(configuration.expectation_type)
            validation_dependencies = expectation_impl(
                configuration
            ).get_validation_dependencies(
                configuration, execution_engine, runtime_configuration
            )

            for metric_name in validation_dependencies.get("metrics"):
                self._populate_dependencies(
                    graph,
                    metric_name,
                    configuration,
                    runtime_configuration=runtime_configuration,
                )

        if metrics is None:
            metrics = dict()

        done: bool = False
        while not done:
            ready_metrics, needed_metrics = self._parse_validation_graph(graph, metrics)
            metrics.update(
                self._resolve_metrics(
                    batches=batches,
                    execution_engine=execution_engine,
                    metrics_to_resolve=ready_metrics,
                    metrics=metrics,
                    runtime_configuration=runtime_configuration,
                )
            )
            if len(ready_metrics) + len(needed_metrics) == 0:
                done = True

        evrs = list()
        for configuration in configurations:
            evrs.append(
                configuration.metrics_validate(
                    metrics,
                    runtime_configuration=runtime_configuration,
                    execution_engine=execution_engine,
                )
            )
        return evrs

    def _parse_validation_graph(self, validation_graph, metrics):
        needed_metric_ids = set()
        needed_metrics = set()
        ready_metric_ids = set()
        ready_metrics = set()

        for edge in validation_graph.edges:
            if edge.left.id not in metrics:
                if edge.right is None or edge.right.id in metrics:
                    if edge.left.id not in ready_metric_ids:
                        ready_metric_ids.add(edge.left.id)
                        ready_metrics.add(edge.left)
                else:
                    if edge.left.id not in needed_metric_ids:
                        needed_metric_ids.add(edge.left.id)
                        needed_metrics.add(edge.left)

        return ready_metrics, needed_metrics

    def _resolve_metrics(
        self,
        batches: Dict[str, Batch],
        execution_engine: "ExecutionEngine",
        metrics_to_resolve: Iterable[MetricEdgeKey],
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        return execution_engine.resolve_metrics(
            batches, metrics_to_resolve, metrics, runtime_configuration
        )

    def autoinspect(self, profiler):
        """Deprecated: use profile instead.

        Use the provided profiler to evaluate this data_asset and assign the resulting expectation suite as its own.

        Args:
            profiler: The profiler to use

        Returns:
            tuple(expectation_suite, validation_results)
        """
        warnings.warn(
            "The term autoinspect is deprecated and will be removed in a future release. Please use 'profile'\
        instead."
        )
        expectation_suite, validation_results = profiler.profile(self)
        return expectation_suite, validation_results

    def profile(self, profiler, profiler_configuration=None):
        """Use the provided profiler to evaluate this data_asset and assign the resulting expectation suite as its own.

        Args:
            profiler: The profiler to use
            profiler_configuration: Optional profiler configuration dict

        Returns:
            tuple(expectation_suite, validation_results)

        """
        expectation_suite, validation_results = profiler.profile(
            self, profiler_configuration
        )
        return expectation_suite, validation_results

    # TODO: add warning if no expectation_explorer_manager and how to turn on
    def edit_expectation_suite(self):
        return self._data_context._expectation_explorer_manager.edit_expectation_suite(
            self
        )

    @classmethod
    def expectation(cls, method_arg_names):
        """Manages configuration and running of expectation objects.

        Expectation builds and saves a new expectation configuration to the DataAsset object. It is the core decorator \
        used by great expectations to manage expectation configurations.

        Args:
            method_arg_names (List) : An ordered list of the arguments used by the method implementing the expectation \
                (typically the result of inspection). Positional arguments are explicitly mapped to \
                keyword arguments when the expectation is run.

        Notes:
            Intermediate decorators that call the core @expectation decorator will most likely need to pass their \
            decorated methods' signature up to the expectation decorator. For example, the MetaPandasDataset \
            column_map_expectation decorator relies on the DataAsset expectation decorator, but will pass through the \
            signature from the implementing method.

            @expectation intercepts and takes action based on the following parameters:
                * include_config (boolean or None) : \
                    If True, then include the generated expectation config as part of the result object. \
                    For more detail, see :ref:`include_config`.
                * catch_exceptions (boolean or None) : \
                    If True, then catch exceptions and include them as part of the result object. \
                    For more detail, see :ref:`catch_exceptions`.
                * result_format (str or None) : \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                    For more detail, see :ref:`result_format <result_format>`.
                * meta (dict or None): \
                    A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                    modification. For more detail, see :ref:`meta`.
        """

        def outer_wrapper(func):
            @wraps(func)
            def wrapper(self, *args, **kwargs):

                # Get the name of the method
                method_name = func.__name__

                # Combine all arguments into a single new "all_args" dictionary to name positional parameters
                all_args = dict(zip(method_arg_names, args))
                all_args.update(kwargs)

                # Unpack display parameters; remove them from all_args if appropriate
                if "include_config" in kwargs:
                    include_config = kwargs["include_config"]
                    del all_args["include_config"]
                else:
                    include_config = self.default_expectation_args["include_config"]

                if "catch_exceptions" in kwargs:
                    catch_exceptions = kwargs["catch_exceptions"]
                    del all_args["catch_exceptions"]
                else:
                    catch_exceptions = self.default_expectation_args["catch_exceptions"]

                if "result_format" in kwargs:
                    result_format = kwargs["result_format"]
                else:
                    result_format = self.default_expectation_args["result_format"]

                # Extract the meta object for use as a top-level expectation_config holder
                if "meta" in kwargs:
                    meta = kwargs["meta"]
                    del all_args["meta"]
                else:
                    meta = None

                # Get the signature of the inner wrapper:
                argspec = inspect.getfullargspec(func)[0][1:]

                if "result_format" in argspec:
                    all_args["result_format"] = result_format
                else:
                    if "result_format" in all_args:
                        del all_args["result_format"]

                all_args = recursively_convert_to_json_serializable(all_args)

                # Patch in PARAMETER args, and remove locally-supplied arguments
                # This will become the stored config
                expectation_args = copy.deepcopy(all_args)

                if self._expectation_suite.evaluation_parameters:
                    (
                        evaluation_args,
                        substituted_parameters,
                    ) = build_evaluation_parameters(
                        expectation_args,
                        self._expectation_suite.evaluation_parameters,
                        self._validator_config.get("interactive_evaluation", True),
                        self._data_context,
                    )
                else:
                    (
                        evaluation_args,
                        substituted_parameters,
                    ) = build_evaluation_parameters(
                        expectation_args,
                        None,
                        self._validator_config.get("interactive_evaluation", True),
                        self._data_context,
                    )

                # Construct the expectation_config object
                expectation_config = ExpectationConfiguration(
                    expectation_type=method_name, kwargs=expectation_args, meta=meta
                )

                raised_exception = False
                exception_traceback = None
                exception_message = None

                # Finally, execute the expectation method itself
                if (
                    self._validator_config.get("interactive_evaluation", True)
                    or self._active_validation
                ):
                    try:
                        return_obj = func(self, **evaluation_args)
                        if isinstance(return_obj, dict):
                            return_obj = ExpectationValidationResult(**return_obj)

                    except Exception as err:
                        if catch_exceptions:
                            raised_exception = True
                            exception_traceback = traceback.format_exc()
                            exception_message = "{}: {}".format(
                                type(err).__name__, str(err)
                            )

                            return_obj = ExpectationValidationResult(success=False)

                        else:
                            raise err

                else:
                    return_obj = ExpectationValidationResult(
                        expectation_config=copy.deepcopy(expectation_config)
                    )

                # If validate has set active_validation to true, then we do not save the config to avoid
                # saving updating expectation configs to the same suite during validation runs
                if self._active_validation is True:
                    stored_config = expectation_config
                else:
                    # Append the expectation to the config.
                    stored_config = self._expectation_suite.add_expectation(
                        expectation_config
                    )

                if include_config:
                    return_obj.expectation_config = copy.deepcopy(stored_config)

                # If there was no interactive evaluation, success will not have been computed.
                if return_obj.success is not None:
                    # Add a "success" object to the config
                    stored_config.success_on_last_run = return_obj.success

                if catch_exceptions:
                    return_obj.exception_info = {
                        "raised_exception": raised_exception,
                        "exception_message": exception_message,
                        "exception_traceback": exception_traceback,
                    }

                if len(substituted_parameters) > 0:
                    if meta is None:
                        meta = dict()
                    meta["substituted_parameters"] = substituted_parameters

                # Add meta to return object
                if meta is not None:
                    return_obj.meta = meta

                return_obj = recursively_convert_to_json_serializable(return_obj)

                if self._data_context is not None:
                    return_obj = self._data_context.update_return_obj(self, return_obj)

                return return_obj

            return wrapper

        return outer_wrapper

    def _initialize_expectations(
        self, expectation_suite=None, expectation_suite_name=None
    ):
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
        if expectation_suite is not None:
            if isinstance(expectation_suite, dict):
                expectation_suite = expectationSuiteSchema.load(expectation_suite)
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
                expectation_suite_name=expectation_suite_name
            )

        self._expectation_suite.execution_engine_type = type(
            self.execution_engine
        ).__name__

    def append_expectation(self, expectation_config):
        """This method is a thin wrapper for ExpectationSuite.append_expectation"""
        warnings.warn(
            "append_expectation is deprecated, and will be removed in a future release. "
            + "Please use ExpectationSuite.add_expectation instead.",
            DeprecationWarning,
        )
        self._expectation_suite.append_expectation(expectation_config)

    def find_expectation_indexes(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
    ) -> List[int]:
        """This method is a thin wrapper for ExpectationSuite.find_expectation_indexes"""
        warnings.warn(
            "find_expectation_indexes is deprecated, and will be removed in a future release. "
            + "Please use ExpectationSuite.find_expectation_indexes instead.",
            DeprecationWarning,
        )
        return self._expectation_suite.find_expectation_indexes(
            expectation_configuration=expectation_configuration, match_type=match_type
        )

    def find_expectations(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
    ) -> List[ExpectationConfiguration]:
        """This method is a thin wrapper for ExpectationSuite.find_expectations()"""
        warnings.warn(
            "find_expectations is deprecated, and will be removed in a future release. "
            + "Please use ExpectationSuite.find_expectation_indexes instead.",
            DeprecationWarning,
        )
        return self._expectation_suite.find_expectations(
            expectation_configuration=expectation_configuration, match_type=match_type
        )

    def remove_expectation(
        self,
        expectation_configuration: ExpectationConfiguration,
        match_type: str = "domain",
        remove_multiple_matches: bool = False,
    ) -> List[ExpectationConfiguration]:
        """This method is a thin wrapper for ExpectationSuite.remove()"""
        warnings.warn(
            "DataAsset.remove_expectations is deprecated, and will be removed in a future release. "
            + "Please use ExpectationSuite.remove_expectation instead.",
            DeprecationWarning,
        )
        return self._expectation_suite.remove_expectation(
            expectation_configuration=expectation_configuration,
            match_type=match_type,
            remove_multiple_matches=remove_multiple_matches,
        )

    def set_config_value(self, key, value):
        self._validator_config[key] = value

    def get_config_value(self, key):
        return self._validator_config[key]

    @property
    def batch(self):
        if self._batch:
            return self._batch
        else:
            return self.execution_engine.loaded_batch

    @property
    def batch_spec(self):
        if not self.batch:
            return None
        else:
            return self.batch.batch_spec

    @property
    def batch_id(self):
        return self.batch_spec.to_id()

    @property
    def batch_markers(self):
        if not self.batch:
            return None
        else:
            return self.batch.batch_markers

    @property
    def batch_definition(self):
        return self.batch.batch_definition

    def discard_failing_expectations(self):
        res = self.validate(only_return_failures=True).results
        if any(res):
            for item in res:
                self.remove_expectation(
                    expectation_configuration=item.expectation_config,
                    match_type="runtime",
                )
            warnings.warn("Removed %s expectations that were 'False'" % len(res))

    def get_default_expectation_arguments(self):
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
        return self.execution_engine.default_expectation_args

    @property
    def default_expectation_args(self):
        return self.execution_engine.default_expectation_args

    def set_default_expectation_argument(self, argument, value):
        """Set a default expectation argument for this data_asset

        Args:
            argument (string): The argument to be replaced
            value : The New argument to use for replacement

        Returns:
            None

        See also:
            get_default_expectation_arguments
        """
        # !!! Maybe add a validation check here?

        self.default_expectation_args[argument] = value

    def get_expectations_config(
        self,
        discard_failed_expectations=True,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False,
    ):
        warnings.warn(
            "get_expectations_config is deprecated, and will be removed in a future release. "
            + "Please use get_expectation_suite instead.",
            DeprecationWarning,
        )
        return self.get_expectation_suite(
            discard_failed_expectations,
            discard_result_format_kwargs,
            discard_include_config_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings,
        )

    def get_expectation_suite(
        self,
        discard_failed_expectations=True,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False,
        suppress_logging=False,
    ):
        """Returns _expectation_config as a JSON object, and perform some cleaning along the way.

        Args:
            discard_failed_expectations (boolean): \
                Only include expectations with success_on_last_run=True in the exported config.  Defaults to `True`.
            discard_result_format_kwargs (boolean): \
                In returned expectation objects, suppress the `result_format` parameter. Defaults to `True`.
            discard_include_config_kwargs (boolean): \
                In returned expectation objects, suppress the `include_config` parameter. Defaults to `True`.
            discard_catch_exceptions_kwargs (boolean): \
                In returned expectation objects, suppress the `catch_exceptions` parameter.  Defaults to `True`.
            suppress_warnings (boolean): \
                If true, do not include warnings in logging information about the operation.
            suppress_logging (boolean): \
                If true, do not create a log entry (useful when using get_expectation_suite programmatically)

        Returns:
            An expectation suite.

        Note:
            get_expectation_suite does not affect the underlying expectation suite at all. The returned suite is a \
             copy of _expectation_suite, not the original object.
        """

        expectation_suite = copy.deepcopy(self._expectation_suite)
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

        message = "\t%d expectation(s) included in expectation_suite." % len(
            expectations
        )

        if discards["failed_expectations"] > 0 and not suppress_warnings:
            message += (
                " Omitting %d expectation(s) that failed when last run; set "
                "discard_failed_expectations=False to include them."
                % discards["failed_expectations"]
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

    def save_expectation_suite(
        self,
        filepath=None,
        discard_failed_expectations=True,
        discard_result_format_kwargs=True,
        discard_include_config_kwargs=True,
        discard_catch_exceptions_kwargs=True,
        suppress_warnings=False,
    ):
        """Writes ``_expectation_config`` to a JSON file.

           Writes the DataAsset's expectation config to the specified JSON ``filepath``. Failing expectations \
           can be excluded from the JSON expectations config with ``discard_failed_expectations``. The kwarg key-value \
           pairs :ref:`result_format`, :ref:`include_config`, and :ref:`catch_exceptions` are optionally excluded from \
           the JSON expectations config.

           Args:
               filepath (string): \
                   The location and name to write the JSON config file to.
               discard_failed_expectations (boolean): \
                   If True, excludes expectations that do not return ``success = True``. \
                   If False, all expectations are written to the JSON config file.
               discard_result_format_kwargs (boolean): \
                   If True, the :ref:`result_format` attribute for each expectation is not written to the JSON config \
                   file.
               discard_include_config_kwargs (boolean): \
                   If True, the :ref:`include_config` attribute for each expectation is not written to the JSON config \
                   file.
               discard_catch_exceptions_kwargs (boolean): \
                   If True, the :ref:`catch_exceptions` attribute for each expectation is not written to the JSON \
                   config file.
               suppress_warnings (boolean): \
                  It True, all warnings raised by Great Expectations, as a result of dropped expectations, are \
                  suppressed.

        """
        expectation_suite = self.get_expectation_suite(
            discard_failed_expectations,
            discard_result_format_kwargs,
            discard_include_config_kwargs,
            discard_catch_exceptions_kwargs,
            suppress_warnings,
        )
        if filepath is None and self._data_context is not None:
            self._data_context.save_expectation_suite(expectation_suite)
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

    def validate(
        self,
        expectation_suite=None,
        run_id=None,
        data_context=None,
        evaluation_parameters=None,
        catch_exceptions=True,
        result_format=None,
        only_return_failures=False,
        run_name=None,
        run_time=None,
    ):
        """Generates a JSON-formatted report describing the outcome of all expectations.

        Use the default expectation_suite=None to validate the expectations config associated with the DataAsset.

        Args:
            expectation_suite (json or None): \
                If None, uses the expectations config generated with the DataAsset during the current session. \
                If a JSON file, validates those expectations.
            run_name (str): \
                Used to identify this validation result as part of a collection of validations. \
                See DataContext for more information.
            data_context (DataContext): \
                A datacontext object to use as part of validation for binding evaluation parameters and \
                registering validation results.
            evaluation_parameters (dict or None): \
                If None, uses the evaluation_paramters from the expectation_suite provided or as part of the \
                data_asset. If a dict, uses the evaluation parameters in the dictionary.
            catch_exceptions (boolean): \
                If True, exceptions raised by tests will not end validation and will be described in the returned \
                report.
            result_format (string or None): \
                If None, uses the default value ('BASIC' or as specified). \
                If string, the returned expectation output follows the specified format ('BOOLEAN_ONLY','BASIC', \
                etc.).
            only_return_failures (boolean): \
                If True, expectation results are only returned when ``success = False`` \

        Returns:
            A JSON-formatted dictionary containing a list of the validation results. \
            An example of the returned format::

            {
              "results": [
                {
                  "unexpected_list": [unexpected_value_1, unexpected_value_2],
                  "expectation_type": "expect_*",
                  "kwargs": {
                    "column": "Column_Name",
                    "output_format": "SUMMARY"
                  },
                  "success": true,
                  "raised_exception: false.
                  "exception_traceback": null
                },
                {
                  ... (Second expectation results)
                },
                ... (More expectations results)
              ],
              "success": true,
              "statistics": {
                "evaluated_expectations": n,
                "successful_expectations": m,
                "unsuccessful_expectations": n - m,
                "success_percent": m / n
              }
            }

        Notes:
           If the configuration object was built with a different version of great expectations then the \
           current environment. If no version was found in the configuration file.

        Raises:
           AttributeError - if 'catch_exceptions'=None and an expectation throws an AttributeError
        """
        try:
            validation_time = datetime.datetime.now(datetime.timezone.utc).strftime(
                "%Y%m%dT%H%M%S.%fZ"
            )

            assert not (run_id and run_name) and not (
                run_id and run_time
            ), "Please provide either a run_id or run_name and/or run_time."
            if isinstance(run_id, str) and not run_name:
                warnings.warn(
                    "String run_ids will be deprecated in the future. Please provide a run_id of type "
                    "RunIdentifier(run_name=None, run_time=None), or a dictionary containing run_name "
                    "and run_time (both optional). Instead of providing a run_id, you may also provide"
                    "run_name and run_time separately.",
                    DeprecationWarning,
                )
                try:
                    run_time = parse(run_id)
                except (ValueError, TypeError):
                    pass
                run_id = RunIdentifier(run_name=run_id, run_time=run_time)
            elif isinstance(run_id, dict):
                run_id = RunIdentifier(**run_id)
            elif not isinstance(run_id, RunIdentifier):
                run_id = RunIdentifier(run_name=run_name, run_time=run_time)

            self._active_validation = True

            # If a different validation data context was provided, override
            validate__data_context = self._data_context
            if data_context is None and self._data_context is not None:
                data_context = self._data_context
            elif data_context is not None:
                # temporarily set self._data_context so it is used inside the expectation decorator
                self._data_context = data_context

            results = []

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
                        "Unable to load expectation suite: IO error while reading %s"
                        % expectation_suite
                    )
            elif not isinstance(expectation_suite, ExpectationSuite):
                logger.error(
                    "Unable to validate using the provided value for expectation suite; does it need to be "
                    "loaded from a dictionary?"
                )
                if getattr(data_context, "_usage_statistics_handler", None):
                    handler = data_context._usage_statistics_handler
                    handler.send_usage_message(
                        event="data_asset.validate",
                        event_payload=handler._batch_anonymizer.anonymize_batch_info(
                            self
                        ),
                        success=False,
                    )
                return ExpectationValidationResult(success=False)
            # Evaluation parameter priority is
            # 1. from provided parameters
            # 2. from expectation configuration
            # 3. from data context
            # So, we load them in reverse order

            if data_context is not None:
                runtime_evaluation_parameters = data_context.evaluation_parameter_store.get_bind_params(
                    run_id
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
            suite_ge_version = expectation_suite.meta.get(
                "great_expectations_version"
            ) or expectation_suite.meta.get("great_expectations.__version__")

            if suite_ge_version:
                if suite_ge_version != ge_version:
                    warnings.warn(
                        "WARNING: This configuration object was built using version %s of great_expectations, but "
                        "is currently being validated by version %s."
                        % (suite_ge_version, ge_version,)
                    )
            else:
                warnings.warn(
                    "WARNING: No great_expectations version found in configuration object."
                )

            ###
            # This is an early example of what will become part of the ValidationOperator
            # This operator would be dataset-semantic aware
            # Adding now to simply ensure we can be slightly better at ordering our expectation evaluation
            ###

            # Group expectations by column
            columns = {}

            for expectation in expectation_suite.expectations:
                if "column" in expectation.kwargs and isinstance(
                    expectation.kwargs["column"], Hashable
                ):
                    column = expectation.kwargs["column"]
                else:
                    column = "_nocolumn"
                if column not in columns:
                    columns[column] = []
                columns[column].append(expectation)

            expectations_to_evaluate = []
            for col in columns:
                expectations_to_evaluate.extend(columns[col])

            for expectation in expectations_to_evaluate:

                try:
                    # copy the config so we can modify it below if needed
                    expectation = copy.deepcopy(expectation)

                    expectation_method = getattr(self, expectation.expectation_type)

                    if result_format is not None:
                        expectation.kwargs.update({"result_format": result_format})

                    # A missing parameter will raise an EvaluationParameterError
                    (
                        evaluation_args,
                        substituted_parameters,
                    ) = build_evaluation_parameters(
                        expectation.kwargs,
                        runtime_evaluation_parameters,
                        self._validator_config.get("interactive_evaluation", True),
                        self._data_context,
                    )

                    result = expectation_method(
                        catch_exceptions=catch_exceptions,
                        include_config=True,
                        **evaluation_args,
                    )

                except Exception as err:
                    if catch_exceptions:
                        raised_exception = True
                        exception_traceback = traceback.format_exc()

                        result = ExpectationValidationResult(
                            success=False,
                            exception_info={
                                "raised_exception": raised_exception,
                                "exception_traceback": exception_traceback,
                                "exception_message": str(err),
                            },
                        )

                    else:
                        raise err

                # if include_config:
                result.expectation_config = expectation

                # Add an empty exception_info object if no exception was caught
                if catch_exceptions and result.exception_info is None:
                    result.exception_info = {
                        "raised_exception": False,
                        "exception_traceback": None,
                        "exception_message": None,
                    }

                results.append(result)

            statistics = _calc_validation_statistics(results)

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
                    "batch_spec": self.batch_spec,
                    "batch_markers": self.batch_markers,
                    "batch_definition": self.batch_definition,
                    "validation_time": validation_time,
                },
            )

            self._data_context = validate__data_context
        except Exception:
            if getattr(data_context, "_usage_statistics_handler", None):
                handler = data_context._usage_statistics_handler
                handler.send_usage_message(
                    event="data_asset.validate",
                    event_payload=handler._batch_anonymizer.anonymize_batch_info(self),
                    success=False,
                )
            raise
        finally:
            self._active_validation = False

        if getattr(data_context, "_usage_statistics_handler", None):
            handler = data_context._usage_statistics_handler
            handler.send_usage_message(
                event="data_asset.validate",
                event_payload=handler._batch_anonymizer.anonymize_batch_info(self),
                success=True,
            )
        return result

    def get_evaluation_parameter(self, parameter_name, default_value=None):
        """Get an evaluation parameter value that has been stored in meta.

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

    def set_evaluation_parameter(self, parameter_name, parameter_value):
        """Provide a value to be stored in the data_asset evaluation_parameters object and used to evaluate
        parameterized expectations.

        Args:
            parameter_name (string): The name of the kwarg to be replaced at evaluation time
            parameter_value (any): The value to be used
        """
        self._expectation_suite.evaluation_parameters.update(
            {parameter_name: parameter_value}
        )

    def add_citation(
        self,
        comment,
        batch_spec=None,
        batch_markers=None,
        batch_definition=None,
        citation_date=None,
    ):
        if batch_spec is None:
            batch_spec = self.batch_spec
        if batch_markers is None:
            batch_markers = self.batch_markers
        if batch_definition is None:
            batch_definition = self.batch_definition
        self._expectation_suite.add_citation(
            comment,
            batch_spec=batch_spec,
            batch_markers=batch_markers,
            batch_definition=batch_definition,
            citation_date=citation_date,
        )

    @property
    def expectation_suite_name(self):
        """Gets the current expectation_suite name of this data_asset as stored in the expectations configuration."""
        return self._expectation_suite.expectation_suite_name

    @expectation_suite_name.setter
    def expectation_suite_name(self, expectation_suite_name):
        """Sets the expectation_suite name of this data_asset as stored in the expectations configuration."""
        self._expectation_suite.expectation_suite_name = expectation_suite_name

    def test_expectation_function(self, function, *args, **kwargs):
        """Test a generic expectation function

        Args:
            function (func): The function to be tested. (Must be a valid expectation function.)
            *args          : Positional arguments to be passed the the function
            **kwargs       : Keyword arguments to be passed the the function

        Returns:
            A JSON-serializable expectation result object.

        Notes:
            This function is a thin layer to allow quick testing of new expectation functions, without having to \
            define custom classes, etc. To use developed expectations from the command-line tool, you will still need \
            to define custom classes, etc.

            Check out :ref:`how_to_guides__creating_and_editing_expectations__how_to_create_custom_expectations` for more information.
        """

        argspec = inspect.getfullargspec(function)[0][1:]

        new_function = self.expectation(argspec)(function)
        return new_function(self, *args, **kwargs)


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


def _calc_validation_statistics(validation_results):
    """
    Calculate summary statistics for the validation results and
    return ``ExpectationStatistics``.
    """
    # calc stats
    successful_expectations = sum(exp.success for exp in validation_results)
    evaluated_expectations = len(validation_results)
    unsuccessful_expectations = evaluated_expectations - successful_expectations
    success = successful_expectations == evaluated_expectations
    try:
        success_percent = successful_expectations / evaluated_expectations * 100
    except ZeroDivisionError:
        # success_percent = float("nan")
        success_percent = None

    return ValidationStatistics(
        successful_expectations=successful_expectations,
        evaluated_expectations=evaluated_expectations,
        unsuccessful_expectations=unsuccessful_expectations,
        success=success,
        success_percent=success_percent,
    )


"""This is currently helping bridge APIs"""


class BridgeValidator:
    def __init__(self, batch, expectation_suite, expectation_engine=None, **kwargs):
        self.batch = batch
        self.expectation_suite = expectation_suite

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
            try:
                import pandas as pd

                if isinstance(batch.data, pd.DataFrame):
                    self.expectation_engine = PandasDataset
            except ImportError:
                pass
        if self.expectation_engine is None:
            if isinstance(batch.data, SqlAlchemyBatchReference):
                self.expectation_engine = SqlAlchemyDataset

        if self.expectation_engine is None:
            try:
                import pyspark

                if isinstance(batch.data, pyspark.sql.DataFrame):
                    self.expectation_engine = SparkDFDataset
            except ImportError:
                pass

        if self.expectation_engine is None:
            raise ValueError(
                "Unable to identify expectation_engine. It must be a subclass of DataAsset."
            )

        self.init_kwargs = kwargs

    def get_dataset(self):
        if issubclass(self.expectation_engine, PandasDataset):
            import pandas as pd

            if not isinstance(self.batch["data"], pd.DataFrame):
                raise ValueError(
                    "PandasDataset expectation_engine requires a Pandas Dataframe for its batch"
                )

            return self.expectation_engine(
                self.batch.data,
                expectation_suite=self.expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )

        elif issubclass(self.expectation_engine, SqlAlchemyDataset):
            if not isinstance(self.batch.data, SqlAlchemyBatchReference):
                raise ValueError(
                    "SqlAlchemyDataset expectation_engine requires a SqlAlchemyBatchReference for its batch"
                )

            init_kwargs = self.batch.data.get_init_kwargs()
            init_kwargs.update(self.init_kwargs)
            return self.expectation_engine(
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                expectation_suite=self.expectation_suite,
                **init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )

        elif issubclass(self.expectation_engine, SparkDFDataset):
            import pyspark

            if not isinstance(self.batch.data, pyspark.sql.DataFrame):
                raise ValueError(
                    "SparkDFDataset expectation_engine requires a spark DataFrame for its batch"
                )

            return self.expectation_engine(
                spark_df=self.batch.data,
                expectation_suite=self.expectation_suite,
                batch_kwargs=self.batch.batch_kwargs,
                batch_parameters=self.batch.batch_parameters,
                batch_markers=self.batch.batch_markers,
                data_context=self.batch.data_context,
                **self.init_kwargs,
                **self.batch.batch_kwargs.get("dataset_options", {}),
            )
