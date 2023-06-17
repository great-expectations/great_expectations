import logging
from functools import wraps
from typing import Callable, Dict, Optional, Tuple, Type, Union

import great_expectations.exceptions as gx_exceptions
from great_expectations.core import ExpectationConfiguration
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import (
    MetricFunctionTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.metrics import MetaMetricProvider
from great_expectations.expectations.registry import (
    get_metric_provider,
    register_metric,
    register_renderer,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)


@public_api
def metric_value(
    engine: Type[ExecutionEngine],
    metric_fn_type: Union[str, MetricFunctionTypes] = MetricFunctionTypes.VALUE,
    **kwargs,
):
    """Decorator used to register a specific function as a metric value function.

    Metric value functions are used by MetricProviders to immediately return
    the value of the requested metric.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/overview

    Args:
        engine: the *type* of ExecutionEngine that this partial supports
        metric_fn_type: the type of metric function. Usually the default value should be maintained.

    Returns:
        Decorated function
    """

    def wrapper(metric_fn: Callable):
        @wraps(metric_fn)
        def inner_func(*args, **kwargs):
            return metric_fn(*args, **kwargs)

        inner_func.metric_engine = engine  # type: ignore[attr-defined]
        inner_func.metric_fn_type = MetricFunctionTypes(metric_fn_type)  # type: ignore[attr-defined]
        inner_func.metric_definition_kwargs = kwargs  # type: ignore[attr-defined]
        return inner_func

    return wrapper


@public_api
def metric_partial(
    engine: Type[ExecutionEngine],
    partial_fn_type: Union[str, MetricPartialFunctionTypes],
    domain_type: Union[str, MetricDomainTypes],
    **kwargs,
):
    """Decorator used to register a specific function as a metric partial.

    Metric partial functions are used by MetricProviders to support batching of
    requests for multiple metrics in an ExecutionEngine. Instead of returning
    the metric value immediately, they return a different function that the
    ExecutionEngine can execute locally on your data to obtain the metric value.

    ---Documentation---
        - https://docs.greatexpectations.io/docs/guides/expectations/features_custom_expectations/how_to_add_spark_support_for_an_expectation
        - https://docs.greatexpectations.io/docs/guides/expectations/features_custom_expectations/how_to_add_sqlalchemy_support_for_an_expectation

    Args:
        engine: the *type* of ExecutionEngine that this partial supports
        partial_fn_type: the type of partial function
        domain_type: the type of domain this metric function processes

    Returns:
        Decorated function
    """

    def wrapper(metric_fn: Callable):
        @wraps(metric_fn)
        def inner_func(*args, **kwargs):
            return metric_fn(*args, **kwargs)

        inner_func.metric_engine = engine  # type: ignore[attr-defined]
        inner_func.metric_fn_type = MetricPartialFunctionTypes(  # type: ignore[attr-defined]
            partial_fn_type
        )  # raises ValueError if unknown type
        inner_func.domain_type = MetricDomainTypes(domain_type)  # type: ignore[attr-defined]
        inner_func.metric_definition_kwargs = kwargs  # type: ignore[attr-defined]
        return inner_func

    return wrapper


@public_api
class MetricProvider(metaclass=MetaMetricProvider):
    """Base class for all metric providers.

    MetricProvider classes *must* have the following attributes set:
        1. `metric_name`: the name to use. Metric Name must be globally unique in
           a great_expectations installation.
        2. `domain_keys`: a tuple of the *keys* used to determine the domain of the
           metric
        3. `value_keys`: a tuple of the *keys* used to determine the value of
           the metric.

    In some cases, subclasses of Expectation, such as TableMetricProvider will already
    have correct values that may simply be inherited.

    They *may* optionally override the `default_kwarg_values` attribute.

    MetricProvider classes *must* implement the following:
        1. `_get_evaluation_dependencies`. Note that often, _get_evaluation_dependencies should
        augment dependencies provided by a parent class; consider calling super()._get_evaluation_dependencies

    In some cases, subclasses of Expectation, such as MapMetricProvider will already
    have correct implementations that may simply be inherited.

    Additionally, they *may* provide implementations of:
        1. Data Docs rendering methods decorated with the @renderer decorator. See the guide
        "How to create renderers for custom expectations" for more information.

    """

    domain_keys: Tuple[str, ...] = tuple()
    value_keys: Tuple[str, ...] = tuple()
    default_kwarg_values: dict = {}

    @classmethod
    def _register_metric_functions(cls) -> None:
        metric_name = getattr(cls, "metric_name", None)
        if not metric_name:
            # No metric name has been defined
            return

        metric_domain_keys = cls.domain_keys
        metric_value_keys = cls.value_keys

        for attr_name in dir(cls):
            attr_obj = getattr(cls, attr_name)
            if not (
                hasattr(attr_obj, "metric_engine")
                or hasattr(attr_obj, "_renderer_type")
            ):
                # This is not a metric or renderer.
                continue

            if hasattr(attr_obj, "metric_engine"):
                engine = getattr(attr_obj, "metric_engine")
                if not issubclass(engine, ExecutionEngine):
                    raise ValueError(
                        "metric functions must be defined with an Execution Engine"
                    )

                metric_fn = attr_obj
                metric_definition_kwargs = getattr(
                    metric_fn, "metric_definition_kwargs", {}
                )
                declared_metric_name = metric_name + metric_definition_kwargs.get(
                    "metric_name_suffix", ""
                )
                metric_fn_type: Optional[
                    Union[MetricFunctionTypes, MetricPartialFunctionTypes]
                ] = getattr(metric_fn, "metric_fn_type", MetricFunctionTypes.VALUE)

                if not metric_fn_type:
                    # This is not a metric (valid metrics possess exectly one metric function).
                    return

                """
                Basic metric implementations (defined by specifying "metric_name" class variable in "metric_class") use
                either "@metric_value" decorator (with default "metric_fn_type" set to "MetricFunctionTypes.VALUE"); or
                "@metric_partial" decorator with specification "partial_fn_type=MetricPartialFunctionTypes.AGGREGATE_FN"
                (which ultimately sets "metric_fn_type" of inner function to this value); or "@column_aggregate_value"
                decorator (with default "metric_fn_type" set to "MetricFunctionTypes.VALUE"); or (applicable for column
                domain metrics only) "column_aggregate_partial" decorator with "partial_fn_type" explicitly set to
                "MetricPartialFunctionTypes.AGGREGATE_FN".  When "metric_fn_type" of metric implementation function is
                of "aggregate partial" type ("MetricPartialFunctionTypes.AGGREGATE_FN"), underlying backend (e.g., SQL
                or Spark) employs "deferred execution" (gather computation needs to build execution plan, then execute
                all computations combined).  Deferred aggregate function calls are bundled (applies to SQL and Spark).
                To instruct "ExecutionEngine" accordingly, original metric is registered with its "declared" name, but
                with "metric_provider" function omitted (set to "None"), and additional "AGGREGATE_FN" metric, with its
                "metric_provider" set to (decorated) implementation function, defined in metric class, is registered.
                Then "AGGREGATE_FN" metric is specified with key "metric_partial_fn" as evaluation metric dependency.
                By convention, aggregate partial metric implementation functions return three-valued tuple, containing
                deferred execution metric implementation function of corresponding "ExecutionEngine" backend (called
                "metric_aggregate") as well as "compute_domain_kwargs" and "accessor_domain_kwargs", which are relevant
                for bundled computation and result access, respectively.  When "ExecutionEngine.resolve_metrics()" finds
                no "metric_provider" (metric_fn being "None"), it then obtains this three-valued tuple from dictionary
                of "resolved_metric_dependencies_by_metric_name" using previously declared "metric_partial_fn" key (as
                described above), composes full metric execution configuration structure, and adds this configuration
                to list of metrics to be resolved as one bundle (specifics pertaining to "ExecutionEngine" subclasses).
                """
                if metric_fn_type not in [
                    MetricFunctionTypes.VALUE,
                    MetricPartialFunctionTypes.AGGREGATE_FN,
                ]:
                    raise ValueError(
                        f"""Basic metric implementations (defined by specifying "metric_name" class variable) only \
support "{MetricFunctionTypes.VALUE.value}" and "{MetricPartialFunctionTypes.AGGREGATE_FN.value}" for "metric_value" \
"metric_fn_type" property."""
                    )

                if metric_fn_type == MetricFunctionTypes.VALUE:
                    register_metric(
                        metric_name=declared_metric_name,
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=metric_fn,
                        metric_fn_type=metric_fn_type,
                    )
                else:
                    register_metric(
                        metric_name=f"{declared_metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=metric_fn,
                        metric_fn_type=metric_fn_type,
                    )
                    register_metric(
                        metric_name=declared_metric_name,
                        metric_domain_keys=metric_domain_keys,
                        metric_value_keys=metric_value_keys,
                        execution_engine=engine,
                        metric_class=cls,
                        metric_provider=None,
                        metric_fn_type=metric_fn_type,
                    )
            elif hasattr(attr_obj, "_renderer_type"):
                register_renderer(
                    object_name=metric_name, parent_class=cls, renderer_fn=attr_obj
                )

    @classmethod
    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """This should return a dictionary:

        {
          "dependency_name": MetricConfiguration,
          ...
        }
        """
        return (
            cls._get_evaluation_dependencies(
                metric=metric,
                configuration=configuration,
                execution_engine=execution_engine,
                runtime_configuration=runtime_configuration,
            )
            or {}
        )

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: Dict[str, MetricConfiguration] = {}

        if execution_engine is None:
            return dependencies

        try:
            metric_name: str = metric.metric_name
            _ = get_metric_provider(
                f"{metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                execution_engine,
            )
            dependencies["metric_partial_fn"] = MetricConfiguration(
                metric_name=f"{metric_name}.{MetricPartialFunctionTypes.AGGREGATE_FN.metric_suffix}",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=metric.metric_value_kwargs,
            )
        except gx_exceptions.MetricProviderError:
            pass

        return dependencies
