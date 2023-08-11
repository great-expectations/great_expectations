from __future__ import annotations

import logging
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    Union,
)

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.id_dict import IDDict

if TYPE_CHECKING:
    from great_expectations.core import ExpectationConfiguration
    from great_expectations.core.metric_function_types import (
        MetricFunctionTypes,
        MetricPartialFunctionTypes,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation import Expectation
    from great_expectations.expectations.metrics.metric_provider import MetricProvider
    from great_expectations.render import (
        AtomicDiagnosticRendererType,
        AtomicPrescriptiveRendererType,
        AtomicRendererType,
        RenderedAtomicContent,
        RenderedContent,
    )
    from great_expectations.validator.computed_metric import MetricValue

logger = logging.getLogger(__name__)

_registered_expectations: dict = {}
_registered_metrics: dict = {}
_registered_renderers: dict = {}

"""
{
  "metric_name"
    metric_domain_keys
    metric_value_keys
    metric_dependencies
    providers:
      engine: provider
}
"""


class RendererImpl(NamedTuple):
    expectation: str
    renderer: Callable[..., Union[RenderedAtomicContent, RenderedContent]]


def register_renderer(
    object_name: str,
    parent_class: Union[Type[Expectation], Type[MetricProvider]],
    renderer_fn: Callable[..., Union[RenderedAtomicContent, RenderedContent]],
):
    # noinspection PyUnresolvedReferences
    renderer_name = renderer_fn._renderer_type  # type: ignore[attr-defined]
    if object_name not in _registered_renderers:
        logger.debug(f"Registering {renderer_name} for expectation_type {object_name}.")
        _registered_renderers[object_name] = {
            renderer_name: (parent_class, renderer_fn)
        }
        return

    if renderer_name in _registered_renderers[object_name]:
        if _registered_renderers[object_name][renderer_name] == (
            parent_class,
            renderer_fn,
        ):
            logger.info(
                f"Multiple declarations of {renderer_name} renderer for expectation_type {object_name} "
                f"found."
            )
            return
        else:
            logger.warning(
                f"Overwriting declaration of {renderer_name} renderer for expectation_type "
                f"{object_name}."
            )
            _registered_renderers[object_name][renderer_name] = (
                parent_class,
                renderer_fn,
            )
        return
    else:
        logger.debug(f"Registering {renderer_name} for expectation_type {object_name}.")
        _registered_renderers[object_name][renderer_name] = (parent_class, renderer_fn)
        return


def get_renderer_names(expectation_or_metric_type: str) -> List[str]:
    """Gets renderer names for a given Expectation or Metric.

    Args:
        expectation_or_metric_type: The type of an Expectation or Metric for which to get renderer names.

    Returns:
        A list of renderer names for the Expectation or Metric.
    """
    return list(_registered_renderers.get(expectation_or_metric_type, {}).keys())


def get_renderer_names_with_renderer_types(
    expectation_or_metric_type: str,
    renderer_types: List[AtomicRendererType],
) -> List[Union[str, AtomicDiagnosticRendererType, AtomicPrescriptiveRendererType]]:
    """Gets renderer names of a given type, for a given Expectation or Metric.

    Args:
        expectation_or_metric_type: The type of an Expectation or Metric for which to get renderer names.
        renderer_types: The type of the renderers for which to return names.

    Returns:
        A list of renderer names for the given prefixes and Expectation or Metric.
    """
    return [
        renderer_name
        for renderer_name in get_renderer_names(
            expectation_or_metric_type=expectation_or_metric_type
        )
        if any(
            renderer_name.startswith(renderer_type) for renderer_type in renderer_types
        )
    ]


def get_renderer_impls(object_name: str) -> List[str]:
    return list(_registered_renderers.get(object_name, {}).values())


def get_renderer_impl(object_name: str, renderer_type: str) -> Optional[RendererImpl]:
    renderer_tuple: Optional[tuple] = _registered_renderers.get(object_name, {}).get(
        renderer_type
    )
    renderer_impl: Optional[RendererImpl] = None
    if renderer_tuple:
        renderer_impl = RendererImpl(
            expectation=renderer_tuple[0], renderer=renderer_tuple[1]
        )
    return renderer_impl


def register_expectation(expectation: Type[Expectation]) -> None:
    expectation_type = expectation.expectation_type
    # TODO: add version to key
    if expectation_type in _registered_expectations:
        if _registered_expectations[expectation_type] == expectation:
            logger.info(
                f"Multiple declarations of expectation {expectation_type} found."
            )
            return
        else:
            logger.warning(
                f"Overwriting declaration of expectation {expectation_type}."
            )

    logger.debug(f"Registering expectation: {expectation_type}")
    _registered_expectations[expectation_type] = expectation


def register_core_expectations() -> None:
    """As Expectation registration is the responsibility of MetaExpectation.__new__,
    simply importing a given class will ensure that it is added to the Expectation
    registry.

    We use this JIT in the Validator to ensure that core Expectations are available
    for usage when called upon.

    Without this function, we need to hope that core Expectations are imported somewhere
    in our import graph - if not, our registry will be empty and Validator workflows
    will fail.
    """
    before_count = len(_registered_expectations)

    # Implicitly calls MetaExpectation.__new__ as Expectations are loaded from core.__init__.py
    # As __new__ calls upon register_expectation, this import builds our core registry
    from great_expectations.expectations import core  # noqa: F401

    after_count = len(_registered_expectations)

    if before_count == after_count:
        logger.debug("Already registered core expectations; no updates to registry")
    else:
        logger.debug(f"Registered {after_count-before_count} core expectations")


def _add_response_key(res, key, value):
    if key in res:
        res[key].append(value)
    else:
        res[key] = [value]
    return res


@public_api
def register_metric(  # noqa: PLR0913
    metric_name: str,
    metric_domain_keys: Tuple[str, ...],
    metric_value_keys: Tuple[str, ...],
    execution_engine: Type[ExecutionEngine],
    metric_class: Type[MetricProvider],
    metric_provider: Optional[Callable],
    metric_fn_type: Optional[
        Union[MetricFunctionTypes, MetricPartialFunctionTypes]
    ] = None,
) -> dict:
    """Register a Metric class for use as a callable metric within Expectations.

    Args:
        metric_name: A name identifying the metric. Metric Name must be globally unique in
            a great_expectations installation.
        metric_domain_keys: A tuple of the keys used to determine the domain of the metric.
        metric_value_keys: A tuple of the keys used to determine the value of the metric.
        execution_engine: The execution_engine used to execute the metric.
        metric_class: A valid Metric class containing logic to compute attributes of data.
        metric_provider: The MetricProvider class from which the metric_class inherits.
        metric_fn_type: The MetricFunctionType or MetricPartialFunctionType used to define the Metric class.

    Returns:
        A dictionary containing warnings thrown during registration if applicable, and the success status of registration.
    """
    res: dict = {}
    execution_engine_name = execution_engine.__name__
    logger.debug(f"Registering metric: {metric_name}")
    if metric_provider is not None and metric_fn_type is not None:
        metric_provider.metric_fn_type = metric_fn_type  # type: ignore[attr-defined]
    if metric_name in _registered_metrics:
        metric_definition = _registered_metrics[metric_name]
        current_domain_keys = metric_definition.get("metric_domain_keys", set())
        if set(current_domain_keys) != set(metric_domain_keys):
            logger.warning(
                f"metric {metric_name} is being registered with different metric_domain_keys; overwriting metric_domain_keys"
            )
            _add_response_key(
                res,
                "warning",
                f"metric {metric_name} is being registered with different metric_domain_keys; overwriting metric_domain_keys",
            )

        current_value_keys = metric_definition.get("metric_value_keys", set())
        if set(current_value_keys) != set(metric_value_keys):
            logger.warning(
                f"metric {metric_name} is being registered with different metric_value_keys; overwriting metric_value_keys"
            )
            _add_response_key(
                res,
                "warning",
                f"metric {metric_name} is being registered with different metric_value_keys; overwriting metric_value_keys",
            )

        providers = metric_definition.get("providers", {})
        if execution_engine_name in providers:
            current_provider_cls, current_provider_fn = providers[execution_engine_name]
            if current_provider_fn != metric_provider:
                logger.warning(
                    f"metric {metric_name} is being registered with different metric_provider; overwriting metric_provider"
                )
                _add_response_key(
                    res,
                    "warning",
                    f"metric {metric_name} is being registered with different metric_provider; overwriting metric_provider",
                )
                providers[execution_engine_name] = metric_class, metric_provider
            else:
                logger.info(
                    f"Multiple declarations of metric {metric_name} for engine {execution_engine_name}."
                )
                _add_response_key(
                    res,
                    "info",
                    f"Multiple declarations of metric {metric_name} for engine {execution_engine_name}.",
                )
        else:
            providers[execution_engine_name] = metric_class, metric_provider
    else:
        metric_definition = {
            "metric_domain_keys": metric_domain_keys,
            "metric_value_keys": metric_value_keys,
            "default_kwarg_values": metric_class.default_kwarg_values,
            "providers": {execution_engine_name: (metric_class, metric_provider)},
        }
        _registered_metrics[metric_name] = metric_definition

    res["success"] = True

    return res


def get_metric_provider(
    metric_name: str, execution_engine: ExecutionEngine
) -> Tuple[MetricProvider, Callable]:
    try:
        metric_definition = _registered_metrics[metric_name]
        return metric_definition["providers"][type(execution_engine).__name__]
    except KeyError:
        raise gx_exceptions.MetricProviderError(
            f"No provider found for {metric_name} using {type(execution_engine).__name__}"
        )


def get_metric_function_type(
    metric_name: str, execution_engine: ExecutionEngine
) -> Optional[Union[MetricPartialFunctionTypes, MetricFunctionTypes]]:
    try:
        metric_definition = _registered_metrics[metric_name]
        provider_fn, provider_class = metric_definition["providers"][
            type(execution_engine).__name__
        ]
        return getattr(provider_fn, "metric_fn_type", None)
    except KeyError:
        raise gx_exceptions.MetricProviderError(
            f"No provider found for {metric_name} using {type(execution_engine).__name__}"
        )


def get_metric_kwargs(
    metric_name: str,
    configuration: Optional[ExpectationConfiguration] = None,
    runtime_configuration: Optional[dict] = None,
) -> dict:
    try:
        metric_definition = _registered_metrics.get(metric_name)
        if metric_definition is None:
            raise gx_exceptions.MetricProviderError(
                f"No definition found for {metric_name}"
            )
        default_kwarg_values = metric_definition["default_kwarg_values"]
        metric_kwargs = {
            "metric_domain_keys": metric_definition["metric_domain_keys"],
            "metric_value_keys": metric_definition["metric_value_keys"],
        }
        if configuration:
            expectation_impl = get_expectation_impl(configuration.expectation_type)
            configuration_kwargs = expectation_impl().get_runtime_kwargs(
                configuration=configuration, runtime_configuration=runtime_configuration
            )
            if len(metric_kwargs["metric_domain_keys"]) > 0:
                metric_domain_kwargs = IDDict(
                    {
                        k: configuration_kwargs.get(k) or default_kwarg_values.get(k)
                        for k in metric_kwargs["metric_domain_keys"]
                    }
                )
            else:
                metric_domain_kwargs = IDDict()
            if len(metric_kwargs["metric_value_keys"]) > 0:
                metric_value_kwargs = IDDict(
                    {
                        k: configuration_kwargs.get(k)
                        if configuration_kwargs.get(k) is not None
                        else default_kwarg_values.get(k)
                        for k in metric_kwargs["metric_value_keys"]
                    }
                )
            else:
                metric_value_kwargs = IDDict()
            metric_kwargs["metric_domain_kwargs"] = metric_domain_kwargs
            metric_kwargs["metric_value_kwargs"] = metric_value_kwargs
        return metric_kwargs
    except KeyError:
        raise gx_exceptions.MetricProviderError(
            f"Incomplete definition found for {metric_name}"
        )


def get_domain_metrics_dict_by_name(
    metrics: Dict[Tuple[str, str, str], MetricValue], metric_domain_kwargs: IDDict
):
    return {
        metric_edge_key_id_tuple[0]: metric_value
        for metric_edge_key_id_tuple, metric_value in metrics.items()
        if metric_edge_key_id_tuple[1] == metric_domain_kwargs.to_id()
    }


def get_expectation_impl(expectation_name: str) -> Type[Expectation]:
    expectation: Type[Expectation] | None = _registered_expectations.get(
        expectation_name
    )
    if not expectation:
        raise gx_exceptions.ExpectationNotFoundError(f"{expectation_name} not found")

    return expectation


def list_registered_expectation_implementations(
    expectation_root: Optional[Type[Expectation]] = None,
) -> List[str]:
    registered_expectation_implementations = []
    for (
        expectation_name,
        expectation_implementation,
    ) in _registered_expectations.items():
        if expectation_root is None:
            registered_expectation_implementations.append(expectation_name)
        elif expectation_root and issubclass(
            expectation_implementation, expectation_root
        ):
            registered_expectation_implementations.append(expectation_name)

    return registered_expectation_implementations
