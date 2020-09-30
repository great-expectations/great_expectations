import logging
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type

from great_expectations.core.id_dict import IDDict
from great_expectations.exceptions.metric_exceptions import MetricProviderError
from great_expectations.validator.validation_graph import MetricEdgeKey

logger = logging.getLogger(__name__)

_registered_expectations = dict()
_registered_metrics = dict()
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


def register_expectation(expectation: Type["Expectation"]) -> None:
    expectation_type = expectation.expectation_type
    # TODO: add version to key
    if expectation_type in _registered_expectations:
        if _registered_expectations[expectation_type] == expectation:
            logger.info(
                "Multiple declarations of expectation " + expectation_type + " found."
            )
            return
        else:
            logger.warning(
                "Overwriting declaration of expectation " + expectation_type + "."
            )
    logger.debug("Registering expectation: " + expectation_type)
    _registered_expectations[expectation_type] = expectation


def _add_response_key(res, key, value):
    if key in res:
        res[key].append(value)
    else:
        res[key] = [value]
    return res


def register_metric(
    metric_name: str,
    metric_domain_keys: Tuple[str],
    metric_value_keys: Tuple[str],
    execution_engine: Type["ExecutionEngine"],
    metric_dependencies: Tuple[str],
    metric_provider: Callable,
    bundle_computation: bool = False,
    filter_column_isnull: bool = True,
) -> dict:
    res = dict()
    execution_engine_name = execution_engine.__name__
    logger.debug(f"Registering metric: {metric_name}")
    metric_provider._can_be_bundled = bundle_computation
    if metric_name in _registered_metrics:
        metric_definition = _registered_metrics[metric_name]
        current_dependencies = metric_definition.get("metric_dependencies", set())
        if set(current_dependencies) != set(metric_dependencies):
            logger.warning(
                f"metric {metric_name} is being registered with different dependencies; overwriting dependencies"
            )
            _add_response_key(
                res,
                "warning",
                f"metric {metric_name} is being registered with different dependencies; overwriting dependencies",
            )
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
        providers = metric_definition.get("providers", dict())
        if execution_engine_name in providers:
            current_provider_fn = providers[execution_engine_name]
            if current_provider_fn != metric_provider:
                logger.warning(
                    f"metric {metric_name} is being registered with different metric_provider; overwriting metric_provider"
                )
                _add_response_key(
                    res,
                    "warning",
                    f"metric {metric_name} is being registered with different metric_provider; overwriting metric_provider",
                )
                providers[execution_engine_name] = metric_provider
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
            providers[execution_engine_name] = metric_provider
    else:
        metric_definition = {
            "metric_domain_keys": metric_domain_keys,
            "metric_value_keys": metric_value_keys,
            "metric_dependencies": metric_dependencies,
            "providers": {execution_engine_name: metric_provider},
            "filter_column_isnull": filter_column_isnull,
        }
        _registered_metrics[metric_name] = metric_definition
    res["success"] = True
    return res


def get_metric_provider(
    metric_name: str, execution_engine: "ExecutionEngine"
) -> Callable:
    try:
        metric_definition = _registered_metrics.get(metric_name, dict())
        return metric_definition["providers"][type(execution_engine).__name__]
    except KeyError:
        raise MetricProviderError(
            f"No provider found for {metric_name} using {type(execution_engine).__name__}"
        )


def get_metric_dependencies(metric_name: str) -> Tuple[str]:
    try:
        metric_definition = _registered_metrics.get(metric_name, dict())
        return metric_definition["metric_dependencies"]
    except KeyError:
        raise MetricProviderError(f"No definition found for {metric_name}")


def get_metric_kwargs(
    metric_name: str,
    configuration: Optional["ExpectationConfiguration"] = None,
    runtime_configuration: Optional[dict] = None,
) -> dict():
    try:
        metric_definition = _registered_metrics.get(metric_name, dict())
        metric_kwargs = {
            "metric_domain_keys": metric_definition["metric_domain_keys"],
            "metric_value_keys": metric_definition["metric_value_keys"],
            "filter_column_isnull": metric_definition["filter_column_isnull"],
        }
        if configuration:
            expectation_impl = get_expectation_impl(configuration.expectation_type)
            configuration_kwargs = expectation_impl(
                configuration=configuration
            ).get_runtime_kwargs(runtime_configuration=runtime_configuration)
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
            metric_kwargs["metric_domain_kwargs"] = metric_domain_kwargs
            metric_kwargs["metric_value_kwargs"] = metric_value_kwargs
        return metric_kwargs
    except KeyError:
        raise MetricProviderError(f"Incomplete definition found for {metric_name}")


def extract_metrics(
    metric_names: Iterable[str],
    metrics: Dict[Tuple, Any],
    configuration: "ExpectationConfiguration",
    runtime_configuration: Optional[dict] = None,
) -> dict:
    res = dict()
    for metric_name in metric_names:
        kwargs = get_metric_kwargs(metric_name, configuration, runtime_configuration)
        res[metric_name] = metrics[
            MetricEdgeKey(
                metric_name,
                kwargs["metric_domain_kwargs"],
                kwargs["metric_value_kwargs"],
                kwargs["filter_column_isnull"],
            ).id
        ]
    return res


def get_domain_metrics_dict_by_name(
    metrics: Dict[Tuple, Any], metric_domain_kwargs: IDDict
):
    return {
        metric_edge_key_id_tuple[0]: metric_value
        for metric_edge_key_id_tuple, metric_value in metrics.items()
        if metric_edge_key_id_tuple[1] == metric_domain_kwargs.to_id()
    }


def get_expectation_impl(expectation_name):
    return _registered_expectations.get(expectation_name)


def list_registered_expectation_implementations(
    expectation_root: Type["Expectation"] = None,
) -> List[str]:
    registered_expectation_implementations = []
    for (
        expectation_name,
        expectation_implementation,
    ) in _registered_expectations.items():
        if expectation_root is None:
            registered_expectation_implementations.append(expectation_name)
        elif expectation_root and issubclass(expectation_implementation, expectation_root):
            registered_expectation_implementations.append(expectation_name)

    return registered_expectation_implementations
