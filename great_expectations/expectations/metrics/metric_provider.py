import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.registry import register_metric, register_renderer
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)


class MetaMetricProvider(type):
    """MetaMetricProvider registers metrics as they are defined."""

    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)
        newclass._register_metric_functions()
        return newclass


def metric(
    engine: Type[ExecutionEngine],
    metric_name: str = None,
    metric_fn_type: str = "data",
    **kwargs
):
    """The metric decorator annotates a method """

    def wrapper(metric_fn: Callable):
        @wraps(metric_fn)
        def inner_func(*args, **kwargs):
            return metric_fn(*args, **kwargs)

        if metric_name is not None:
            inner_func.metric_name = metric_name
        inner_func.metric_fn_engine = engine
        inner_func.metric_fn_type = metric_fn_type
        inner_func.metric_definition_kwargs = kwargs
        return inner_func

    return wrapper


class MetricProvider(metaclass=MetaMetricProvider):
    domain_keys = tuple()
    value_keys = tuple()
    metric_fn_type = "data"
    default_kwarg_values = dict()

    @classmethod
    def _register_metric_functions(cls):
        metric_name = getattr(cls, "metric_name", None)
        metric_domain_keys = cls.domain_keys
        metric_value_keys = cls.value_keys

        for attr_name in dir(cls):
            attr_obj = getattr(cls, attr_name)
            if not hasattr(attr_obj, "metric_fn_engine") and not hasattr(
                attr_obj, "_renderer_type"
            ):
                # This is not a metric or renderer
                continue
            elif hasattr(attr_obj, "metric_fn_engine"):
                engine = getattr(attr_obj, "metric_fn_engine")
                if not issubclass(engine, ExecutionEngine):
                    raise ValueError(
                        "metric functions must be defined with an Execution Engine"
                    )
                metric_fn = attr_obj
                metric_name = getattr(metric_fn, "metric_name", metric_name)
                if metric_name is None:
                    # No metric name has been defined
                    continue
                metric_definition_kwargs = getattr(
                    metric_fn, "metric_definition_kwargs", dict()
                )
                declared_metric_name = metric_name + metric_definition_kwargs.get(
                    "metric_name_suffix", ""
                )
                metric_fn_type = getattr(
                    metric_fn, "metric_fn_type", getattr(cls, "metric_fn_type", "data")
                )
                register_metric(
                    metric_name=declared_metric_name,
                    metric_domain_keys=metric_domain_keys,
                    metric_value_keys=metric_value_keys,
                    execution_engine=engine,
                    metric_class=cls,
                    metric_provider=metric_fn,
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
        return dict()
