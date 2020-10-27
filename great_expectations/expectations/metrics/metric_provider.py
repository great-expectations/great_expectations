import logging
from functools import wraps
from typing import Any, Callable, Dict, Optional, Tuple, Type

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.registry import register_metric
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)


class MetaMetricProvider(type):
    """MetaMetricProvider registers metrics as they are defined."""

    def __new__(cls, clsname, bases, attrs):
        newclass = super().__new__(cls, clsname, bases, attrs)
        newclass._register_metric_functions()
        return newclass


def metric(engine: Type[ExecutionEngine], metric_fn_type: str = "data", **kwargs):
    """The metric decorator annotates a method """

    def wrapper(metric_fn: Callable):
        @wraps(metric_fn)
        def inner_func(*args, **kwargs):
            return metric_fn(*args, **kwargs)

        inner_func.metric_fn_engine = engine
        inner_func.metric_fn_type = metric_fn_type
        inner_func.metric_definition_kwargs = kwargs
        return inner_func

    return wrapper


def renders(mode: str, **kwargs):
    def wrapper(render_fn: Callable):
        @wraps(render_fn)
        def inner_func(*args, **kwargs):
            return render_fn(*args, **kwargs)

        inner_func._renders_mode = mode
        inner_func._renders_definition_kwargs = kwargs
        return inner_func

    return wrapper


class MetricProvider(metaclass=MetaMetricProvider):
    domain_keys = tuple()
    value_keys = tuple()
    metric_fn_type = "data"

    @classmethod
    def _register_metric_functions(cls):
        if not hasattr(cls, "metric_name"):
            # no metric name was defined; do not register methods
            return

        metric_name = getattr(cls, "metric_name")
        metric_domain_keys = cls.domain_keys
        metric_value_keys = cls.value_keys

        for attr, candidate_metric_fn in cls.__dict__.items():
            if not hasattr(candidate_metric_fn, "metric_fn_engine"):
                # This is not a metric
                continue
            engine = getattr(candidate_metric_fn, "metric_fn_engine")
            if not issubclass(engine, ExecutionEngine):
                raise ValueError(
                    "metric functions must be defined with an Execution Engine"
                )
            metric_fn = getattr(cls, attr)
            metric_definition_kwargs = getattr(
                metric_fn, "metric_definition_kwargs", dict()
            )
            declared_metric_name = metric_name + metric_definition_kwargs.get(
                "metric_name_suffix", ""
            )
            metric_fn_type = metric_fn.get(
                "metric_fn_type", getattr(cls, "metric_fn_type", "data")
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
