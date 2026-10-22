from typing import Annotated, Any, ClassVar, Final, Generic, TypeVar

from typing_extensions import dataclass_transform, get_args

from great_expectations.compatibility.pydantic import BaseModel, Field, ModelMetaclass, StrictStr
from great_expectations.metrics.metric_results import MetricResult
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,
    MetricConfigurationID,
)

ALLOWABLE_METRIC_MIXINS: Final[int] = 1

NonEmptyString = Annotated[StrictStr, Field(min_length=1)]


class MixinTypeError(TypeError):
    def __init__(self, class_name: str, mixin_superclass_name: str) -> None:
        super().__init__(
            f"`{class_name}` must use a single `{mixin_superclass_name}` subclass mixin."
        )


class MissingAttributeError(AttributeError):
    def __init__(self, class_name: str, attribute_name: str) -> None:
        super().__init__(f"`{class_name}` must define `{attribute_name}` attribute.")


class UnregisteredMetricError(ValueError):
    def __init__(self, metric_name: str) -> None:
        super().__init__(f"Metric `{metric_name}` was not found in the registry.")


class AbstractClassInstantiationError(TypeError):
    def __init__(self, class_name: str) -> None:
        super().__init__(f"Cannot instantiate abstract class `{class_name}`.")


class EmptyStrError(ValueError):
    def __init__(self, param_name) -> None:
        super().__init__(f"{param_name} must be a non-empty string.")


@dataclass_transform()
class MetaMetric(ModelMetaclass):
    """Metaclass for Metric classes that maintains a registry of all concrete Metric types."""

    _registry: dict[str, type["Metric"]] = {}

    def __new__(cls, name, bases, attrs, **kwargs):
        register_cls = super().__new__(cls, name, bases, attrs)
        # Don't register the base Metric class
        if name != "Metric":
            metric_name = attrs.get("name")
            # Some subclasses of metric may not have a name
            # Those classes will not be registered
            if metric_name:
                MetaMetric._registry[metric_name] = register_cls
        return register_cls

    @classmethod
    def get_registered_metric_class_from_metric_name(cls, metric_name: str) -> type["Metric"]:
        """Returns the registered Metric class for a given metric name."""
        try:
            return cls._registry[metric_name]
        except KeyError:
            raise UnregisteredMetricError(metric_name)


_MetricResult = TypeVar("_MetricResult", bound=MetricResult)


class Metric(BaseModel, Generic[_MetricResult], metaclass=MetaMetric):
    """The abstract base class for defining all metrics.

    A Metric represents a measurable property that can be computed over a specific domain
    of data (e.g., a column, table, or column pair). All concrete metric implementations
    must inherit from this class and specify their domain type as a mixin.

    Examples:
        A metric for a single column max value:

        >>> class ColumnMaxResult(MetricResult[int]): ...
        >>>
        >>> class ColumnMax(Metric[ColumnMaxResult]):
        ...     ...

        A metric for a single batch row count value:

        >>> class BatchRowCountResult(MetricResult[int]): ...
        >>>
        >>> class BatchRowCount(Metric[BatchRowCountResult]):
        ...     ...

    Notes:
        - The Metric class cannot be instantiated directly - it must be subclassed.
        - Once Metrics are instantiated, they are immutable.

    See Also:
        MetricConfiguration: Configuration class for metric computation
    """

    # we wouldn't mind removing this `name` attribute
    # it's currently our only hook into the legacy metrics system
    name: ClassVar[StrictStr]

    # we use this list to build a dictionary of domain-specific fields
    # by introspecting inheriting models for these keys
    _domain_fields: ClassVar[tuple[str, ...]] = (
        "column",
        "column_list",
        "column_A",
        "column_B",
        "condition_parser",
        "ignore_row_if",
        "row_condition",
    )

    class Config:
        arbitrary_types_allowed = True
        frozen = True

    def __new__(cls, *args, **kwargs):
        if cls is Metric:
            raise AbstractClassInstantiationError(cls.__name__)
        return super().__new__(cls)

    def metric_id_for_batch(self, batch_id: str) -> MetricConfigurationID:
        return self.config(batch_id=batch_id).id

    def config(self, batch_id: str) -> MetricConfiguration:
        # This class is frozen so Metric._to_config will always return the same value
        # when the same batch_id is passed in.
        if not batch_id:
            raise EmptyStrError("batch_id")

        config = Metric._to_config(
            instance_class=self.__class__,
            batch_id=batch_id,
            metric_value_set=list(self.dict().items()),
        )
        return config

    @classmethod
    def get_metric_result_type(cls) -> type[_MetricResult]:
        """Returns the MetricResult type for this Metric."""
        return get_args(getattr(cls, "__orig_bases__", [])[0])[0]

    @staticmethod
    def _to_config(
        instance_class: type["Metric"], batch_id: str, metric_value_set: list[tuple[str, Any]]
    ) -> MetricConfiguration:
        """Returns a MetricConfiguration instance for this Metric."""
        metric_domain_kwargs = {}
        metric_value_kwargs = {}
        metric_values = dict(metric_value_set)
        metric_fields = Metric.__fields__
        domain_fields = {
            field_name: field_info
            for field_name, field_info in instance_class.__fields__.items()
            if field_name in Metric._domain_fields and field_name not in metric_fields
        }
        for field_name, field_info in domain_fields.items():
            metric_domain_kwargs[field_name] = metric_values.get(field_name, field_info.default)
        # Set the batch_id with the passed in value
        metric_domain_kwargs["batch_id"] = batch_id

        value_fields = {
            field_name: field_info
            for field_name, field_info in instance_class.__fields__.items()
            if field_name not in Metric._domain_fields and field_name not in metric_fields
        }
        for field_name, field_info in value_fields.items():
            metric_value_kwargs[field_name] = metric_values.get(field_name, field_info.default)

        return MetricConfiguration(
            metric_name=instance_class.name,
            metric_domain_kwargs=metric_domain_kwargs,
            metric_value_kwargs=metric_value_kwargs,
        )
