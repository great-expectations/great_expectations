"""Every pydantic model in the package must register each annotated name as a field.

On Python 3.14, releases of pydantic before 2.13 silently drop annotation-only fields
(names annotated without a default) from models built on the ``pydantic.v1`` namespace: the
class is created without error and the missing fields surface later as ``AttributeError`` or
as data that never validates. Comparing each model's own annotations with the fields pydantic
registered catches that class of defect directly, on every interpreter.
"""

import sys
from typing import Any, ClassVar, ForwardRef, get_origin

import pytest

from great_expectations.compatibility import pydantic

if sys.version_info >= (3, 14):
    import annotationlib

# Models whose annotations deliberately include names that are not fields. Each entry maps a
# qualified model name to the names to ignore and the reason. Widening the checker's filter
# is not an acceptable alternative: a wide filter is exactly what would hide a real drop.
_DELIBERATE_NON_FIELD_ANNOTATIONS: dict[str, dict[str, str]] = {}

# Models whose fields exist only as annotations (no default, no field() call) in modules that
# do not defer annotation evaluation. This is the defect class the test guards, so the test
# must prove these models were actually checked rather than passing over an empty collection.
_ANNOTATION_ONLY_MODELS = (
    "great_expectations.core.serdes._IdentifierBundle",
    "great_expectations.expectations.window.Window",
    "great_expectations.metrics.batch.batch_column_types.ColumnType",
    "great_expectations.metrics.column.descriptive_stats.DescriptiveStats",
    "great_expectations.metrics.metric_results.MetricResult",
)

pytestmark = pytest.mark.unit


def _all_subclasses(base: type) -> set[type]:
    found: set[type] = set()
    pending = [base]
    while pending:
        for subclass in pending.pop().__subclasses__():
            if subclass not in found:
                found.add(subclass)
                pending.append(subclass)
    return found


def gx_pydantic_models() -> list[type[pydantic.BaseModel]]:
    """Every BaseModel subclass reachable from the compatibility shim that the package defines.

    Collected after importing the package, in a deterministic order.
    """
    import great_expectations  # noqa: F401  # importing the package defines its models

    models = [
        model
        for model in _all_subclasses(pydantic.BaseModel)
        if model.__module__.split(".")[0] == "great_expectations"
    ]
    return sorted(models, key=lambda model: (model.__module__, model.__qualname__))


def _own_annotations(model: type) -> dict[str, Any]:
    if sys.version_info >= (3, 14):
        # Annotations are evaluated lazily on 3.14, and the class dict no longer holds them.
        # FORWARDREF keeps names whose annotations cannot be resolved yet instead of raising.
        return dict(annotationlib.get_annotations(model, format=annotationlib.Format.FORWARDREF))
    return dict(model.__dict__.get("__annotations__", {}))


def _is_class_var(annotation: Any) -> bool:
    if isinstance(annotation, ForwardRef):
        annotation = annotation.__forward_arg__
    if isinstance(annotation, str):
        return annotation.replace(" ", "").split("[")[0].split(".")[-1] == "ClassVar"
    return annotation is ClassVar or get_origin(annotation) is ClassVar


def unregistered_annotations(model: type[pydantic.BaseModel]) -> set[str]:
    """Names in the model's own (not inherited) annotations that pydantic did not register.

    ClassVar-annotated names, names starting with an underscore and ``Config`` are not fields
    and are excluded.
    """
    annotated = {
        name
        for name, annotation in _own_annotations(model).items()
        if not name.startswith("_") and name != "Config" and not _is_class_var(annotation)
    }
    return annotated - set(model.__fields__)


def test_every_gx_model_registers_its_annotated_fields() -> None:
    models = [model for model in gx_pydantic_models() if _own_annotations(model)]
    assert models, "no great_expectations pydantic model with its own annotations was collected"

    checked = {f"{model.__module__}.{model.__qualname__}" for model in models}
    assert set(_ANNOTATION_ONLY_MODELS) <= checked, (
        f"annotation-only models were not checked: {sorted(set(_ANNOTATION_ONLY_MODELS) - checked)}"
    )

    unregistered: dict[str, list[str]] = {}
    for model in models:
        qualified_name = f"{model.__module__}.{model.__qualname__}"
        allowed = set(_DELIBERATE_NON_FIELD_ANNOTATIONS.get(qualified_name, {}))
        missing = unregistered_annotations(model) - allowed
        if missing:
            unregistered[qualified_name] = sorted(missing)

    assert not unregistered, (
        f"pydantic did not register these annotated names as fields: {unregistered}"
    )


def test_checker_reports_a_stripped_field() -> None:
    class Planted(pydantic.BaseModel):
        kept: int
        stripped: str
        shared: ClassVar[int] = 0
        _private: int = 0

    assert unregistered_annotations(Planted) == set()

    del Planted.__fields__["stripped"]

    assert unregistered_annotations(Planted) == {"stripped"}
