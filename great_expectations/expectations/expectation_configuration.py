from __future__ import annotations

import copy
import json
import logging
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)

from marshmallow import Schema, ValidationError, fields, post_dump, post_load, pre_dump
from typing_extensions import TypedDict

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.suite_parameters import (
    build_suite_parameters,
)
from great_expectations.exceptions import (
    ExpectationNotFoundError,
    InvalidExpectationConfigurationError,
    InvalidExpectationKwargsError,
)
from great_expectations.expectations.metadata_types import FailureSeverity
from great_expectations.expectations.registry import get_expectation_impl
from great_expectations.render import RenderedAtomicContent, RenderedAtomicContentSchema
from great_expectations.types import SerializableDictDot
from great_expectations.util import (
    convert_to_json_serializable,  # noqa: TID251 # FIXME CoP
    ensure_json_serializable,  # noqa: TID251 # FIXME CoP
)

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations.expectation import Expectation
logger = logging.getLogger(__name__)


def parse_result_format(result_format: Union[str, dict]) -> dict:
    """This is a simple helper utility that can be used to parse a string result_format into the dict format used
    internally by great_expectations. It is not necessary but allows shorthand for result_format in cases where
    there is no need to specify a custom partial_unexpected_count."""  # noqa: E501 # FIXME CoP
    if isinstance(result_format, str):
        result_format = {
            "result_format": result_format,
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
            "map_expectation_unexpected_rows_as_dict": False,
        }
    else:
        if "include_unexpected_rows" in result_format and "result_format" not in result_format:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "When using `include_unexpected_rows`, `result_format` must be explicitly specified"
            )

        if "partial_unexpected_count" not in result_format:
            result_format["partial_unexpected_count"] = 20

        if "include_unexpected_rows" not in result_format:
            result_format["include_unexpected_rows"] = False

        if "map_expectation_unexpected_rows_as_dict" not in result_format:
            result_format["map_expectation_unexpected_rows_as_dict"] = False

    return result_format


class ExpectationContext(SerializableDictDot):
    def __init__(self, description: Optional[str] = None) -> None:
        self._description = description

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value) -> None:
        self._description = value


class ExpectationContextSchema(Schema):
    description = fields.String(required=False, allow_none=True)

    @post_load
    def make_expectation_context(self, data, **kwargs):
        return ExpectationContext(**data)


class KWargDetailsDict(TypedDict):
    domain_kwargs: tuple[str, ...]
    success_kwargs: tuple[str, ...]
    default_kwarg_values: dict[str, str | bool | float | None]


# NOTE: This class is a legacy class that is being replaced by the new class-first expectations.
#       While it is still used in some places (namely around serialization), it is being phased out.
#       Please utilize Expectation objects directly when possible.
class ExpectationConfiguration(SerializableDictDot):
    """Defines the parameters and name of a specific Expectation.

    Args:
        type: The name of the expectation class to use in snake case, e.g. `expect_column_values_to_not_be_null`.
        kwargs: The keyword arguments to pass to the expectation class.
        meta: A dictionary of metadata to attach to the expectation.
        notes: Notes about this expectation.
        description: The description of the expectation. This will be rendered instead of the default template.
        severity: The severity of the expectation failure.
        success_on_last_run: Whether the expectation succeeded on the last run.
        id: The corresponding GX Cloud ID for the expectation.
        expectation_context: The context for the expectation.
        rendered_content: Rendered content for the expectation.
    Raises:
        InvalidExpectationConfigurationError: If `expectation_type` arg is not a str.
        InvalidExpectationConfigurationError: If `kwargs` arg is not a dict.
        InvalidExpectationKwargsError: If domain kwargs are missing.
        ValueError: If a `domain_type` cannot be determined.
    """  # noqa: E501 # FIXME CoP

    runtime_kwargs: ClassVar[tuple[str, ...]] = (
        "result_format",
        "catch_exceptions",
    )

    def __init__(  # noqa: PLR0913 # FIXME CoP
        self,
        type: str,
        kwargs: dict,
        meta: Optional[dict] = None,
        notes: str | list[str] | None = None,
        description: str | None = None,
        severity: FailureSeverity = FailureSeverity.CRITICAL,
        success_on_last_run: Optional[bool] = None,
        id: Optional[str] = None,
        expectation_context: Optional[ExpectationContext] = None,
        rendered_content: Optional[List[RenderedAtomicContent]] = None,
    ) -> None:
        if not isinstance(type, str):
            raise InvalidExpectationConfigurationError("expectation_type must be a string")  # noqa: TRY003 # FIXME CoP
        self._type = type
        if not isinstance(kwargs, dict):
            raise InvalidExpectationConfigurationError(  # noqa: TRY003 # FIXME CoP
                "expectation configuration kwargs must be a dict."
            )
        self._kwargs = kwargs
        # the kwargs before suite parameters are evaluated
        self._raw_kwargs: dict[str, Any] | None = None
        if meta is None:
            meta = {}
        # We require meta information to be serializable, but do not convert until necessary
        ensure_json_serializable(meta)
        self.meta = meta
        self.notes = notes
        self.description = description
        self.severity = severity
        self.success_on_last_run = success_on_last_run
        self._id = id
        self._expectation_context = expectation_context
        self._rendered_content = rendered_content

    def process_suite_parameters(
        self,
        suite_parameters,
        interactive_evaluation: bool = True,
        data_context: Optional[AbstractDataContext] = None,
    ) -> None:
        if not self._raw_kwargs:
            suite_args, _ = build_suite_parameters(
                expectation_args=self._kwargs,
                suite_parameters=suite_parameters,
                interactive_evaluation=interactive_evaluation,
                data_context=data_context,
            )

            self._raw_kwargs = self._kwargs
            self._kwargs = suite_args
        else:
            logger.debug("suite_parameters have already been built on this expectation")

    def get_raw_configuration(self) -> ExpectationConfiguration:
        # return configuration without substituted suite parameters
        raw_config = deepcopy(self)
        if raw_config._raw_kwargs is not None:
            raw_config._kwargs = raw_config._raw_kwargs
            raw_config._raw_kwargs = None

        return raw_config

    @property
    def id(self) -> Optional[str]:
        return self._id

    @id.setter
    def id(self, value: str) -> None:
        self._id = value

    @property
    def expectation_context(self) -> Optional[ExpectationContext]:
        return self._expectation_context

    @expectation_context.setter
    def expectation_context(self, value: ExpectationContext) -> None:
        self._expectation_context = value

    @property
    def type(self) -> str:
        return self._type

    @property
    def kwargs(self) -> dict:
        return self._kwargs

    @kwargs.setter
    def kwargs(self, value: dict) -> None:
        self._kwargs = value

    @property
    def rendered_content(self) -> Optional[List[RenderedAtomicContent]]:
        return self._rendered_content

    @rendered_content.setter
    def rendered_content(self, value: Optional[List[RenderedAtomicContent]]) -> None:
        self._rendered_content = value

    @property
    def severity(self) -> FailureSeverity:
        return self._severity

    @severity.setter
    def severity(self, value: Union[FailureSeverity, str]) -> None:
        # Convert string severity to enum and validate
        if isinstance(value, str):
            try:
                self._severity = FailureSeverity(value)
            except ValueError:
                valid_values = ", ".join([member.value for member in FailureSeverity])
                raise InvalidExpectationConfigurationError(  # noqa: TRY003
                    f"Invalid severity '{value}'. Must be one of: {valid_values}"
                )
        else:
            # Validate that it's a valid FailureSeverity enum
            if not isinstance(value, FailureSeverity):
                raise InvalidExpectationConfigurationError(  # noqa: TRY003
                    f"Severity must be string or enum, got {type(value).__name__}"
                )
            self._severity = value

    def _get_default_custom_kwargs(self) -> KWargDetailsDict:
        # NOTE: this is a holdover until class-first expectations control their
        # defaults, and so defaults are inherited.
        if self.type.startswith("expect_column_pair"):
            return {
                "domain_kwargs": (
                    "column_A",
                    "column_B",
                    "row_condition",
                    "condition_parser",
                ),
                # NOTE: this is almost certainly incomplete; subclasses should override
                "success_kwargs": (),
                "default_kwarg_values": {
                    "column_A": None,
                    "column_B": None,
                    "row_condition": None,
                    "condition_parser": None,
                },
            }
        elif self.type.startswith("expect_column"):
            return {
                "domain_kwargs": ("column", "row_condition", "condition_parser"),
                # NOTE: this is almost certainly incomplete; subclasses should override
                "success_kwargs": (),
                "default_kwarg_values": {
                    "column": None,
                    "row_condition": None,
                    "condition_parser": None,
                },
            }

        logger.warning("Requested kwargs for an unrecognized expectation.")
        return {
            "domain_kwargs": (),
            # NOTE: this is almost certainly incomplete; subclasses should override
            "success_kwargs": (),
            "default_kwarg_values": {},
        }

    def get_domain_kwargs(self) -> dict:
        default_kwarg_values: dict[str, Any] = {}
        try:
            impl = self._get_expectation_impl()
        except ExpectationNotFoundError:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
            domain_keys = expectation_kwargs_dict["domain_kwargs"]
        else:
            domain_keys = impl.domain_keys
            default_kwarg_values = self._get_expectation_class_defaults()

        domain_kwargs = {
            key: self.kwargs.get(key, default_kwarg_values.get(key)) for key in domain_keys
        }
        missing_kwargs = set(domain_keys) - set(domain_kwargs.keys())
        if missing_kwargs:
            raise InvalidExpectationKwargsError(f"Missing domain kwargs: {list(missing_kwargs)}")  # noqa: TRY003 # FIXME CoP

        return domain_kwargs

    def get_success_kwargs(self) -> dict:
        """Gets the success and domain kwargs for this ExpectationConfiguration.

        Raises:
            ExpectationNotFoundError: If the expectation implementation is not found.

        Returns:
            A dictionary with the success and domain kwargs of an expectation.
        """
        default_kwarg_values: Mapping[str, str | bool | float | object | None]
        try:
            impl = self._get_expectation_impl()
        except ExpectationNotFoundError:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
            default_kwarg_values = expectation_kwargs_dict.get("default_kwarg_values", {})
            success_keys = expectation_kwargs_dict["success_kwargs"]
        else:
            success_keys = impl.success_keys
            default_kwarg_values = self._get_expectation_class_defaults()

        domain_kwargs = self.get_domain_kwargs()
        success_kwargs = {
            key: self.kwargs.get(key, default_kwarg_values.get(key)) for key in success_keys
        }
        success_kwargs.update(domain_kwargs)

        return success_kwargs

    def get_runtime_kwargs(self, runtime_configuration: Optional[dict] = None) -> dict:
        runtime_keys: tuple[str, ...]
        default_kwarg_values: Mapping[str, str | bool | float | object | None]
        try:
            impl = self._get_expectation_impl()
        except ExpectationNotFoundError:
            expectation_kwargs_dict = self._get_default_custom_kwargs()
            default_kwarg_values = expectation_kwargs_dict.get("default_kwarg_values", {})
            runtime_keys = self.runtime_kwargs
        else:
            runtime_keys = impl.runtime_keys
            default_kwarg_values = self._get_expectation_class_defaults()

        success_kwargs = self.get_success_kwargs()
        lookup_kwargs = deepcopy(self.kwargs)
        if runtime_configuration:
            lookup_kwargs.update(runtime_configuration)

        runtime_kwargs = {
            key: lookup_kwargs.get(key, default_kwarg_values.get(key)) for key in runtime_keys
        }
        result_format = runtime_kwargs["result_format"]
        if result_format is not None:
            runtime_kwargs["result_format"] = parse_result_format(result_format)
        runtime_kwargs.update(success_kwargs)

        return runtime_kwargs

    def applies_to_same_domain(
        self, other_expectation_configuration: ExpectationConfiguration
    ) -> bool:
        if self.type != other_expectation_configuration.type:
            return False
        return self.get_domain_kwargs() == other_expectation_configuration.get_domain_kwargs()

    # noinspection PyPep8Naming
    def isEquivalentTo(
        self,
        other: Union[dict, ExpectationConfiguration],
        match_type: str = "success",
    ) -> bool:
        """ExpectationConfiguration equivalence does not include meta, and relies on *equivalence* of kwargs."""  # noqa: E501 # FIXME CoP
        if not isinstance(other, self.__class__):
            if isinstance(other, dict):
                try:
                    # noinspection PyNoneFunctionAssignment
                    other = expectationConfigurationSchema.load(other)
                except ValidationError:
                    logger.debug(
                        "Unable to evaluate equivalence of ExpectationConfiguration object with dict because "  # noqa: E501 # FIXME CoP
                        "dict other could not be instantiated as an ExpectationConfiguration"
                    )
                    return False
            else:
                # Not comparable; a plain bool, because callers use the result as a truth value
                # and `NotImplemented` is only meaningful to the comparison operators.
                return False

        if match_type == "domain":
            return all(
                (
                    self.type == other.type,  # type: ignore[union-attr] # could be dict
                    self.get_domain_kwargs() == other.get_domain_kwargs(),  # type: ignore[union-attr] # could be dict
                )
            )

        if match_type == "success":
            return all(
                (
                    self.type == other.type,  # type: ignore[union-attr] # could be dict
                    self.get_success_kwargs() == other.get_success_kwargs(),  # type: ignore[union-attr] # could be dict
                )
            )

        if match_type == "runtime":
            return all(
                (
                    self.type == other.type,  # type: ignore[union-attr] # could be dict
                    self.kwargs == other.kwargs,  # type: ignore[union-attr] # could be dict
                )
            )

        return False

    @override
    def __eq__(self, other):
        """ExpectationConfiguration equality does include meta, but ignores instance identity."""
        if not isinstance(other, self.__class__):
            # Delegate comparison to the other instance's __eq__.
            return NotImplemented
        this_kwargs: dict = convert_to_json_serializable(self.kwargs)
        other_kwargs: dict = convert_to_json_serializable(other.kwargs)
        this_meta: dict = convert_to_json_serializable(self.meta)
        other_meta: dict = convert_to_json_serializable(other.meta)
        return all(
            (
                self.type == other.type,
                this_kwargs == other_kwargs,
                this_meta == other_meta,
                str(self.id) == str(other.id),
            )
        )

    @override
    def __hash__(self) -> int:
        this_kwargs: dict = convert_to_json_serializable(self.kwargs)
        this_meta: dict = convert_to_json_serializable(self.meta)

        def make_hashable(obj):
            """Convert unhashable types to hashable ones recursively."""
            if isinstance(obj, (str, int, float, bool, type(None))):
                return obj
            elif isinstance(obj, list):
                return tuple(make_hashable(item) for item in obj)
            elif isinstance(obj, dict):
                return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
            else:
                return str(obj)

        return hash((self.type, make_hashable(this_kwargs), make_hashable(this_meta), str(self.id)))

    def __ne__(self, other):  # type: ignore[explicit-override] # FIXME
        # By using the == operator, the returned NotImplemented is handled correctly.
        return not self == other

    def __repr__(self):  # type: ignore[explicit-override] # FIXME
        return json.dumps(self.to_json_dict())

    @override
    def __str__(self):
        return json.dumps(self.to_json_dict(), indent=2)

    @override
    def to_json_dict(self) -> Dict[str, JSONValues]:
        """Returns a JSON-serializable dict representation of this ExpectationConfiguration.

        Returns:
            A JSON-serializable dict representation of this ExpectationConfiguration.
        """
        myself = expectationConfigurationSchema.dump(self)
        # NOTE - JPC - 20191031: migrate to expectation-specific schemas that subclass result with properly-typed  # noqa: E501 # FIXME CoP
        # schemas to get serialization all-the-way down via dump
        myself["kwargs"] = convert_to_json_serializable(myself["kwargs"])

        # Post dump hook removes this value if null so we need to ensure applicability before conversion  # noqa: E501 # FIXME CoP
        if "expectation_context" in myself:
            myself["expectation_context"] = convert_to_json_serializable(
                myself["expectation_context"]
            )
        if "rendered_content" in myself:
            myself["rendered_content"] = convert_to_json_serializable(myself["rendered_content"])
        return myself

    def _get_expectation_impl(self) -> Type[Expectation]:
        return get_expectation_impl(self.type)

    def to_domain_obj(self) -> Expectation:
        expectation_impl = self._get_expectation_impl()
        kwargs: dict[Any, Any] = {
            "id": self.id,
            "meta": self.meta,
            "notes": self.notes,
            "rendered_content": self.rendered_content,
            "severity": self.severity,
        }
        # it's possible description could be subclassed as a class variable,
        # because we have documented it that way in the past.
        # if that is the case, passing a self.description of any type would raise an error
        # we can't check for the presence of expectation_impl.description
        # because _get_expectation_impl() only returns registered expectations
        if self.description:
            kwargs.update({"description": self.description})
        kwargs.update(self.kwargs)
        return expectation_impl(**kwargs)

    def get_domain_type(self) -> MetricDomainTypes:
        """Return "domain_type" of this expectation."""
        if self.type.startswith("expect_table_"):
            return MetricDomainTypes.TABLE

        if "column" in self.kwargs:
            return MetricDomainTypes.COLUMN

        if "column_A" in self.kwargs and "column_B" in self.kwargs:
            return MetricDomainTypes.COLUMN_PAIR

        if "column_list" in self.kwargs:
            return MetricDomainTypes.MULTICOLUMN

        raise ValueError(  # noqa: TRY003 # FIXME CoP
            'Unable to determine "domain_type" of this "ExpectationConfiguration" object from "kwargs" and heuristics.'  # noqa: E501 # FIXME CoP
        )

    def _get_expectation_class_defaults(self) -> dict[str, Any]:
        cls = self._get_expectation_impl()
        return {
            name: field.default if not field.required else None
            for name, field in cls.__fields__.items()
        }


class ExpectationConfigurationSchema(Schema):
    type = fields.Str(
        required=True,
        error_messages={"required": "type missing in expectation configuration"},
    )
    kwargs = fields.Dict(
        required=False,
        allow_none=True,
    )
    meta = fields.Dict(
        required=False,
        allow_none=True,
    )
    notes = fields.Raw(
        required=False,
        allow_none=True,
    )
    id = fields.UUID(required=False, allow_none=True)
    expectation_context = fields.Nested(
        lambda: ExpectationContextSchema,
        required=False,
        allow_none=True,
    )
    rendered_content = fields.List(
        fields.Nested(
            lambda: RenderedAtomicContentSchema,
            required=False,
            allow_none=True,
        )
    )
    description = fields.Str(required=False, allow_none=True)
    severity = fields.Enum(FailureSeverity, required=False, allow_none=True, by_value=True)

    REMOVE_KEYS_IF_NONE = [
        "id",
        "expectation_context",
        "rendered_content",
        "notes",
        "description",
    ]

    @pre_dump
    def convert_result_to_serializable(self, data, **kwargs):
        data = copy.deepcopy(data)
        data["kwargs"] = convert_to_json_serializable(data.get("kwargs", {}))
        # If severity is already a string (from convert_to_json_serializable), convert
        # it back to enum so marshmallow's Enum field can handle it properly
        if "severity" in data and isinstance(data["severity"], str):
            data["severity"] = FailureSeverity(data["severity"])
        return data

    @post_dump
    def clean_null_attrs(self, data: dict, **kwargs: dict) -> dict:
        """Removes the attributes in ExpectationConfigurationSchema.REMOVE_KEYS_IF_NONE during serialization if
        their values are None."""  # noqa: E501 # FIXME CoP
        data = copy.deepcopy(data)
        for key in ExpectationConfigurationSchema.REMOVE_KEYS_IF_NONE:
            if key in data and data[key] is None:
                data.pop(key)
        return data

    def _convert_uuids_to_str(self, data):
        """
        Utilize UUID for data validation but convert to string before usage in business logic
        """
        attr = "id"
        uuid_val = data.get(attr)
        if uuid_val:
            data[attr] = str(uuid_val)
        return data

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_configuration(self, data: dict, **kwargs):
        data = self._convert_uuids_to_str(data=data)
        return ExpectationConfiguration(**data)


expectationConfigurationSchema = ExpectationConfigurationSchema()
expectationContextSchema = ExpectationContextSchema()
