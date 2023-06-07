from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional, Union

from marshmallow import Schema, fields, post_load

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.core.data_context_key import DataContextKey
from great_expectations.core.id_dict import BatchKwargs, IDDict
from great_expectations.core.run_identifier import RunIdentifier, RunIdentifierSchema

if TYPE_CHECKING:
    from great_expectations.data_context.cloud_constants import GXCloudRESTResource

logger = logging.getLogger(__name__)


class ExpectationSuiteIdentifier(DataContextKey):
    def __init__(self, expectation_suite_name: str) -> None:
        super().__init__()
        if not isinstance(expectation_suite_name, str):
            raise gx_exceptions.InvalidDataContextKeyError(
                f"expectation_suite_name must be a string, not {type(expectation_suite_name).__name__}"
            )
        self._expectation_suite_name = expectation_suite_name

    @property
    def expectation_suite_name(self):
        return self._expectation_suite_name

    def to_tuple(self):
        return tuple(self.expectation_suite_name.split("."))

    def to_fixed_length_tuple(self):
        return (self.expectation_suite_name,)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(".".join(tuple_))

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(expectation_suite_name=tuple_[0])

    def __repr__(self):
        return f"{self.__class__.__name__}::{self._expectation_suite_name}"


class ExpectationSuiteIdentifierSchema(Schema):
    expectation_suite_name = fields.Str()

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite_identifier(self, data, **kwargs):
        return ExpectationSuiteIdentifier(**data)


class BatchIdentifier(DataContextKey):
    """A BatchIdentifier tracks"""

    def __init__(
        self,
        batch_identifier: Union[BatchKwargs, dict, str],
        data_asset_name: Optional[str] = None,
    ) -> None:
        super().__init__()
        # if isinstance(batch_identifier, (BatchKwargs, dict)):
        #     self._batch_identifier = batch_identifier.batch_fingerprint

        self._batch_identifier = batch_identifier
        self._data_asset_name = data_asset_name

    @property
    def batch_identifier(self):
        return self._batch_identifier

    @property
    def data_asset_name(self):
        return self._data_asset_name

    def to_tuple(self):
        return (self.batch_identifier,)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(batch_identifier=tuple_[0])


class BatchIdentifierSchema(Schema):
    batch_identifier = fields.Str()
    data_asset_name = fields.Str()

    # noinspection PyUnusedLocal
    @post_load
    def make_batch_identifier(self, data, **kwargs):
        return BatchIdentifier(**data)


@public_api
class ValidationResultIdentifier(DataContextKey):
    """A ValidationResultIdentifier identifies a validation result by the fully-qualified expectation_suite_identifier and run_id."""

    def __init__(self, expectation_suite_identifier, run_id, batch_identifier) -> None:
        """Constructs a ValidationResultIdentifier

        Args:
            expectation_suite_identifier (ExpectationSuiteIdentifier, list, tuple, or dict):
                identifying information for the fully-qualified expectation suite used to validate
            run_id (RunIdentifier): The run_id for which validation occurred
        """
        super().__init__()
        self._expectation_suite_identifier = expectation_suite_identifier
        if isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif run_id is None:
            run_id = RunIdentifier()
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=str(run_id))

        self._run_id = run_id
        self._batch_identifier = batch_identifier

    @property
    def expectation_suite_identifier(self) -> ExpectationSuiteIdentifier:
        return self._expectation_suite_identifier

    @property
    def run_id(self):
        return self._run_id

    @property
    def batch_identifier(self):
        return self._batch_identifier

    def to_tuple(self):
        return tuple(
            list(self.expectation_suite_identifier.to_tuple())
            + list(self.run_id.to_tuple())
            + [self.batch_identifier or "__none__"]
        )

    def to_fixed_length_tuple(self):
        return tuple(
            [self.expectation_suite_identifier.expectation_suite_name]
            + list(self.run_id.to_tuple())
            + [self.batch_identifier or "__none__"]
        )

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(
            ExpectationSuiteIdentifier.from_tuple(tuple_[0:-3]),
            RunIdentifier.from_tuple((tuple_[-3], tuple_[-2])),
            tuple_[-1],
        )

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(
            ExpectationSuiteIdentifier(tuple_[0]),
            RunIdentifier.from_tuple((tuple_[1], tuple_[2])),
            tuple_[3],
        )

    @classmethod
    def from_object(cls, validation_result):
        batch_kwargs = validation_result.meta.get("batch_kwargs", {})
        if isinstance(batch_kwargs, IDDict):
            batch_identifier = batch_kwargs.to_id()
        elif isinstance(batch_kwargs, dict):
            batch_identifier = IDDict(batch_kwargs).to_id()
        else:
            raise gx_exceptions.DataContextError(
                "Unable to construct ValidationResultIdentifier from provided object."
            )
        return cls(
            expectation_suite_identifier=ExpectationSuiteIdentifier(
                validation_result.meta["expectation_suite_name"]
            ),
            run_id=validation_result.meta.get("run_id"),
            batch_identifier=batch_identifier,
        )


class MetricIdentifier(DataContextKey):
    """A MetricIdentifier serves as a key to store and retrieve Metrics."""

    def __init__(self, metric_name, metric_kwargs_id) -> None:
        self._metric_name = metric_name
        self._metric_kwargs_id = metric_kwargs_id

    @property
    def metric_name(self):
        return self._metric_name

    @property
    def metric_kwargs_id(self):
        return self._metric_kwargs_id

    def to_fixed_length_tuple(self):
        return self.to_tuple()

    def to_tuple(self):
        if self._metric_kwargs_id is None:
            tuple_metric_kwargs_id = "__"
        else:
            tuple_metric_kwargs_id = self._metric_kwargs_id
        return tuple(
            (self.metric_name, tuple_metric_kwargs_id)
        )  # We use the placeholder in to_tuple

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls.from_tuple(tuple_)

    @classmethod
    def from_tuple(cls, tuple_):
        if tuple_[-1] == "__":
            return cls(*tuple_[:-1], None)
        return cls(*tuple_)


class ValidationMetricIdentifier(MetricIdentifier):
    def __init__(  # noqa: PLR0913
        self,
        run_id,
        data_asset_name,
        expectation_suite_identifier,
        metric_name,
        metric_kwargs_id,
    ) -> None:
        super().__init__(metric_name, metric_kwargs_id)
        if not isinstance(expectation_suite_identifier, ExpectationSuiteIdentifier):
            expectation_suite_identifier = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_identifier
            )

        if isinstance(run_id, dict):
            run_id = RunIdentifier(**run_id)
        elif run_id is None:
            run_id = RunIdentifier()
        elif not isinstance(run_id, RunIdentifier):
            run_id = RunIdentifier(run_name=str(run_id))

        self._run_id = run_id
        self._data_asset_name = data_asset_name
        self._expectation_suite_identifier = expectation_suite_identifier

    @property
    def run_id(self):
        return self._run_id

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def expectation_suite_identifier(self):
        return self._expectation_suite_identifier

    def to_tuple(self):
        if self.data_asset_name is None:
            tuple_data_asset_name = "__"
        else:
            tuple_data_asset_name = self.data_asset_name
        return tuple(
            list(self.run_id.to_tuple())
            + [tuple_data_asset_name]
            + list(self.expectation_suite_identifier.to_tuple())
            + [self.metric_name, self.metric_kwargs_id or "__"]
        )

    def to_fixed_length_tuple(self):
        if self.data_asset_name is None:
            tuple_data_asset_name = "__"
        else:
            tuple_data_asset_name = self.data_asset_name
        return tuple(
            list(self.run_id.to_tuple())
            + [tuple_data_asset_name]
            + list(self.expectation_suite_identifier.to_fixed_length_tuple())
            + [self.metric_name, self.metric_kwargs_id or "__"]
        )

    def to_evaluation_parameter_urn(self):
        if self._metric_kwargs_id is None:
            return "urn:great_expectations:validations:" + ":".join(
                list(self.expectation_suite_identifier.to_fixed_length_tuple())
                + [self.metric_name]
            )
        else:
            return "urn:great_expectations:validations:" + ":".join(
                list(self.expectation_suite_identifier.to_fixed_length_tuple())
                + [self.metric_name, self._metric_kwargs_id]
            )

    @classmethod
    def from_tuple(cls, tuple_):
        if len(tuple_) < 6:  # noqa: PLR2004
            raise gx_exceptions.GreatExpectationsError(
                "ValidationMetricIdentifier tuple must have at least six components."
            )
        if tuple_[2] == "__":
            tuple_data_asset_name = None
        else:
            tuple_data_asset_name = tuple_[2]
        metric_id = MetricIdentifier.from_tuple(tuple_[-2:])
        return cls(
            run_id=RunIdentifier.from_tuple((tuple_[0], tuple_[1])),
            data_asset_name=tuple_data_asset_name,
            expectation_suite_identifier=ExpectationSuiteIdentifier.from_tuple(
                tuple_[3:-2]
            ),
            metric_name=metric_id.metric_name,
            metric_kwargs_id=metric_id.metric_kwargs_id,
        )

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        if len(tuple_) != 6:  # noqa: PLR2004
            raise gx_exceptions.GreatExpectationsError(
                "ValidationMetricIdentifier fixed length tuple must have exactly six "
                "components."
            )
        if tuple_[2] == "__":
            tuple_data_asset_name = None
        else:
            tuple_data_asset_name = tuple_[2]
        metric_id = MetricIdentifier.from_tuple(tuple_[-2:])
        return cls(
            run_id=RunIdentifier.from_fixed_length_tuple((tuple_[0], tuple_[1])),
            data_asset_name=tuple_data_asset_name,
            expectation_suite_identifier=ExpectationSuiteIdentifier.from_fixed_length_tuple(
                tuple((tuple_[3],))
            ),
            metric_name=metric_id.metric_name,
            metric_kwargs_id=metric_id.metric_kwargs_id,
        )


class GXCloudIdentifier(DataContextKey):
    def __init__(
        self,
        resource_type: GXCloudRESTResource,
        id: str | None = None,
        resource_name: str | None = None,
    ) -> None:
        super().__init__()

        self._resource_type = resource_type
        self._id = id
        self._resource_name = resource_name

    @property
    def resource_type(self) -> GXCloudRESTResource:
        return self._resource_type

    @resource_type.setter
    def resource_type(self, value: GXCloudRESTResource) -> None:
        self._resource_type = value

    @property
    def id(self) -> str | None:
        return self._id

    @id.setter
    def id(self, value: str) -> None:
        self._id = value

    @property
    def resource_name(self) -> str | None:
        return self._resource_name

    def to_tuple(self):
        return (self.resource_type, self.id, self.resource_name)

    def to_fixed_length_tuple(self):
        return self.to_tuple()

    @classmethod
    def from_tuple(cls, tuple_):
        # Only add resource name if it exists in the tuple_
        if len(tuple_) == 3:  # noqa: PLR2004
            return cls(resource_type=tuple_[0], id=tuple_[1], resource_name=tuple_[2])
        return cls(resource_type=tuple_[0], id=tuple_[1])

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls.from_tuple(tuple_)

    def __repr__(self):
        repr = f"{self.__class__.__name__}::{self.resource_type}::{self.id}"
        if self.resource_name:
            repr += f"::{self.resource_name}"
        return repr


class ValidationResultIdentifierSchema(Schema):
    expectation_suite_identifier = fields.Nested(
        ExpectationSuiteIdentifierSchema,
        required=True,
        error_messages={
            "required": "expectation_suite_identifier is required for a ValidationResultIdentifier"
        },
    )
    run_id = fields.Nested(
        RunIdentifierSchema,
        required=True,
        error_messages={
            "required": "run_id is required for a " "ValidationResultIdentifier"
        },
    )
    batch_identifier = fields.Nested(BatchIdentifierSchema, required=True)

    # noinspection PyUnusedLocal
    @post_load
    def make_validation_result_identifier(self, data, **kwargs):
        return ValidationResultIdentifier(**data)


class SiteSectionIdentifier(DataContextKey):
    def __init__(self, site_section_name, resource_identifier) -> None:
        self._site_section_name = site_section_name
        if site_section_name in ["validations", "profiling"]:
            if isinstance(resource_identifier, ValidationResultIdentifier):
                self._resource_identifier = resource_identifier
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ValidationResultIdentifier(
                    *resource_identifier
                )
            else:
                self._resource_identifier = ValidationResultIdentifier(
                    **resource_identifier
                )
        elif site_section_name == "expectations":
            if isinstance(resource_identifier, ExpectationSuiteIdentifier):
                self._resource_identifier = resource_identifier  # type: ignore[assignment]
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ExpectationSuiteIdentifier(  # type: ignore[assignment]
                    *resource_identifier
                )
            else:
                self._resource_identifier = ExpectationSuiteIdentifier(  # type: ignore[assignment]
                    **resource_identifier
                )
        else:
            raise gx_exceptions.InvalidDataContextKeyError(
                "SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names"
            )

    @property
    def site_section_name(self):
        return self._site_section_name

    @property
    def resource_identifier(self):
        return self._resource_identifier

    def to_tuple(self):
        site_section_identifier_tuple_list = [self.site_section_name] + list(
            self.resource_identifier.to_tuple()
        )
        return tuple(site_section_identifier_tuple_list)

    @classmethod
    def from_tuple(cls, tuple_):
        if tuple_[0] == "validations":
            return cls(
                site_section_name=tuple_[0],
                resource_identifier=ValidationResultIdentifier.from_tuple(tuple_[1:]),
            )
        elif tuple_[0] == "expectations":
            return cls(
                site_section_name=tuple_[0],
                resource_identifier=ExpectationSuiteIdentifier.from_tuple(tuple_[1:]),
            )
        else:
            raise gx_exceptions.InvalidDataContextKeyError(
                "SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names"
            )


class ConfigurationIdentifier(DataContextKey):
    def __init__(self, configuration_key: str) -> None:
        super().__init__()
        if not isinstance(configuration_key, str):
            raise gx_exceptions.InvalidDataContextKeyError(
                f"configuration_key must be a string, not {type(configuration_key).__name__}"
            )
        self._configuration_key = configuration_key

    @property
    def configuration_key(self) -> str:
        return self._configuration_key

    def to_tuple(self):
        return tuple(self.configuration_key.split("."))

    def to_fixed_length_tuple(self):
        return (self.configuration_key,)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(".".join(tuple_))

    @classmethod
    def from_fixed_length_tuple(cls, tuple_):
        return cls(configuration_key=tuple_[0])

    def __repr__(self):
        return f"{self.__class__.__name__}::{self._configuration_key}"


class ConfigurationIdentifierSchema(Schema):
    configuration_key = fields.Str()

    # noinspection PyUnusedLocal
    @post_load
    def make_configuration_identifier(self, data, **kwargs):
        return ConfigurationIdentifier(**data)


expectationSuiteIdentifierSchema = ExpectationSuiteIdentifierSchema()
validationResultIdentifierSchema = ValidationResultIdentifierSchema()
runIdentifierSchema = RunIdentifierSchema()
batchIdentifierSchema = BatchIdentifierSchema()
configurationIdentifierSchema = ConfigurationIdentifierSchema()
