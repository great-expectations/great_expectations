import logging

from six import PY3

from marshmallow import Schema, fields, post_load

from great_expectations.core import DataContextKey, DataAssetIdentifier, DataAssetIdentifierSchema
from great_expectations.exceptions import InvalidDataContextKeyError

logger = logging.getLogger(__name__)


class ExpectationSuiteIdentifier(DataContextKey):

    def __init__(self, data_asset_name, expectation_suite_name):
        super(ExpectationSuiteIdentifier, self).__init__()
        if isinstance(data_asset_name, DataAssetIdentifier):
            self._data_asset_name = data_asset_name
        elif isinstance(data_asset_name, (tuple, list)):
            self._data_asset_name = DataAssetIdentifier(*data_asset_name)
        else:
            self._data_asset_name = DataAssetIdentifier(**data_asset_name)
        self._expectation_suite_name = expectation_suite_name

    @property
    def data_asset_name(self):
        return self._data_asset_name

    @property
    def expectation_suite_name(self):
        return self._expectation_suite_name

    def to_tuple(self):
        if PY3:
            return (*self.data_asset_name.to_tuple(), self.expectation_suite_name)
        else:
            expectation_suite_identifier_tuple_list = \
                list(self.data_asset_name.to_tuple()) + [self.expectation_suite_name]
            return tuple(expectation_suite_identifier_tuple_list)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(
            data_asset_name=DataAssetIdentifier.from_tuple(tuple_[:-1]),
            expectation_suite_name=tuple_[-1]
        )


class ExpectationSuiteIdentifierSchema(Schema):
    data_asset_name = fields.Nested(DataAssetIdentifierSchema)
    expectation_suite_name = fields.Str()

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite_identifier(self, data, **kwargs):
        return ExpectationSuiteIdentifier(**data)


class ValidationResultIdentifier(DataContextKey):
    """A ValidationResultIdentifier identifies a validation result by the fully qualified expectation_suite_identifer
    and run_id.
    """

    def __init__(self, expectation_suite_identifier, run_id):
        """Constructs a ValidationResultIdentifier

        Args:
            expectation_suite_identifier (ExpectationSuiteIdentifier, list, tuple, or dict):
                identifying information for the fully qualified expectation suite used to validate
            run_id (str): The run_id for which validation occurred
        """
        super(ValidationResultIdentifier, self).__init__()
        if isinstance(expectation_suite_identifier, ExpectationSuiteIdentifier):
            self._expectation_suite_identifier = expectation_suite_identifier
        elif isinstance(expectation_suite_identifier, (tuple, list)):
            self._expectation_suite_identifier = ExpectationSuiteIdentifier(*expectation_suite_identifier)
        else:
            self._expectation_suite_identifier = ExpectationSuiteIdentifier(**expectation_suite_identifier)
        self._run_id = run_id

    @property
    def expectation_suite_identifier(self):
        return self._expectation_suite_identifier

    @property
    def run_id(self):
        return self._run_id

    def to_tuple(self):
        if PY3:
            return (*self.expectation_suite_identifier.to_tuple(), self.run_id)
        else:
            validation_result_identifier_tuple_list = list(self.expectation_suite_identifier.to_tuple()) + [self.run_id]
            return tuple(validation_result_identifier_tuple_list)

    @classmethod
    def from_tuple(cls, tuple_):
        return cls(
            expectation_suite_identifier=ExpectationSuiteIdentifier.from_tuple(tuple_[:-1]),
            run_id=tuple_[-1]
        )


class ValidationResultIdentifierSchema(Schema):
    expectation_suite_identifier = fields.Nested(ExpectationSuiteIdentifierSchema, required=True, error_messages={
        'required': 'expectation_suite_identifier is required for a ValidationResultIdentifier'})
    run_id = fields.Str(required=True, error_messages={'required': "run_id is required for a "
                                                                   "ValidationResultIdentifier"})

    # noinspection PyUnusedLocal
    @post_load
    def make_validation_result_identifier(self, data, **kwargs):
        return ValidationResultIdentifier(**data)

# # TODO: Rename to ValidatioResultKey, for consistency
# class ValidationResultIdentifier(OrderedDataContextKey):
#     _key_order = [
#         "expectation_suite_identifier",
#         "run_id",
#         # "purpose"
#     ]
#     _key_types = {
#         "expectation_suite_identifier": ExpectationSuiteIdentifier,
#         "run_id": string_types
#     }
#     # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
#     _required_keys = set(_key_order)
#     _allowed_keys = set(_key_order)


class SiteSectionIdentifier(DataContextKey):
    def __init__(self, site_section_name, resource_identifier):
        self._site_section_name = site_section_name
        if site_section_name in ["validations", "profiling"]:
            if isinstance(resource_identifier, ValidationResultIdentifier):
                self._resource_identifier = resource_identifier
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ValidationResultIdentifier(*resource_identifier)
            else:
                self._resource_identifier = ValidationResultIdentifier(**resource_identifier)
        elif site_section_name == "expectations":
            if isinstance(resource_identifier, ExpectationSuiteIdentifier):
                self._resource_identifier = resource_identifier
            elif isinstance(resource_identifier, (tuple, list)):
                self._resource_identifier = ExpectationSuiteIdentifier(*resource_identifier)
            else:
                self._resource_identifier = ExpectationSuiteIdentifier(**resource_identifier)
        else:
            raise InvalidDataContextKeyError(
                "SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names"
            )

    @property
    def site_section_name(self):
        return self._site_section_name

    @property
    def resource_identifier(self):
        return self._resource_identifier

    def to_tuple(self):
        if PY3:
            return (self.site_section_name, *self.resource_identifier.to_tuple())
        else:
            site_section_identifier_tuple_list = [self.site_section_name] + list(self.resource_identifier.to_tuple())
            return tuple(site_section_identifier_tuple_list)

    @classmethod
    def from_tuple(cls, tuple_):
        if tuple_[0] == "validations":
            return cls(
                site_section_name=tuple_[0],
                resource_identifier=ValidationResultIdentifier.from_tuple(tuple_[1:])
            )
        elif tuple_[0] == "expectations":
            return cls(
                site_section_name=tuple_[0],
                resource_identifier=ExpectationSuiteIdentifier.from_tuple(tuple_[1:])
            )
        else:
            raise InvalidDataContextKeyError(
                "SiteSectionIdentifier only supports 'validations' and 'expectations' as site section names"
            )

#
# # TODO: Rename to SiteSectionKey, for consistency
# class SiteSectionIdentifier(DataContextKey):
#     _required_keys = set([
#         "site_section_name",
#         "resource_identifier",
#     ])
#     _allowed_keys = _required_keys
#     _key_types = {
#         "site_section_name" : string_types,
#         "resource_identifier" : DataContextKey,
#         # "resource_identifier", ... is NOT strictly typed, since it can contain any type of ResourceIdentifier
#     }
#
#     def __hash__(self):
#         return hash((self.site_section_name, self.resource_identifier.to_tuple()))
#
#     def __eq__(self, other):
#         print(self)
#         print(other)
#         return self.__hash__() == other.__hash__()


dataAssetIdentifierSchema = DataAssetIdentifierSchema(strict=True)
expectationSuiteIdentifierSchema = ExpectationSuiteIdentifierSchema(strict=True)
validationResultIdentifierSchema = ValidationResultIdentifierSchema(strict=True)
