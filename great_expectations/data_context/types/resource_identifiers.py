import logging

from six import PY3

from marshmallow import Schema, fields, ValidationError, pre_load, post_load

from .base_resource_identifiers import DataContextKey
from great_expectations.exceptions import InvalidDataContextKeyError

logger = logging.getLogger(__name__)


class DataAssetIdentifier(DataContextKey):

    prioritized_valid_delimiter_list = ["/", "."]

    def __init__(self, datasource, generator, generator_asset, delimiter='/'):
        self._datasource = datasource
        self._generator = generator
        self._generator_asset = generator_asset
        if delimiter not in self.prioritized_valid_delimiter_list:
            raise InvalidDataContextKeyError("'%s' is not a valid delimiter for DataAssetIdentifier; valid delimiters"
                                             "are %s" % (delimiter, str(self.prioritized_valid_delimiter_list)))
        self._delimiter = delimiter

    @property
    def datasource(self):
        return self._datasource

    @property
    def generator(self):
        return self._generator

    @property
    def generator_asset(self):
        return self._generator_asset

    def to_tuple(self):
        return self.datasource, self.generator, self.generator_asset

    #####
    # DataAssetIdentifier is treated specially to make it very string friendly.
    # Use DataAssetIdentifierSchema to load from a string
    #####

    def to_path(self):
        return "/".join((
            self.datasource.replace("/", "__"),
            self.generator.replace("/", "__"),
            self.generator_asset.replace("/", "__")
        ))

    def __str__(self):
        return self._delimiter.join((self.datasource, self.generator, self.generator_asset))

    def __repr__(self):
        return self._delimiter.join((self.datasource, self.generator, self.generator_asset))


class DataAssetIdentifierSchema(Schema):
    datasource = fields.Str(required=True)
    generator = fields.Str(required=True)
    generator_asset = fields.Str(required=True)

    # noinspection PyUnusedLocal
    @pre_load(pass_many=False)
    def parse_from_string(self, data, **kwargs):
        for delimiter in DataAssetIdentifier.prioritized_valid_delimiter_list:
            if data.count(delimiter) == 2:
                data_split = data.split(delimiter)
                return {
                    "datasource": data_split[0],
                    "generator": data_split[1],
                    "generator_asset": data_split[2]
                }
        raise ValidationError("No valid delimiter found to parse DataAssetIdentifier: tried %s"
                              % str(self.prioritized_delimiter_list))

    # noinspection PyUnusedLocal
    @post_load(pass_many=False)
    def make_data_asset_identifier(self, data, **kwargs):
        return DataAssetIdentifier(**data)

#
#
# # TODO: Rename to DataAssetKey, for consistency
# class DataAssetIdentifier(OrderedDataContextKey):
#
#     def __init__(self, *args, **kwargs):
#         delimiter = kwargs.pop('delimiter', '/')
#         super(DataAssetIdentifier, self).__init__(*args, **kwargs)
#         self.__delimiter = delimiter
#
#     _key_order = [
#         "datasource",
#         "generator",
#         "generator_asset"
#     ]
#     _key_types = {
#         "datasource": string_types,
#         "generator": string_types,
#         "generator_asset": string_types
#     }
#     # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
#     _required_keys = set(_key_order)
#     _allowed_keys = set(_key_order) | {"_DataAssetIdentifier__delimiter"}
#
#     def __str__(self):
#         return self.__delimiter.join(
#             (self.datasource,
#              self.generator,
#              self.generator_asset)
#         )
#
#     def __repr__(self):
#         return str(self)


# # TODO: Rename to ExpectationSuiteKey, for consistency
# class ExpectationSuiteIdentifier(OrderedDataContextKey):
#     _key_order = [
#         "data_asset_name",
#         "expectation_suite_name",
#     ]
#     _key_types = {
#         "data_asset_name" : DataAssetIdentifier,
#         "expectation_suite_name" : string_types,
#     }
#     # NOTE: This pattern is kinda awkward. It would be nice to ONLY specify _key_order
#     _required_keys = set(_key_order)
#     _allowed_keys = set(_key_order)


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
        if site_section_name == "validations":
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
