from marshmallow import Schema, fields, validates_schema, ValidationError, post_load, pre_dump

from great_expectations.data_context.types.resource_identifiers import DataAssetIdentifier

from great_expectations import __version__ as ge_version


class DictDot(object):
    def __getitem__(self, item):
        return getattr(self, item)

    def __setitem__(self, key, value):
        self.key = value


class ExpectationKwargs(DictDot):
    """ExpectationKwargs store all permanent information necessary to evaluate an expectation."""
    def __init__(self, include_config=False):
        self._include_config = include_config

    @property
    def include_config(self):
        return self._include_config


class ExpectationKwargsSchema(Schema):
    include_config = fields.Bool()


expectationKwargsSchema = ExpectationKwargsSchema()


class ExpectationConfiguration(DictDot):
    def __init__(self, expectation_type, kwargs, meta=None):
        self._expectation_type = expectation_type
        self._kwargs = kwargs
        if meta is None:
            meta = {}
        self._meta = meta

    @property
    def expectation_type(self):
        return self._expectation_type

    @property
    def kwargs(self):
        return self._kwargs

    @property
    def meta(self):
        return self._meta


class ExpectationConfigurationSchema(Schema):
    expectation_type = fields.Str(
        required=True,
        error_messages={"required": "expectation_type missing in expectation configuration"}
    )
    kwargs = fields.Nested(ExpectationKwargsSchema)


class GreatExpectationsMetaStore(dict):
    """The GreatExpectationsMetaStore """

    @validates_schema
    def validate_json_serializable(self, data, **kwargs):
        import json
        try:
            json.dumps(data)
        except (TypeError, OverflowError):
            raise ValidationError("MetaStore information must be json serializable.")


class ExpectationSuite(DictDot):
    def __init__(self, data_asset_name, expectation_suite_name, data_asset_type, meta, expectations):
        self.data_asset_name = data_asset_name
        self.expectation_suite_name = expectation_suite_name
        self.data_asset_type = data_asset_type
        self.meta = meta
        self.expectations = expectations


class ExpectationSuiteSchema(Schema):
    data_asset_name = fields.Nested(DataAssetIdentifier)
    expectation_suite_name = fields.Str()
    data_asset_type = fields.Str()
    meta = fields.Nested(GreatExpectationsMetaStore)
    expectations = fields.List(fields.Nested(ExpectationConfigurationSchema))

    # noinspection PyUnusedLocal
    @post_load
    def make_expectation_suite(self, data, **kwargs):
        return ExpectationSuite(**data)


def get_empty_expectation_suite(data_asset_name=None, expectation_suite_name="default"):
    return ExpectationSuite(
        data_asset_name=data_asset_name,
        expectation_suite_name=expectation_suite_name,
        meta=GreatExpectationsMetaStore({
            "great_expectations.__version__": ge_version
        }),
        expectations=[]
    )
