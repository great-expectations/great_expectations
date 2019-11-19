from copy import deepcopy
import logging

from marshmallow import Schema, fields, ValidationError, pre_dump, post_load, validates_schema
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.types import DictDot
from great_expectations.types.configurations import ClassConfigSchema

logger = logging.getLogger(__name__)

yaml = YAML()

CURRENT_CONFIG_VERSION = 1
MINIMUM_SUPPORTED_CONFIG_VERSION = 1


class DataContextConfig(DictDot):

    def __init__(
            self,
            config_version,
            datasources,
            expectations_store_name,
            validations_store_name,
            evaluation_parameter_store_name,
            plugins_directory,
            validation_operators,
            stores,
            data_docs_sites,
            config_variables_file_path=None,
            commented_map=None
    ):
        if commented_map is None:
            commented_map = CommentedMap()
        self._commented_map = commented_map
        self._config_version = config_version
        self.datasources = datasources
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.plugins_directory = plugins_directory
        self.validation_operators = validation_operators
        self.stores = stores
        self.data_docs_sites = data_docs_sites
        self.config_variables_file_path = config_variables_file_path

    @property
    def commented_map(self):
        return self._commented_map

    @classmethod
    def from_commented_map(cls, commented_map):
        try:
            config = dataContextConfigSchema.load(commented_map).data
            return cls(commented_map=commented_map, **config)
        except ValidationError:
            logger.error("Encountered errors during loading data context config. See ValidationError for more details.")
            raise

    def to_yaml(self, outfile):
        commented_map = deepcopy(self.commented_map)
        commented_map.update(dataContextConfigSchema.dump(self).data)
        yaml.dump(commented_map, outfile)

    def as_dict(self):
        myself = {
            "config_version": self._config_version,
            "datasources": self.datasources,
            "expectations_store_name": self.expectations_store_name,
            "validations_store_name": self.validations_store_name,
            "evaluation_parameter_store_name": self.evaluation_parameter_store_name,
            "plugins_directory": self.plugins_directory,
            "validation_operators": self.validation_operators,
            "stores": self.stores,
            "data_docs_sites": self.data_docs_sites,
            "config_variables_file_path": self.config_variables_file_path,
        }
        if self.config_variables_file_path is None:
            del myself['config_variables_file_path']
        return myself


class DatasourceConfig(DictDot):
    def __init__(self, class_name, module_name=None, data_asset_type=None, generators=None):
        self._class_name = class_name
        self._module_name = module_name
        self.data_asset_type = data_asset_type,
        self.generators = generators

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self.module_name


class DatasourceConfigSchema(Schema):
    class_name = fields.Str()
    module_name = fields.Str(allow_none=True)
    data_asset_type = fields.Nested(ClassConfigSchema)
    # TODO: Update to generator-specific
    # generators = fields.Mapping(keys=fields.Str(), values=fields.Nested(fields.GeneratorSchema))
    generators = fields.Dict(keys=fields.Str(), values=fields.Dict())
    credentials = fields.Raw(allow_none=True)

    # noinspection PyUnusedLocal
    @post_load
    def make_datasource_config(self, data, **kwargs):
        return DatasourceConfig(**data)


class DataContextConfigSchema(Schema):
    config_version = fields.Number(validate=lambda x: 0 < x < 100, error_messages={"invalid": "BLARG!"})
    datasources = fields.Dict(keys=fields.Str(), values=fields.Nested(DatasourceConfigSchema))
    expectations_store_name = fields.Str()
    validations_store_name = fields.Str()
    evaluation_parameter_store_name = fields.Str()
    plugins_directory = fields.Str()
    validation_operators = fields.Dict(keys=fields.Str(), values=fields.Dict())
    stores = fields.Dict(keys=fields.Str(), values=fields.Dict())
    data_docs_sites = fields.Dict(keys=fields.Str(), values=fields.Dict(), allow_none=True)
    config_variables_file_path = fields.Str(allow_none=True)

    # noinspection PyUnusedLocal
    @pre_dump
    def handle_nested_dictionaries(self, data, **kwargs):
        # PY2 support -> this local handling will be unnecessary when we can upgrade to marshmallow 3
        datasources = {name: datasourceConfigSchema.dump(config).data for name, config in data['datasources'].items()}
        data['datasources'] = datasources
        return data

    # noinspection PyMethodMayBeStatic
    # noinspection PyUnusedLocal
    def handle_error(self, exc, data, **kwargs):
        """Log and raise our custom exception when (de)serialization fails."""
        logger.error(exc.messages)
        raise ge_exceptions.InvalidDataContextConfigError("Error while processing DataContextConfig.",
                                                          exc)

    @validates_schema
    def validate_schema(self, data):
        if 'config_version' not in data:
            raise ge_exceptions.InvalidDataContextConfigError(
                "The key `config_version` is missing; please check your config file.",
                validation_error=ValidationError("no config_version key"))

        if not isinstance(data['config_version'], (int, float)):
            raise ge_exceptions.InvalidDataContextConfigError(
                "The key `config_version` must be a number. Please check your config file.",
                validation_error=ValidationError("config version not a number")
            )

        # When migrating from 0.7.x to 0.8.0
        if data['config_version'] == 0 and (
                "validations_store" in list(data.keys()) or "validations_stores" in list(data.keys())):
            raise ge_exceptions.ZeroDotSevenConfigVersionError(
                "You appear to be using a config version from the 0.7.x series. This version is no longer supported."
            )
        elif data['config_version'] < MINIMUM_SUPPORTED_CONFIG_VERSION:
            raise ge_exceptions.UnsupportedConfigVersionError(
                "You appear to have an invalid config version ({}).\n    The version number must be between {} and {}.".format(
                    data['config_version'],
                    MINIMUM_SUPPORTED_CONFIG_VERSION,
                    CURRENT_CONFIG_VERSION,
                ),
            )
        elif data['config_version'] > CURRENT_CONFIG_VERSION:
            raise ge_exceptions.InvalidDataContextConfigError(
                "You appear to have an invalid config version ({}).\n    The maximum valid version is {}.".format(
                    data['config_version'],
                    CURRENT_CONFIG_VERSION
                ),
                validation_error=ValidationError("config version too high")
            )

    #
    # # noinspection PyUnusedLocal
    # @post_load
    # def make_data_context_config(self, data, **kwargs):
    #     return DataContextConfig(**data)


dataContextConfigSchema = DataContextConfigSchema(strict=True)
datasourceConfigSchema = DatasourceConfigSchema(strict=True)
