import abc
import enum
import logging
import uuid
from copy import deepcopy
from typing import Dict, Optional, Union

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from ruamel.yaml.compat import StringIO

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    validates_schema,
)
from great_expectations.types import DictDot, SerializableDictDot
from great_expectations.types.configurations import ClassConfigSchema

logger = logging.getLogger(__name__)

yaml = YAML()

CURRENT_CONFIG_VERSION = 2
MINIMUM_SUPPORTED_CONFIG_VERSION = 2
DEFAULT_USAGE_STATISTICS_URL = (
    "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)


def object_to_yaml_str(obj):
    output_str: str
    with StringIO() as string_stream:
        yaml.dump(obj, string_stream)
        output_str = string_stream.getvalue()
    return output_str


class AssetConfig(DictDot):
    def __init__(
        self,
        **kwargs,
    ):
        for k, v in kwargs.items():
            setattr(self, k, v)


class AssetConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    base_directory = fields.String(required=False, allow_none=True)
    glob_directive = fields.String(required=False, allow_none=True)
    pattern = fields.String(required=False, allow_none=True)
    group_names = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )

    @validates_schema
    def validate_schema(self, data, **kwargs):
        pass

    # noinspection PyUnusedLocal
    @post_load
    def make_asset_config(self, data, **kwargs):
        return AssetConfig(**data)


class SorterConfig(DictDot):
    def __init__(
        self,
        name,
        class_name=None,
        module_name=None,
        orderby="asc",
        **kwargs,
    ):
        self._name = name
        self._class_name = class_name
        self._module_name = module_name
        self._orderby = orderby
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def name(self):
        return self._name

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name

    @property
    def orderby(self):
        return self._orderby


class SorterConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    name = fields.String(required=True)
    class_name = fields.String(required=True)
    module_name = fields.String(
        missing="great_expectations.datasource.data_connector.sorter"
    )
    orderby = fields.String(required=False, missing="asc", allow_none=False)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        pass

    # noinspection PyUnusedLocal
    @post_load
    def make_sorter_config(self, data, **kwargs):
        return SorterConfig(**data)


class DataConnectorConfig(DictDot):
    def __init__(
        self,
        class_name,
        module_name=None,
        assets=None,
        base_directory=None,
        glob_directive=None,
        default_regex=None,
        runtime_keys=None,
        bucket=None,
        prefix=None,
        delimiter=None,
        max_keys=None,
        boto3_options=None,
        **kwargs,
    ):
        self._class_name = class_name
        self._module_name = module_name
        if assets is not None:
            self.assets = assets
        if base_directory is not None:
            self.base_directory = base_directory
        if glob_directive is not None:
            self.glob_directive = glob_directive
        if default_regex is not None:
            self.default_regex = default_regex
        if runtime_keys is not None:
            self.runtime_keys = runtime_keys
        if bucket is not None:
            self.bucket = bucket
        if prefix is not None:
            self.prefix = prefix
        if delimiter is not None:
            self.delimiter = delimiter
        if max_keys is not None:
            self.max_keys = max_keys
        if boto3_options is not None:
            self.boto3_options = boto3_options
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name


class DataConnectorConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(missing="great_expectations.datasource.data_connector")

    assets = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(AssetConfigSchema, required=False, allow_none=True),
        required=False,
        allow_none=True,
    )

    base_directory = fields.String(required=False, allow_none=True)
    glob_directive = fields.String(required=False, allow_none=True)
    default_regex = fields.Dict(required=False, allow_none=True)
    runtime_keys = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
    bucket = fields.String(required=False, allow_none=True)
    prefix = fields.String(required=False, allow_none=True)
    delimiter = fields.String(required=False, allow_none=True)
    max_keys = fields.Integer(required=False, allow_none=True)
    boto3_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    data_asset_name_prefix = fields.String(required=False, allow_none=True)
    data_asset_name_suffix = fields.String(required=False, allow_none=True)
    include_schema_name = fields.Boolean(required=False, allow_none=True)
    splitter_method = fields.String(required=False, allow_none=True)
    splitter_kwargs = fields.Dict(required=False, allow_none=True)
    sampling_method = fields.String(required=False, allow_none=True)
    sampling_kwargs = fields.Dict(required=False, allow_none=True)
    excluded_tables = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
    included_tables = fields.List(
        cls_or_instance=fields.Str(), required=False, allow_none=True
    )
    skip_inapplicable_tables = fields.Boolean(required=False, allow_none=True)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
        if data["class_name"][0] == "$":
            return
        if ("default_regex" in data) and not (
            data["class_name"]
            in [
                "InferredAssetFilesystemDataConnector",
                "ConfiguredAssetFilesystemDataConnector",
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
            ]
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector, that are required only by a
subclass of the FilePathDataConnector class (your data conntector is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )
        if ("glob_directive" in data) and not (
            data["class_name"]
            in [
                "InferredAssetFilesystemDataConnector",
                "ConfiguredAssetFilesystemDataConnector",
            ]
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector, that are required only by a
filesystem type of the data connector (your data conntector is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )
        if (
            "bucket" in data
            or "prefix" in data
            or "delimiter" in data
            or "max_keys" in data
        ) and not (
            data["class_name"]
            in [
                "InferredAssetS3DataConnector",
                "ConfiguredAssetS3DataConnector",
            ]
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector, that are required only by an
S3 type of the data connector (your data conntector is "{data['class_name']}").  Please update your configuration to
continue.
                """
            )
        if (
            "data_asset_name_prefix" in data
            or "data_asset_name_suffix" in data
            or "include_schema_name" in data
            or "splitter_method" in data
            or "splitter_kwargs" in data
            or "sampling_method" in data
            or "sampling_kwargs" in data
            or "excluded_tables" in data
            or "included_tables" in data
            or "skip_inapplicable_tables" in data
        ) and not (
            data["class_name"]
            in [
                "InferredAssetSqlDataConnector",
                "ConfiguredAssetSqlDataConnector",
            ]
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data connector, that are required only by an
SQL type of the data connector (your data conntector is "{data['class_name']}").  Please update your configuration to
continue.
                """
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_data_connector_config(self, data, **kwargs):
        return DataConnectorConfig(**data)


class ExecutionEngineConfig(DictDot):
    def __init__(
        self,
        class_name,
        module_name=None,
        caching=None,
        batch_spec_defaults=None,
        connection_string=None,
        spark_config=None,
        boto3_options=None,
        **kwargs,
    ):
        self._class_name = class_name
        self._module_name = module_name
        if caching is not None:
            self.caching = caching
        if batch_spec_defaults is not None:
            self._batch_spec_defaults = batch_spec_defaults
        if connection_string is not None:
            self.connection_string = connection_string
        if spark_config is not None:
            self.spark_config = spark_config
        if boto3_options is not None:
            self.boto3_options = boto3_options
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def module_name(self):
        return self._module_name

    @property
    def class_name(self):
        return self._class_name

    @property
    def batch_spec_defaults(self):
        return self._batch_spec_defaults


class ExecutionEngineConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(missing="great_expectations.execution_engine")
    connection_string = fields.String(required=False, allow_none=True)
    spark_config = fields.Raw(required=False, allow_none=True)
    boto3_options = fields.Dict(
        keys=fields.Str(), values=fields.Str(), required=False, allow_none=True
    )
    caching = fields.Boolean(required=False, allow_none=True)
    batch_spec_defaults = fields.Dict(required=False, allow_none=True)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
        if data["class_name"][0] == "$":
            return
        if "connection_string" in data and not (
            data["class_name"] == "SqlAlchemyExecutionEngine"
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses the "connection_string" key in an execution engine, but only
SqlAlchemyExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )
        if "spark_config" in data and not (
            data["class_name"] == "SparkDFExecutionEngine"
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses the "spark_config" key in an execution engine, but only
SparkDFExecutionEngine requires this attribute (your execution engine is "{data['class_name']}").  Please update your
configuration to continue.
                """
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_execution_engine_config(self, data, **kwargs):
        return ExecutionEngineConfig(**data)


class DatasourceConfig(DictDot):
    def __init__(
        self,
        class_name=None,
        module_name: Optional[str] = "great_expectations.datasource",
        execution_engine=None,
        data_connectors=None,
        data_asset_type=None,
        batch_kwargs_generators=None,
        connection_string=None,
        credentials=None,
        introspection=None,
        tables=None,
        reader_method=None,
        limit=None,
        **kwargs,
    ):
        # NOTE - JPC - 20200316: Currently, we are mostly inconsistent with respect to this type...
        self._class_name = class_name
        self._module_name = module_name
        if execution_engine is not None:
            self.execution_engine = execution_engine
        if data_connectors is not None and isinstance(data_connectors, dict):
            self.data_connectors = data_connectors

        # NOTE - AJB - 20201202: This should use the datasource class build_configuration method as in DataContext.add_datasource()
        if data_asset_type is None:
            if class_name == "PandasDatasource":
                data_asset_type = {
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset",
                }
            elif class_name == "SqlAlchemyDatasource":
                data_asset_type = {
                    "class_name": "SqlAlchemyDataset",
                    "module_name": "great_expectations.dataset",
                }
            elif class_name == "SparkDFDatasource":
                data_asset_type = {
                    "class_name": "SparkDFDataset",
                    "module_name": "great_expectations.dataset",
                }
        if data_asset_type is not None:
            self.data_asset_type = data_asset_type
        if batch_kwargs_generators is not None:
            self.batch_kwargs_generators = batch_kwargs_generators
        if connection_string is not None:
            self.connection_string = connection_string
        if credentials is not None:
            self.credentials = credentials
        if introspection is not None:
            self.introspection = introspection
        if tables is not None:
            self.tables = tables
        if reader_method is not None:
            self.reader_method = reader_method
        if limit is not None:
            self.limit = limit
        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name


class DatasourceConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(missing="Datasource")
    module_name = fields.String(missing="great_expectations.datasource")

    execution_engine = fields.Nested(
        ExecutionEngineConfigSchema, required=False, allow_none=True
    )
    data_connectors = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(DataConnectorConfigSchema),
        required=False,
        allow_none=True,
    )

    data_asset_type = fields.Nested(ClassConfigSchema, required=False, allow_none=True)

    # TODO: Update to generator-specific
    # batch_kwargs_generators = fields.Mapping(keys=fields.Str(), values=fields.Nested(fields.GeneratorSchema))
    batch_kwargs_generators = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), required=False, allow_none=True
    )
    connection_string = fields.String(required=False, allow_none=True)
    credentials = fields.Raw(required=False, allow_none=True)
    introspection = fields.Dict(required=False, allow_none=True)
    tables = fields.Dict(required=False, allow_none=True)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        if "generators" in data:
            raise ge_exceptions.InvalidConfigError(
                'Your current configuration uses the "generators" key in a datasource, but in version 0.10 of '
                'GE, that key is renamed to "batch_kwargs_generators". Please update your configuration to continue.'
            )
        # If a class_name begins with the dollar sign ("$"), then it is assumed to be a variable name to be substituted.
        if data["class_name"][0] == "$":
            return
        if (
            "connection_string" in data
            or "credentials" in data
            or "introspection" in data
            or "tables" in data
        ) and not (
            data["class_name"]
            in [
                "SqlAlchemyDatasource",
                "SimpleSqlalchemyDatasource",
            ]
        ):
            raise ge_exceptions.InvalidConfigError(
                f"""Your current configuration uses one or more keys in a data source, that are required only by a
sqlalchemy data source (your data source is "{data['class_name']}").  Please update your configuration to continue.
                """
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_datasource_config(self, data, **kwargs):
        return DatasourceConfig(**data)


class AnonymizedUsageStatisticsConfig(DictDot):
    def __init__(self, enabled=True, data_context_id=None, usage_statistics_url=None):
        self._enabled = enabled
        if data_context_id is None:
            data_context_id = str(uuid.uuid4())
            self._explicit_id = False
        else:
            self._explicit_id = True

        self._data_context_id = data_context_id
        if usage_statistics_url is None:
            usage_statistics_url = DEFAULT_USAGE_STATISTICS_URL
            self._explicit_url = False
        else:
            self._explicit_url = True
        self._usage_statistics_url = usage_statistics_url

    @property
    def enabled(self):
        return self._enabled

    @enabled.setter
    def enabled(self, enabled):
        if not isinstance(enabled, bool):
            raise ValueError("usage statistics enabled property must be boolean")
        self._enabled = enabled

    @property
    def data_context_id(self):
        return self._data_context_id

    @data_context_id.setter
    def data_context_id(self, data_context_id):
        try:
            uuid.UUID(data_context_id)
        except ValueError:
            raise ge_exceptions.InvalidConfigError(
                "data_context_id must be a valid uuid"
            )
        self._data_context_id = data_context_id
        self._explicit_id = True

    @property
    def explicit_id(self):
        return self._explicit_id

    @property
    def usage_statistics_url(self):
        return self._usage_statistics_url

    @usage_statistics_url.setter
    def usage_statistics_url(self, usage_statistics_url):
        self._usage_statistics_url = usage_statistics_url
        self._explicit_url = True


class AnonymizedUsageStatisticsConfigSchema(Schema):
    data_context_id = fields.UUID()
    enabled = fields.Boolean(default=True)
    usage_statistics_url = fields.URL(allow_none=True)
    _explicit_url = fields.Boolean(required=False)

    # noinspection PyUnusedLocal
    @post_load()
    def make_usage_statistics_config(self, data, **kwargs):
        if "data_context_id" in data:
            data["data_context_id"] = str(data["data_context_id"])
        return AnonymizedUsageStatisticsConfig(**data)

    # noinspection PyUnusedLocal
    @post_dump()
    def filter_implicit(self, data, **kwargs):
        if not data.get("_explicit_url") and "usage_statistics_url" in data:
            del data["usage_statistics_url"]
        if "_explicit_url" in data:
            del data["_explicit_url"]
        return data


class NotebookTemplateConfig(DictDot):
    def __init__(self, file_name, template_kwargs=None):
        self.file_name = file_name
        if template_kwargs:
            self.template_kwargs = template_kwargs
        else:
            self.template_kwargs = {}


class NotebookTemplateConfigSchema(Schema):
    file_name = fields.String()
    template_kwargs = fields.Dict(
        keys=fields.Str(), values=fields.Str(), allow_none=True
    )

    # noinspection PyUnusedLocal
    @post_load
    def make_notebook_template_config(self, data, **kwargs):
        return NotebookTemplateConfig(**data)


class NotebookConfig(DictDot):
    def __init__(
        self,
        class_name,
        module_name,
        custom_templates_module,
        header_markdown=None,
        footer_markdown=None,
        table_expectations_header_markdown=None,
        column_expectations_header_markdown=None,
        table_expectations_not_found_markdown=None,
        column_expectations_not_found_markdown=None,
        authoring_intro_markdown=None,
        column_expectations_markdown=None,
        header_code=None,
        footer_code=None,
        column_expectation_code=None,
        table_expectation_code=None,
    ):
        self.class_name = class_name
        self.module_name = module_name
        self.custom_templates_module = custom_templates_module

        self.header_markdown = header_markdown
        self.footer_markdown = footer_markdown
        self.table_expectations_header_markdown = table_expectations_header_markdown
        self.column_expectations_header_markdown = column_expectations_header_markdown
        self.table_expectations_not_found_markdown = (
            table_expectations_not_found_markdown
        )
        self.column_expectations_not_found_markdown = (
            column_expectations_not_found_markdown
        )
        self.authoring_intro_markdown = authoring_intro_markdown
        self.column_expectations_markdown = column_expectations_markdown

        self.header_code = header_code
        self.footer_code = footer_code
        self.column_expectation_code = column_expectation_code
        self.table_expectation_code = table_expectation_code


class NotebookConfigSchema(Schema):
    class_name = fields.String(missing="SuiteEditNotebookRenderer")
    module_name = fields.String(
        missing="great_expectations.render.renderer.suite_edit_notebook_renderer"
    )
    custom_templates_module = fields.String()

    header_markdown = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    footer_markdown = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    table_expectations_header_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_header_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    table_expectations_not_found_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_not_found_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    authoring_intro_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    column_expectations_markdown = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )

    header_code = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    footer_code = fields.Nested(NotebookTemplateConfigSchema, allow_none=True)
    column_expectation_code = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )
    table_expectation_code = fields.Nested(
        NotebookTemplateConfigSchema, allow_none=True
    )

    # noinspection PyUnusedLocal
    @post_load
    def make_notebook_config(self, data, **kwargs):
        return NotebookConfig(**data)


class NotebooksConfig(DictDot):
    def __init__(self, suite_edit):
        self.suite_edit = suite_edit


class NotebooksConfigSchema(Schema):
    # for now only suite_edit, could have other customization options for
    # notebooks in the future
    suite_edit = fields.Nested(NotebookConfigSchema)

    # noinspection PyUnusedLocal
    @post_load
    def make_notebooks_config(self, data, **kwargs):
        return NotebooksConfig(**data)


class DataContextConfigSchema(Schema):
    config_version = fields.Number(
        validate=lambda x: 0 < x < 100,
        error_messages={"invalid": "config version must " "be a number."},
    )
    datasources = fields.Dict(
        keys=fields.Str(),
        values=fields.Nested(DatasourceConfigSchema),
        required=False,
        allow_none=True,
    )
    expectations_store_name = fields.Str()
    validations_store_name = fields.Str()
    evaluation_parameter_store_name = fields.Str()
    plugins_directory = fields.Str(allow_none=True)
    validation_operators = fields.Dict(keys=fields.Str(), values=fields.Dict())
    stores = fields.Dict(keys=fields.Str(), values=fields.Dict())
    notebooks = fields.Nested(NotebooksConfigSchema, allow_none=True)
    data_docs_sites = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), allow_none=True
    )
    config_variables_file_path = fields.Str(allow_none=True)
    anonymous_usage_statistics = fields.Nested(AnonymizedUsageStatisticsConfigSchema)

    # noinspection PyMethodMayBeStatic
    # noinspection PyUnusedLocal
    def handle_error(self, exc, data, **kwargs):
        """Log and raise our custom exception when (de)serialization fails."""
        logger.error(exc.messages)
        raise ge_exceptions.InvalidDataContextConfigError(
            "Error while processing DataContextConfig.", exc
        )

    @validates_schema
    def validate_schema(self, data, **kwargs):
        if "config_version" not in data:
            raise ge_exceptions.InvalidDataContextConfigError(
                "The key `config_version` is missing; please check your config file.",
                validation_error=ValidationError("no config_version key"),
            )

        if not isinstance(data["config_version"], (int, float)):
            raise ge_exceptions.InvalidDataContextConfigError(
                "The key `config_version` must be a number. Please check your config file.",
                validation_error=ValidationError("config version not a number"),
            )

        # When migrating from 0.7.x to 0.8.0
        if data["config_version"] == 0 and (
            "validations_store" in list(data.keys())
            or "validations_stores" in list(data.keys())
        ):
            raise ge_exceptions.UnsupportedConfigVersionError(
                "You appear to be using a config version from the 0.7.x series. This version is no longer supported."
            )
        elif data["config_version"] < MINIMUM_SUPPORTED_CONFIG_VERSION:
            raise ge_exceptions.UnsupportedConfigVersionError(
                "You appear to have an invalid config version ({}).\n    The version number must be at least {}. "
                "Please see the migration guide at https://docs.greatexpectations.io/en/latest/guides/how_to_guides/migrating_versions.html".format(
                    data["config_version"], MINIMUM_SUPPORTED_CONFIG_VERSION
                ),
            )
        elif data["config_version"] > CURRENT_CONFIG_VERSION:
            raise ge_exceptions.InvalidDataContextConfigError(
                "You appear to have an invalid config version ({}).\n    The maximum valid version is {}.".format(
                    data["config_version"], CURRENT_CONFIG_VERSION
                ),
                validation_error=ValidationError("config version too high"),
            )


class DataContextConfigDefaults(enum.Enum):
    DEFAULT_CONFIG_VERSION = CURRENT_CONFIG_VERSION
    DEFAULT_EXPECTATIONS_STORE_NAME = "expectations_store"
    DEFAULT_VALIDATIONS_STORE_NAME = "validations_store"
    DEFAULT_EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store"
    DEFAULT_DATA_DOCS_SITE_NAME = "local_site"
    DEFAULT_CONFIG_VARIABLES_FILEPATH = "uncommitted/config_variables.yml"
    DEFAULT_PLUGINS_DIRECTORY = "plugins/"
    DEFAULT_VALIDATION_OPERATORS = {
        "action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "store_evaluation_params",
                    "action": {"class_name": "StoreEvaluationParametersAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        }
    }
    DEFAULT_STORES = {
        DEFAULT_EXPECTATIONS_STORE_NAME: {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "expectations/",
            },
        },
        DEFAULT_VALIDATIONS_STORE_NAME: {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/validations/",
            },
        },
        DEFAULT_EVALUATION_PARAMETER_STORE_NAME: {
            "class_name": "EvaluationParameterStore"
        },
    }
    DEFAULT_DATA_DOCS_SITES = {
        DEFAULT_DATA_DOCS_SITE_NAME: {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/local_site/",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
            },
        }
    }


class BaseStoreBackendDefaults(DictDot):
    """
    Define base defaults for platform specific StoreBackendDefaults.
    StoreBackendDefaults define defaults for specific cases of often used configurations.
    For example, if you plan to store expectations, validations, and data_docs in s3 use the S3StoreBackendDefaults and you may be able to specify less parameters.
    """

    def __init__(
        self,
        expectations_store_name: str = DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value,
        validations_store_name=(
            DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value
        ),
        evaluation_parameter_store_name=(
            DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value
        ),
        data_docs_site_name=(
            DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITE_NAME.value
        ),
        validation_operators=None,
        stores=None,
        data_docs_sites=None,
    ):
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        if validation_operators is None:
            validation_operators = deepcopy(
                DataContextConfigDefaults.DEFAULT_VALIDATION_OPERATORS.value
            )
        self.validation_operators = validation_operators
        if stores is None:
            stores = deepcopy(DataContextConfigDefaults.DEFAULT_STORES.value)
        self.stores = stores
        if data_docs_sites is None:
            data_docs_sites = deepcopy(
                DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value
            )
        self.data_docs_sites = data_docs_sites
        self.data_docs_site_name = data_docs_site_name


class S3StoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for s3 backends, with some accessible parameters
    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validations_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validations_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
    """

    def __init__(
        self,
        default_bucket_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validations_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validations_store_prefix: str = "validations",
        data_docs_prefix: str = "data_docs",
        expectations_store_name: str = "expectations_S3_store",
        validations_store_name: str = "validations_S3_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
    ):
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validations_store_bucket_name is None:
            validations_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": expectations_store_bucket_name,
                    "prefix": expectations_store_prefix,
                },
            },
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": validations_store_bucket_name,
                    "prefix": validations_store_prefix,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
        }
        self.data_docs_sites = {
            "s3_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": data_docs_bucket_name,
                    "prefix": data_docs_prefix,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        }


class FilesystemStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for filesystem backends, with some accessible parameters
    Args:
        root_directory: Absolute directory prepended to the base_directory for each store
        plugins_directory: Overrides default if supplied
    """

    def __init__(
        self,
        root_directory: Optional[str] = None,
        plugins_directory: Optional[str] = None,
    ):
        # Initialize base defaults
        super().__init__()

        if plugins_directory is None:
            plugins_directory = (
                DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            )
        self.plugins_directory = plugins_directory
        if root_directory is not None:
            self.stores[self.expectations_store_name]["store_backend"][
                "root_directory"
            ] = root_directory
            self.stores[self.validations_store_name]["store_backend"][
                "root_directory"
            ] = root_directory
            self.data_docs_sites[self.data_docs_site_name]["store_backend"][
                "root_directory"
            ] = root_directory


class GCSStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for Google Cloud Storage (GCS) backends, with some accessible parameters
    Args:
        default_bucket_name: Use this bucket name for stores that do not have a bucket name provided
        default_project_name: Use this project name for stores that do not have a project name provided
        expectations_store_bucket_name: Overrides default_bucket_name if supplied
        validations_store_bucket_name: Overrides default_bucket_name if supplied
        data_docs_bucket_name: Overrides default_bucket_name if supplied
        expectations_store_project_name: Overrides default_project_name if supplied
        validations_store_project_name: Overrides default_project_name if supplied
        data_docs_project_name: Overrides default_project_name if supplied
        expectations_store_prefix: Overrides default if supplied
        validations_store_prefix: Overrides default if supplied
        data_docs_prefix: Overrides default if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
    """

    def __init__(
        self,
        default_bucket_name: Optional[str] = None,
        default_project_name: Optional[str] = None,
        expectations_store_bucket_name: Optional[str] = None,
        validations_store_bucket_name: Optional[str] = None,
        data_docs_bucket_name: Optional[str] = None,
        expectations_store_project_name: Optional[str] = None,
        validations_store_project_name: Optional[str] = None,
        data_docs_project_name: Optional[str] = None,
        expectations_store_prefix: str = "expectations",
        validations_store_prefix: str = "validations",
        data_docs_prefix: str = "data_docs",
        expectations_store_name: str = "expectations_GCS_store",
        validations_store_name: str = "validations_GCS_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
    ):
        # Initialize base defaults
        super().__init__()

        # Use default_bucket_name if separate store buckets are not provided
        if expectations_store_bucket_name is None:
            expectations_store_bucket_name = default_bucket_name
        if validations_store_bucket_name is None:
            validations_store_bucket_name = default_bucket_name
        if data_docs_bucket_name is None:
            data_docs_bucket_name = default_bucket_name

        # Use default_project_name if separate store projects are not provided
        if expectations_store_project_name is None:
            expectations_store_project_name = default_project_name
        if validations_store_project_name is None:
            validations_store_project_name = default_project_name
        if data_docs_project_name is None:
            data_docs_project_name = default_project_name

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": expectations_store_project_name,
                    "bucket": expectations_store_bucket_name,
                    "prefix": expectations_store_prefix,
                },
            },
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": validations_store_project_name,
                    "bucket": validations_store_bucket_name,
                    "prefix": validations_store_prefix,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
        }
        self.data_docs_sites = {
            "gcs_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleGCSStoreBackend",
                    "project": data_docs_project_name,
                    "bucket": data_docs_bucket_name,
                    "prefix": data_docs_prefix,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        }


class DatabaseStoreBackendDefaults(BaseStoreBackendDefaults):
    """
    Default store configs for database backends, with some accessible parameters
    Args:
        default_credentials: Use these credentials for all stores that do not have credentials provided
        expectations_store_credentials: Overrides default_credentials if supplied
        validations_store_credentials: Overrides default_credentials if supplied
        expectations_store_name: Overrides default if supplied
        validations_store_name: Overrides default if supplied
        evaluation_parameter_store_name: Overrides default if supplied
    """

    def __init__(
        self,
        default_credentials: Optional[Dict] = None,
        expectations_store_credentials: Optional[Dict] = None,
        validations_store_credentials: Optional[Dict] = None,
        expectations_store_name: str = "expectations_database_store",
        validations_store_name: str = "validations_database_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
    ):
        # Initialize base defaults
        super().__init__()

        # Use default credentials if seprate credentials not supplied for expectations_store and validations_store
        if expectations_store_credentials is None:
            expectations_store_credentials = default_credentials
        if validations_store_credentials is None:
            validations_store_credentials = default_credentials

        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name

        self.stores = {
            expectations_store_name: {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": expectations_store_credentials,
                },
            },
            validations_store_name: {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "DatabaseStoreBackend",
                    "credentials": validations_store_credentials,
                },
            },
            evaluation_parameter_store_name: {"class_name": "EvaluationParameterStore"},
        }


class DataContextConfig(SerializableDictDot):
    def __init__(
        self,
        config_version: Optional[float] = None,
        datasources: Optional[
            Union[
                Dict[str, DatasourceConfig],
                Dict[str, Dict[str, Union[Dict[str, str], str, dict]]],
            ]
        ] = None,
        expectations_store_name: Optional[str] = None,
        validations_store_name: Optional[str] = None,
        evaluation_parameter_store_name: Optional[str] = None,
        plugins_directory: Optional[str] = None,
        validation_operators=None,
        stores: Optional[Dict] = None,
        data_docs_sites: Optional[Dict] = None,
        notebooks=None,
        config_variables_file_path: Optional[str] = None,
        anonymous_usage_statistics=None,
        commented_map=None,
        store_backend_defaults: Optional[BaseStoreBackendDefaults] = None,
    ):
        # Set defaults
        if config_version is None:
            config_version = DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value

        # Set defaults via store_backend_defaults if one is passed in
        # Override attributes from store_backend_defaults with any items passed into the constructor:
        if store_backend_defaults is not None:
            if stores is None:
                stores = store_backend_defaults.stores
            if expectations_store_name is None:
                expectations_store_name = store_backend_defaults.expectations_store_name
            if validations_store_name is None:
                validations_store_name = store_backend_defaults.validations_store_name
            if evaluation_parameter_store_name is None:
                evaluation_parameter_store_name = (
                    store_backend_defaults.evaluation_parameter_store_name
                )
            if validation_operators is None:
                validation_operators = store_backend_defaults.validation_operators
            if data_docs_sites is None:
                data_docs_sites = store_backend_defaults.data_docs_sites

        if commented_map is None:
            commented_map = CommentedMap()
        self._commented_map = commented_map
        self._config_version = config_version
        if datasources is None:
            datasources = {}
        self.datasources = datasources
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        self.plugins_directory = plugins_directory
        if not isinstance(validation_operators, dict):
            raise ValueError(
                "validation_operators must be configured with a dictionary"
            )
        self.validation_operators = validation_operators
        self.stores = stores
        self.notebooks = notebooks
        self.data_docs_sites = data_docs_sites
        self.config_variables_file_path = config_variables_file_path
        if anonymous_usage_statistics is None:
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig()
        elif isinstance(anonymous_usage_statistics, dict):
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig(
                **anonymous_usage_statistics
            )
        self.anonymous_usage_statistics = anonymous_usage_statistics

    @property
    def commented_map(self):
        return self._commented_map

    @property
    def config_version(self):
        return self._config_version

    @classmethod
    def from_commented_map(cls, commented_map):
        try:
            config = dataContextConfigSchema.load(commented_map)
            return cls(commented_map=commented_map, **config)
        except ValidationError:
            logger.error(
                "Encountered errors during loading data context config. See ValidationError for more details."
            )
            raise

    def get_schema_validated_updated_commented_map(self) -> CommentedMap:
        commented_map: CommentedMap = deepcopy(self.commented_map)
        commented_map.update(dataContextConfigSchema.dump(self))
        return commented_map

    def to_yaml(self, outfile):
        """
        :returns None (but writes a YAML file containing the project configuration)
        """
        yaml.dump(self.get_schema_validated_updated_commented_map(), outfile)

    def to_yaml_str(self) -> str:
        """
        :returns a YAML string containing the project configuration
        """
        return object_to_yaml_str(self.get_schema_validated_updated_commented_map())

    def to_json_dict(self) -> dict:
        """
        :returns a JSON-serialiable dict containing the project configuration
        """
        commented_map: CommentedMap = self.get_schema_validated_updated_commented_map()
        return convert_to_json_serializable(data=commented_map)


dataContextConfigSchema = DataContextConfigSchema()
datasourceConfigSchema = DatasourceConfigSchema()
dataConnectorConfigSchema = DataConnectorConfigSchema()
assetConfigSchema = AssetConfigSchema()
sorterConfigSchema = SorterConfigSchema()
anonymizedUsageStatisticsSchema = AnonymizedUsageStatisticsConfigSchema()
notebookConfigSchema = NotebookConfigSchema()
