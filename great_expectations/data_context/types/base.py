import abc
import enum
import logging
import uuid
from copy import deepcopy
from typing import Dict, Optional, Union

from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.marshmallow__shade import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    validates_schema,
)
from great_expectations.types import DictDot
from great_expectations.types.configurations import ClassConfigSchema

logger = logging.getLogger(__name__)

yaml = YAML()

CURRENT_CONFIG_VERSION = 2
MINIMUM_SUPPORTED_CONFIG_VERSION = 2
DEFAULT_USAGE_STATISTICS_URL = (
    "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)


class DatasourceDefaults(metaclass=abc.ABCMeta):
    """

    """

    # TODO: Should MODULE_NAME be used in datasource.py as the default?
    # TODO: Should we make this class abstract and raise a NotImplementedError
    #  if a subclass doesn't have a CLASS_NAME or DATA_ASSET_TYPE?
    MODULE_NAME = "great_expectations.datasource"


class PandasDatasourceDefaults(DatasourceDefaults):
    """

    """

    CLASS_NAME = "PandasDatasource"
    DATA_ASSET_TYPE = {
        "module_name": "great_expectations.dataset",
        "class_name": "PandasDataset",
    }


class SqlAlchemyDatasourceDefaults(DatasourceDefaults):
    """

    """

    CLASS_NAME = "SqlAlchemyDatasource"
    DATA_ASSET_TYPE = {
        "module_name": "great_expectations.dataset",
        "class_name": "SqlAlchemyDataset",
    }


class SparkDFDatasourceDefaults(DatasourceDefaults):
    """

    """

    CLASS_NAME = "SparkDFDatasource"
    DATA_ASSET_TYPE = {
        "module_name": "great_expectations.dataset",
        "class_name": "SparkDFDataset",
    }


class DatasourceConfig(DictDot):
    def set_defaults(
        self,
        class_name: Optional[str],
        module_name: Optional[str],
        data_asset_type: Optional[dict],
    ):
        """
        Return defaults based on the type of datasource being configured
        Args:
            class_name: Datasource class name
            module_name: Datasource module name
            data_asset_type: module_name and class_name for the data_asset e.g. for Pandas dataset: {
                "module_name": "great_expectations.dataset",
                "class_name": "PandasDataset",
            }

        Returns:
            module_name and data_asset_type as configured or defaults
        """
        defaults_class = None
        if class_name == PandasDatasourceDefaults.CLASS_NAME:
            defaults_class = PandasDatasourceDefaults
        elif class_name == SqlAlchemyDatasourceDefaults.CLASS_NAME:
            defaults_class = SqlAlchemyDatasourceDefaults
        elif class_name == SparkDFDatasourceDefaults.CLASS_NAME:
            defaults_class = SparkDFDatasourceDefaults

        data_asset_type = (
            defaults_class.DATA_ASSET_TYPE
            if data_asset_type is None
            else data_asset_type
        )
        module_name = defaults_class.MODULE_NAME if module_name is None else module_name

        return module_name, data_asset_type

    def __init__(
        self,
        class_name,
        module_name=None,
        data_asset_type=None,
        batch_kwargs_generators=None,
        credentials=None,
        boto3_options=None,
        reader_method=None,
        limit=None,
        **kwargs
    ):
        # Set Defaults
        module_name, data_asset_type = self.set_defaults(
            class_name, module_name, data_asset_type
        )

        # NOTE - JPC - 20200316: Currently, we are mostly inconsistent with respect to this type...
        self._class_name = class_name
        self._module_name = module_name
        self.data_asset_type = data_asset_type
        if batch_kwargs_generators is not None:
            self.batch_kwargs_generators = batch_kwargs_generators
        if credentials is not None:
            self.credentials = credentials
        if reader_method is not None:
            self.reader_method = reader_method
        if limit is not None:
            self.limit = limit
        for k, v in kwargs.items():
            setattr(self, k, v)
        if boto3_options is not None:
            self.boto3_options = boto3_options

    @property
    def class_name(self):
        return self._class_name

    @property
    def module_name(self):
        return self._module_name


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


class DatasourceConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(missing="great_expectations.datasource")
    data_asset_type = fields.Nested(ClassConfigSchema)
    boto3_options = fields.Dict(keys=fields.Str(), values=fields.Str(), allow_none=True)
    # TODO: Update to generator-specific
    # batch_kwargs_generators = fields.Mapping(keys=fields.Str(), values=fields.Nested(fields.GeneratorSchema))
    batch_kwargs_generators = fields.Dict(
        keys=fields.Str(), values=fields.Dict(), allow_none=True
    )
    credentials = fields.Raw(allow_none=True)
    spark_context = fields.Raw(allow_none=True)

    @validates_schema
    def validate_schema(self, data, **kwargs):
        if "generators" in data:
            raise ge_exceptions.InvalidConfigError(
                "Your current configuration uses the 'generators' key in a datasource, but in version 0.10 of "
                "GE, that key is renamed to 'batch_kwargs_generators'. Please update your config to continue."
            )

    # noinspection PyUnusedLocal
    @post_load
    def make_datasource_config(self, data, **kwargs):
        return DatasourceConfig(**data)


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
        keys=fields.Str(), values=fields.Nested(DatasourceConfigSchema)
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
    # TODO: should default plugins_directory be "plugins/" or None?
    #  I think it may depend on in-memory or not. Is there checking
    #  for this directory when it is used and graceful handling if not found?

    DEFAULT_CONFIG_VERSION = CURRENT_CONFIG_VERSION
    DEFAULT_EXPECTATIONS_STORE_NAME = "expectations_store"
    DEFAULT_VALIDATIONS_STORE_NAME = "validations_store"
    DEFAULT_EVALUATION_PARAMETER_STORE_NAME = "evaluation_parameter_store"
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
        "expectations_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "expectations/",
            },
        },
        "validations_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/validations/",
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    }
    DEFAULT_DATA_DOCS_SITES = {
        "local_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "base_directory": "uncommitted/data_docs/local_site/",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "show_how_to_buttons": True,
        }
    }


class BaseBackendEcosystem(DictDot):
    """
    Define base defaults
    """

    def __init__(self):
        self.config_version = DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value
        self.expectations_store_name: str = DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value
        self.validations_store_name = (
            DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value
        )
        self.evaluation_parameter_store_name = (
            DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value
        )
        self.stores = DataContextConfigDefaults.DEFAULT_STORES.value
        self.data_docs_sites = DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value


class S3BackendEcosystem(BaseBackendEcosystem):
    def __init__(
        self,
        default_bucket_name: str,
        expectations_store_bucket_name: str = None,
        validations_store_bucket_name: str = None,
        data_docs_bucket_name: str = None,
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
        expectations_store_bucket_name = (
            expectations_store_bucket_name
            if expectations_store_bucket_name is not None
            else default_bucket_name
        )
        validations_store_bucket_name = (
            validations_store_bucket_name
            if validations_store_bucket_name is not None
            else default_bucket_name
        )
        data_docs_bucket_name = (
            data_docs_bucket_name
            if data_docs_bucket_name is not None
            else default_bucket_name
        )
        # Overwrite defaults
        self.expectations_store_name = expectations_store_name
        self.validations_store_name = validations_store_name
        self.evaluation_parameter_store_name = evaluation_parameter_store_name
        # TODO: Instead of creating from scratch, modify existing default to keep a bit more DRY?
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
                    "show_cta_footer": True,
                },
            }
        }


class FilesystemBackendEcosystem(BaseBackendEcosystem):
    """
    The default backend BaseBackendEcosystem is currently filesystem based. This class is just a convenience to make that explicit, and a placeholder for future modifications. This should be used in place of BaseBackendEcosystem.
    """

    pass


class GCSBackendEcosystem(BaseBackendEcosystem):
    def __init__(
        self,
        default_bucket_name: str,
        default_project_name: str,
        expectations_store_bucket_name: str = None,
        validations_store_bucket_name: str = None,
        data_docs_bucket_name: str = None,
        expectations_store_project_name: str = None,
        validations_store_project_name: str = None,
        data_docs_project_name: str = None,
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
        expectations_store_bucket_name = (
            expectations_store_bucket_name
            if expectations_store_bucket_name is not None
            else default_bucket_name
        )
        validations_store_bucket_name = (
            validations_store_bucket_name
            if validations_store_bucket_name is not None
            else default_bucket_name
        )
        data_docs_bucket_name = (
            data_docs_bucket_name
            if data_docs_bucket_name is not None
            else default_bucket_name
        )

        # Use default_project_name if separate store projects are not provided
        expectations_store_project_name = (
            expectations_store_project_name
            if expectations_store_project_name is not None
            else default_project_name
        )
        validations_store_project_name = (
            validations_store_project_name
            if validations_store_project_name is not None
            else default_project_name
        )
        data_docs_project_name = (
            data_docs_project_name
            if data_docs_project_name is not None
            else default_project_name
        )
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
                    "show_cta_footer": True,
                },
            }
        }


class DatabaseBackendEcosystem(BaseBackendEcosystem):
    def __init__(
        self,
        default_credentials: Dict = None,
        expectations_store_credentials: Dict = None,
        validations_store_credentials: Dict = None,
        expectations_store_name: str = "expectations_database_store",
        validations_store_name: str = "validations_database_store",
        evaluation_parameter_store_name: str = "evaluation_parameter_store",
    ):
        # Initialize base defaults
        super().__init__()

        # Use default credentials if seprate credentials not supplied for expectations_store and validations_store
        expectations_store_credentials = (
            expectations_store_credentials
            if expectations_store_credentials is not None
            else default_credentials
        )
        validations_store_credentials = (
            validations_store_credentials
            if validations_store_credentials is not None
            else default_credentials
        )

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


class InMemoryBackendEcosystem(BaseBackendEcosystem):
    pass
    # TODO: Are in-memory metadata stores supported? If so create this class.


class DataContextConfig(DictDot):
    def __init__(
        self,
        config_version: Optional[float] = None,
        datasources: Optional[
            Union[
                Dict[str, DatasourceConfig],
                # Dict[
                #     Union[
                #         DatasourceConfig, Dict[
                #             str, DatasourceConfig
                #         ]
                #     ]
                # ],
                Dict[str, Dict[str, Union[Dict[str, str], str, dict]]],
            ]
        ] = None,
        expectations_store_name: Optional[str] = None,
        validations_store_name: Optional[str] = None,
        evaluation_parameter_store_name: Optional[str] = None,
        plugins_directory: Optional[str] = None,
        validation_operators=DataContextConfigDefaults.DEFAULT_VALIDATION_OPERATORS.value,
        stores: Optional[Dict] = None,
        data_docs_sites: Optional[Dict] = None,
        notebooks=None,
        config_variables_file_path: Optional[str] = None,
        anonymous_usage_statistics=None,
        commented_map=None,
        backend_ecosystem: BaseBackendEcosystem = BaseBackendEcosystem(),
    ):

        # TODO: should default plugins_directory be "plugins/" or None?
        #  I think it may depend on in-memory or not. Is there checking
        #  for this directory when it is used and graceful handling if not found?

        # Set defaults via backend_ecosystem
        # What happens / should happen if a user specifies a backend_ecosystem
        #  and also items that would be specified inside of it e.g. expectations_store_name?
        # Override attributes from backend_ecosystem with any items passed into the constructor:
        config_version = (
            config_version
            if config_version is not None
            else backend_ecosystem.config_version
        )
        stores = stores if stores is not None else backend_ecosystem.stores
        expectations_store_name = (
            expectations_store_name
            if expectations_store_name is not None
            else backend_ecosystem.expectations_store_name
        )
        validations_store_name = (
            validations_store_name
            if validations_store_name is not None
            else backend_ecosystem.validations_store_name
        )
        evaluation_parameter_store_name = (
            evaluation_parameter_store_name
            if evaluation_parameter_store_name is not None
            else backend_ecosystem.evaluation_parameter_store_name
        )
        validation_operators = (
            validation_operators
            if validation_operators is not None
            else backend_ecosystem.validation_operators
        )
        data_docs_sites = (
            data_docs_sites
            if data_docs_sites is not None
            else backend_ecosystem.data_docs_sites
        )

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

    def to_yaml(self, outfile):
        commented_map = deepcopy(self.commented_map)
        commented_map.update(dataContextConfigSchema.dump(self))
        yaml.dump(commented_map, outfile)


dataContextConfigSchema = DataContextConfigSchema()
datasourceConfigSchema = DatasourceConfigSchema()
anonymizedUsageStatisticsSchema = AnonymizedUsageStatisticsConfigSchema()
notebookConfigSchema = NotebookConfigSchema()
