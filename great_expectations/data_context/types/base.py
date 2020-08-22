import logging
import uuid
from collections import OrderedDict
from copy import deepcopy
from functools import wraps
from inspect import getfullargspec
from io import StringIO
from typing import Callable, Union

from marshmallow import (
    INCLUDE,
    Schema,
    ValidationError,
    fields,
    post_dump,
    post_load,
    validates_schema,
)
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.util import (
    build_configuration_store,
    build_store_from_config,
    build_tuple_filesystem_store_backend,
    build_tuple_s3_store_backend,
)
from great_expectations.data_context.templates import get_templated_yaml
from great_expectations.data_context.util import substitute_config_variable
from great_expectations.types import DictDot
from great_expectations.types.configurations import ClassConfigSchema
from great_expectations.util import (
    filter_properties_dict,
    get_currently_executing_function_call_arguments,
    load_class,
    verify_dynamic_loading_support,
)
from great_expectations.validation_operators import ValidationOperator

logger = logging.getLogger(__name__)

yaml = YAML()

yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


CURRENT_CONFIG_VERSION = 2
MINIMUM_SUPPORTED_CONFIG_VERSION = 2
DEFAULT_USAGE_STATISTICS_URL = (
    "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics"
)
DATA_CONTEXT_ID: str = str(uuid.uuid4())

GE_IDENTIFICATION_CONFIGURATION_STORE_NAME: str = "anon_data_context_id"
GE_PROJECT_CONFIGURATION_STORE_NAME: str = "great_expectations"

GE_EVALUATION_PARAMETER_STORE_NAME: str = "evaluation_parameter_store"


def validate_identification_config(identification_config):
    if isinstance(identification_config, DataContextIdentificationConfig):
        return True
    try:
        dataContextIdentificationConfigSchema.load(identification_config)
    except ValidationError:
        raise
    return True


def validate_project_config(project_config):
    if isinstance(project_config, DataContextConfig):
        return True
    try:
        dataContextConfigSchema.load(project_config)
    except ValidationError:
        raise
    return True


def substitute_all_config_variables(data, replace_variables_dict):
    """
    Substitute all config variables of the form ${SOME_VARIABLE} in a dictionary-like
    config object for their values.

    The method traverses the dictionary recursively.

    :param data:
    :param replace_variables_dict:
    :return: a dictionary with all the variables replaced with their values
    """
    if isinstance(data, DataContextConfig):
        data = DataContextConfigSchema().dump(data)

    if isinstance(data, dict) or isinstance(data, OrderedDict):
        return {
            k: substitute_all_config_variables(v, replace_variables_dict)
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [
            substitute_all_config_variables(v, replace_variables_dict) for v in data
        ]
    return substitute_config_variable(data, replace_variables_dict)


def object_to_yaml_str(obj):
    output_str: str
    with StringIO() as string_stream:
        yaml.dump(obj, string_stream)
        output_str = string_stream.getvalue()
    return output_str


class BaseConfig(DictDot):
    def __init__(
        self, commented_map: CommentedMap = None,
    ):
        if commented_map is None:
            commented_map = CommentedMap()
        self._commented_map = commented_map

    @classmethod
    def from_commented_map(cls, commented_map: CommentedMap):
        raise NotImplementedError

    @property
    def commented_map(self) -> CommentedMap:
        return self._commented_map

    def get_schema_validated_updated_commented_map(self) -> CommentedMap:
        raise NotImplementedError

    def to_yaml(self, outfile):
        yaml.dump(self.get_schema_validated_updated_commented_map(), outfile)

    def to_yaml_str(self) -> str:
        return object_to_yaml_str(self.get_schema_validated_updated_commented_map())

    def to_dict(self) -> dict:
        return dict(self.get_schema_validated_updated_commented_map())


class DataContextConfig(BaseConfig):
    def __init__(
        self,
        config_version,
        datasources=None,
        expectations_store_name=None,
        validations_store_name=None,
        evaluation_parameter_store_name=None,
        plugins_directory=None,
        validation_operators=None,
        stores=None,
        data_docs_sites=None,
        notebooks=None,
        config_variables_file_path=None,
        anonymous_usage_statistics=None,
        commented_map=None,
    ):
        self._config_version = config_version
        if datasources is None:
            datasources = {}
        self._datasources = datasources
        self._expectations_store_name = expectations_store_name
        self._validations_store_name = validations_store_name
        self._evaluation_parameter_store_name = evaluation_parameter_store_name
        self.plugins_directory = plugins_directory
        if validation_operators is None:
            validation_operators = {}
        elif not isinstance(validation_operators, dict):
            raise ValueError(
                "validation_operators must be configured with a dictionary"
            )
        self._validation_operators = validation_operators
        if stores is None:
            stores = {}
        self._stores = stores
        self.notebooks = notebooks
        if data_docs_sites is None:
            data_docs_sites = {}
        self._data_docs_sites = data_docs_sites
        self.config_variables_file_path = config_variables_file_path
        if anonymous_usage_statistics is None:
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig()
        elif isinstance(anonymous_usage_statistics, dict):
            anonymous_usage_statistics = AnonymizedUsageStatisticsConfig(
                **anonymous_usage_statistics
            )
        self._anonymous_usage_statistics = anonymous_usage_statistics

        super().__init__(commented_map=commented_map)

    @classmethod
    def from_commented_map(cls, commented_map: CommentedMap) -> BaseConfig:
        try:
            config: dict = dataContextConfigSchema.load(commented_map)
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

    # noinspection PyUnusedLocal
    # noinspection SpellCheckingInspection
    @classmethod
    def build(
        cls,
        backend_ecosystem: str,
        datasource_type: str,
        *,
        expectations_store_bucket: str = None,
        expectations_store_prefix: str = None,
        validations_store_bucket: str = None,
        validations_store_prefix: str = None,
        data_docs_store_bucket: str = None,
        data_docs_store_prefix: str = None,
        expectations_store_name: str = None,
        validations_store_name: str = None,
        data_docs_site_name: str = None,
        expectations_store_kwargs: dict = None,
        validations_store_kwargs: dict = None,
        data_docs_store_kwargs: dict = None,
        project_config_bucket: str = None,
        project_config_prefix: str = None,
        project_config_kwargs: dict = None,
        slack_webhook: str = None,
        show_how_to_buttons: bool = None,
        show_cta_footer: bool = None,
        include_profiling: bool = None,
        runtime_environment: dict = None,
        overwrite_existing: bool = None,
        usage_statistics_enabled: bool = None,
    ):
        kwargs_callee: dict

        if backend_ecosystem == "aws":
            func_callee: Callable = create_standard_s3_backend_project_config
            kwargs_callee = filter_properties_dict(
                properties=get_currently_executing_function_call_arguments(),
                delete_fields=["backend_ecosystem"],
                clean_empty=False,
                inplace=False,
            )
            return func_callee(**kwargs_callee)
        else:
            raise ge_exceptions.DataContextError(
                f"""
Only "aws" is currently supported as the backend ecosystem ("{backend_ecosystem}" is not currently supported).
                """
            )

    def add_store(
        self,
        store_name: str = None,
        store_config: dict = None,
        module_name="great_expectations.data_context.store",
        runtime_environment: dict = None,
    ):
        try:
            store_obj = build_store_from_config(
                store_config=store_config,
                module_name=module_name,
                runtime_environment=runtime_environment,
            )
            if store_name is not None:
                self.stores[store_name] = store_config
        except ge_exceptions.ClassInstantiationError as e:
            raise e

        if store_name is not None:
            self.stores[store_name] = store_config
        return store_obj

    def add_expectations_store(
        self,
        name: str,
        store_backend=None,
        *,
        module_name: str = "great_expectations.data_context.store",
        class_name: str = "ExpectationsStore",
        **kwargs,
    ):
        if store_backend is not None:
            store_backend = store_backend.config
        store_config: dict = {
            "module_name": module_name,
            "class_name": class_name,
            "store_backend": store_backend,
        }
        store_config.update(**kwargs)
        return self.add_store(store_name=name, store_config=store_config)

    def add_validation_store(
        self,
        name: str,
        store_backend,
        *,
        module_name: str = "great_expectations.data_context.store",
        class_name: str = "ValidationsStore",
        **kwargs,
    ):
        if store_backend is not None:
            store_backend = store_backend.config
        store_config: dict = {
            "module_name": module_name,
            "class_name": class_name,
            "store_backend": store_backend,
        }
        store_config.update(**kwargs)
        return self.add_store(store_name=name, store_config=store_config)

    def add_evaluation_parameters_store(
        self,
        name: str = GE_EVALUATION_PARAMETER_STORE_NAME,
        store_backend=None,
        *,
        module_name: str = "great_expectations.data_context.store.metric_store",
        class_name: str = "EvaluationParameterStore",
        **kwargs,
    ):
        if store_backend is not None:
            store_backend = store_backend.config
        store_config: dict = {
            "module_name": module_name,
            "class_name": class_name,
            "store_backend": store_backend,
        }
        store_config.update(**kwargs)
        return self.add_store(store_name=name, store_config=store_config)

    def add_data_docs_site(
        self,
        name: str,
        store_backend,
        *,
        show_how_to_buttons: bool = True,
        show_cta_footer: bool = True,
        **kwargs,
    ) -> Union[dict, None]:
        if name is None:
            return None

        logger.debug(
            f"Starting DataContextConfig.add_data_docs_site for data_docs_site {name}"
        )

        if store_backend is not None:
            store_backend = store_backend.config

        data_docs_site_config: dict = {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": show_how_to_buttons,
            "store_backend": store_backend,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": show_cta_footer,
            },
        }
        data_docs_site_config.update(**kwargs)

        self.data_docs_sites[name] = data_docs_site_config
        return self.data_docs_sites[name]

    def add_datasource(self, name, **kwargs) -> Union[dict, None]:
        logger.debug("Starting DataContextConfig.add_datasource for %s" % name)
        module_name = kwargs.get("module_name", "great_expectations.datasource")
        verify_dynamic_loading_support(module_name=module_name)
        class_name = kwargs.get("class_name")
        datasource_class = load_class(module_name=module_name, class_name=class_name)

        # For any class that should be loaded, it may control its configuration construction
        # by implementing a classmethod called build_configuration
        if hasattr(datasource_class, "build_configuration"):
            config = datasource_class.build_configuration(**kwargs)
        else:
            config = kwargs

        config = datasourceConfigSchema.load(config)
        self.datasources[name] = config
        return self.datasources[name]

    def add_pandas_datasource(
        self,
        name: str,
        *,
        module_name: str = "great_expectations.datasource",
        class_name: str = "PandasDatasource",
        **kwargs,
    ) -> Union[dict, None]:
        logger.debug(
            f"Starting DataContextConfig.add_pandas_datasource for datasource_name {name}"
        )
        datasource_config: dict = {
            "module_name": module_name,
            "class_name": class_name,
            "data_asset_type": {
                "class_name": "PandasDataset",
                "module_name": "great_expectations.dataset",
            },
        }
        datasource_config.update(**kwargs)
        return self.add_datasource(name=name, **datasource_config)

    def add_spark_df_datasource(
        self,
        name: str,
        *,
        module_name: str = "great_expectations.datasource",
        class_name: str = "SparkDFDatasource",
        **kwargs,
    ) -> Union[dict, None]:
        logger.debug(
            f"Starting DataContextConfig.add_spark_df_datasource for datasource_name {name}"
        )
        datasource_config: dict = {
            "module_name": module_name,
            "class_name": class_name,
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
        }
        datasource_config.update(**kwargs)
        return self.add_datasource(name=name, **datasource_config)

    def add_validation_operator(
        self, validation_operator_name: str, validation_operator_config: dict
    ) -> Union[dict, None]:
        self.validation_operators[validation_operator_name] = validation_operator_config
        return self.validation_operators[validation_operator_name]

    def add_action_list_validation_operator(
        self, name: str, slack_webhook: str = None, slack_notify_on: str = "all"
    ) -> dict:
        logger.debug(
            f"Starting DataContextConfig.add_action_list_validation_operator for validation_operator_name {name}"
        )
        action_list: list = [
            {
                "name": "store_validation_result",
                "action": {
                    "module_name": "great_expectations.validation_operators.actions",
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "module_name": "great_expectations.validation_operators.actions",
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "module_name": "great_expectations.validation_operators.actions",
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ]

        notify_slack_action_dict: dict = {
            "name": "notify_slack",
            "action": {
                "module_name": "great_expectations.validation_operators.actions",
                "class_name": "SlackNotificationAction",
                "slack_webhook": slack_webhook,
                "notify_on": slack_notify_on,
                "renderer": {
                    "module_name": "great_expectations.render.renderer.slack_renderer",
                    "class_name": "SlackRenderer",
                },
            },
        }

        if slack_webhook is not None:
            action_list.append(notify_slack_action_dict)

        validation_operator_config: dict = {
            "module_name": "great_expectations.validation_operators.validation_operators",
            "class_name": "ActionListValidationOperator",
            "action_list": action_list,
        }

        return self.add_validation_operator(
            validation_operator_name=name,
            validation_operator_config=validation_operator_config,
        )

    @property
    def config_version(self):
        return self._config_version

    @property
    def expectations_store_name(self) -> str:
        return self._expectations_store_name

    @expectations_store_name.setter
    def expectations_store_name(self, expectations_store_name: str):
        self._expectations_store_name = expectations_store_name

    @property
    def validations_store_name(self) -> str:
        return self._validations_store_name

    @validations_store_name.setter
    def validations_store_name(self, validations_store_name: str):
        self._validations_store_name = validations_store_name

    @property
    def evaluation_parameter_store_name(self) -> str:
        return self._evaluation_parameter_store_name

    @evaluation_parameter_store_name.setter
    def evaluation_parameter_store_name(self, evaluation_parameter_store_name: str):
        self._evaluation_parameter_store_name = evaluation_parameter_store_name

    def set_evaluation_parameter_store_name(
        self, evaluation_parameter_store_name: str = GE_EVALUATION_PARAMETER_STORE_NAME
    ):
        self.evaluation_parameter_store_name = evaluation_parameter_store_name

    @property
    def datasources(self) -> dict:
        return self._datasources

    @datasources.setter
    def datasources(self, datasources: dict):
        self._datasources = datasources

    @property
    def validation_operators(self) -> dict:
        return self._validation_operators

    @validation_operators.setter
    def validation_operators(self, validation_operators: dict):
        self._validation_operators = validation_operators

    @property
    def data_docs_sites(self) -> dict:
        return self._data_docs_sites

    @data_docs_sites.setter
    def data_docs_sites(self, data_docs_sites: dict):
        self._data_docs_sites = data_docs_sites

    @property
    def anonymous_usage_statistics(self):
        return self._anonymous_usage_statistics

    @anonymous_usage_statistics.setter
    def anonymous_usage_statistics(self, anonymous_usage_statistics):
        self._anonymous_usage_statistics = anonymous_usage_statistics

    @property
    def stores(self):
        return self._stores

    @stores.setter
    def stores(self, stores):
        self._stores = stores


class DatasourceConfig(DictDot):
    def __init__(
        self,
        class_name,
        module_name=None,
        data_asset_type=None,
        batch_kwargs_generators=None,
        credentials=None,
        reader_method=None,
        limit=None,
        **kwargs,
    ):
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
            data_context_id = DATA_CONTEXT_ID
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


class DataContextIdentificationConfig(BaseConfig):
    def __init__(
        self, data_context_id: str = None, commented_map: CommentedMap = None,
    ):
        if data_context_id is None:
            data_context_id = DATA_CONTEXT_ID
            self._explicit_id = False
        else:
            self._explicit_id = True

        self._data_context_id = data_context_id

        super().__init__(commented_map=commented_map)

    @property
    def data_context_id(self) -> str:
        return self._data_context_id

    @data_context_id.setter
    def data_context_id(self, data_context_id: str):
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

    @classmethod
    def from_commented_map(cls, commented_map: CommentedMap) -> BaseConfig:
        try:
            config: dict = dataContextIdentificationConfigSchema.load(commented_map)
            return cls(commented_map=commented_map, **config)
        except ValidationError:
            logger.error(
                "Encountered errors during loading data context config. See ValidationError for more details."
            )
            raise

    def get_schema_validated_updated_commented_map(self) -> CommentedMap:
        commented_map: CommentedMap = deepcopy(self.commented_map)
        commented_map.update(dataContextIdentificationConfigSchema.dump(self))
        return commented_map


class DataContextIdentificationConfigSchema(Schema):
    data_context_id = fields.UUID()

    @post_load()
    def make_data_context_identification_config(self, data, **kwargs) -> dict:
        if "data_context_id" in data:
            data["data_context_id"] = str(data["data_context_id"])
        return DataContextIdentificationConfig(**data).to_dict()


class DatasourceConfigSchema(Schema):
    class Meta:
        unknown = INCLUDE

    class_name = fields.String(required=True)
    module_name = fields.String(missing="great_expectations.datasource")
    data_asset_type = fields.Nested(ClassConfigSchema)
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
    expectations_store_name = fields.Str(allow_none=True)
    validations_store_name = fields.Str(allow_none=True)
    evaluation_parameter_store_name = fields.Str(allow_none=True)
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


dataContextConfigSchema = DataContextConfigSchema()
datasourceConfigSchema = DatasourceConfigSchema()
anonymizedUsageStatisticsSchema = AnonymizedUsageStatisticsConfigSchema()
dataContextIdentificationConfigSchema = DataContextIdentificationConfigSchema()
notebookConfigSchema = NotebookConfigSchema()


# noinspection PyMethodParameters
def create_using_s3_backend(func: Callable = None,) -> Callable:
    """
    A decorator for loading or creating data context with S3 serving as the backend store for all
    application-level stores (Expectation Suites, Validations, Evaluation Parameters, and Data Docs).
    """

    @wraps(func)
    def initialize_using_s3_backend_wrapped_method(**kwargs):
        kwargs_callee: dict

        func_callee: Callable = create_s3_backend_project_config
        # noinspection SpellCheckingInspection
        argspec: list = getfullargspec(func_callee)[0]
        kwargs_callee = filter_properties_dict(
            properties=kwargs, keep_fields=argspec, clean_empty=False, inplace=False
        )
        working_project_config_info: dict = func_callee(**kwargs_callee)

        project_config_store = working_project_config_info["project_config_store"]
        new_project_config_created: bool = working_project_config_info[
            "new_project_config_created"
        ]

        if new_project_config_created:
            kwargs_callee = deepcopy(kwargs)
            kwargs_callee.update({"project_config_store": project_config_store})
            return func(**kwargs_callee)

        return project_config_store.load_configuration()

    return initialize_using_s3_backend_wrapped_method


def create_s3_backend_project_config(
    expectations_store_bucket: str,
    expectations_store_prefix: str,
    expectations_store_name: str,
    expectations_store_kwargs: dict = None,
    project_config_bucket: str = None,
    project_config_prefix: str = None,
    project_config_kwargs: dict = None,
    overwrite_existing: bool = False,
    usage_statistics_enabled: bool = True,
):
    if expectations_store_kwargs is None:
        expectations_store_kwargs = {}
    ge_id_config_bucket: str = expectations_store_bucket
    ge_id_config_prefix: str = expectations_store_prefix
    ge_id_config_kwargs: dict = expectations_store_kwargs
    if project_config_bucket is None:
        project_config_bucket = expectations_store_bucket
    if project_config_prefix is None:
        project_config_prefix = expectations_store_prefix
    if project_config_kwargs is None:
        project_config_kwargs = expectations_store_kwargs

    store_config: dict = {
        "bucket": project_config_bucket,
        "prefix": project_config_prefix,
    }
    store_config.update(**project_config_kwargs)
    s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
    project_config_store = build_configuration_store(
        configuration_class=DataContextConfig,
        store_name=GE_PROJECT_CONFIGURATION_STORE_NAME,
        store_backend=s3_store_backend_obj,
        overwrite_existing=True,
    )

    project_config: Union[DataContextConfig, None]

    try:
        # noinspection PyTypeChecker
        project_config = project_config_store.load_configuration()
    except ge_exceptions.ConfigNotFoundError:
        project_config = None

    new_project_config_created: bool = False

    if overwrite_existing or project_config is None:
        project_config = create_minimal_project_config(
            usage_statistics_enabled=usage_statistics_enabled
        )
        new_project_config_created = True

    validate_project_config(project_config=project_config)

    compute_and_persist_to_s3_data_context_id(
        project_config=project_config,
        bucket=ge_id_config_bucket,
        prefix=ge_id_config_prefix,
        runtime_environment=None,
        **ge_id_config_kwargs,
    )

    store_config: dict = {
        "bucket": expectations_store_bucket,
        "prefix": expectations_store_prefix,
    }
    store_config.update(**expectations_store_kwargs)
    s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
    # noinspection PyUnusedLocal
    expectations_store_obj = project_config.add_expectations_store(
        name=expectations_store_name, store_backend=s3_store_backend_obj
    )
    project_config.expectations_store_name = expectations_store_name

    project_config_store.save_configuration(configuration=project_config)

    return {
        "project_config_store": project_config_store,
        "new_project_config_created": new_project_config_created,
    }


# noinspection pyargumentlist
@create_using_s3_backend
def create_standard_s3_backend_project_config(**kwargs,):
    func_callee: Callable = build_s3_backend_project_config
    # noinspection SpellCheckingInspection
    argspec: list = getfullargspec(func_callee)[0]
    filter_properties_dict(
        properties=kwargs, keep_fields=argspec, clean_empty=False, inplace=True
    )
    return func_callee(**kwargs)


# noinspection SpellCheckingInspection
def build_s3_backend_project_config(
    datasource_type: str,
    validations_store_bucket: str = None,
    validations_store_prefix: str = None,
    data_docs_store_bucket: str = None,
    data_docs_store_prefix: str = None,
    validations_store_name: str = None,
    data_docs_site_name: str = "data_docs_site",
    validations_store_kwargs: dict = None,
    data_docs_store_kwargs: dict = None,
    slack_webhook: str = None,
    show_how_to_buttons: bool = True,
    show_cta_footer: bool = True,
    include_profiling: bool = True,
    project_config_store=None,
):
    if project_config_store is None:
        raise ge_exceptions.DataContextError(
            f"""The build_s3_backend_project_config method requires a valid project_config_store reference.
            """
        )
    if validations_store_bucket is None:
        validations_store_bucket = ""
    if validations_store_prefix is None:
        validations_store_prefix = ""
    if validations_store_kwargs is None:
        validations_store_kwargs = {}
    if data_docs_store_bucket is None:
        data_docs_store_bucket = ""
    if data_docs_store_prefix is None:
        data_docs_store_prefix = ""
    if data_docs_store_kwargs is None:
        data_docs_store_kwargs = {}

    project_config = project_config_store.load_configuration()

    # Add the Data Source (Spark Dataframe and Pandas Dataframe data sources types are currently implemented).
    if datasource_type == "spark":
        spark_df_datasource_name: str = "s3_files_spark_datasource"
        # noinspection PyUnusedLocal
        spark_df_datasource: Datasource = project_config.add_spark_df_datasource(
            name=spark_df_datasource_name
        )
    elif datasource_type == "pandas":
        pandas_datasource_name: str = "s3_files_pandas_datasource"
        # noinspection PyUnusedLocal
        pandas_datasource: Datasource = project_config.add_pandas_datasource(
            name=pandas_datasource_name
        )
    else:
        raise ge_exceptions.DataContextError(
            f"""
Only "spark" and "pandas" are currently supported as datasource types when "aws" is the specified backend ecosystem
("{datasource_type}" is not currently supported).
            """
        )

    # Create the Validations Store:
    # First, allocated the backend store (to be used for storing Validations in the JSON format);
    if validations_store_bucket:
        store_config: dict = {
            "bucket": validations_store_bucket,
            "prefix": validations_store_prefix,
        }
        store_config.update(**validations_store_kwargs)
        s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
        # Second, add the Validations Store that will use the AWS S3 backend store, allocated in the previous step.
        # noinspection PyUnusedLocal
        validations_store_obj = project_config.add_validation_store(
            name=validations_store_name, store_backend=s3_store_backend_obj
        )
        # Third, set the name of the Validations Store added to be the one used by Great Expectations (required).
        project_config.validations_store_name = validations_store_name

    # Create the Evaluation Parameters Store (no arguments means an In-Memory backend store and the default name).
    # noinspection PyUnusedLocal
    evaluation_parameters_store_obj = project_config.add_evaluation_parameters_store()
    # Set the default Evaluation Parameters Store just added to be the one used by Great Expectations (required).
    project_config.set_evaluation_parameter_store_name()

    # Add the Action List Operator for data validation (satisfies the needs of a wide variety of applications).
    # noinspection PyUnusedLocal
    action_list_validation_operator: ValidationOperator = project_config.add_action_list_validation_operator(
        name="action_list_operator", slack_webhook=slack_webhook
    )

    if data_docs_store_bucket:
        # Create the Data Docs Site:
        # First, allocated the backend store (to be used for storing Data Docs in the HTML/CSS format);
        store_config: dict = {
            "bucket": data_docs_store_bucket,
            "prefix": data_docs_store_prefix,
        }
        store_config.update(**data_docs_store_kwargs)
        s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
        # Second, add the Data Docs Site that will use the AWS S3 backend store, allocated in the previous step.
        # noinspection PyUnusedLocal
        data_docs_site_dict: dict = project_config.add_data_docs_site(
            name=data_docs_site_name,
            store_backend=s3_store_backend_obj,
            show_how_to_buttons=show_how_to_buttons,
            show_cta_footer=show_cta_footer,
            include_profiling=include_profiling,
        )

    project_config_store.save_configuration(configuration=project_config)

    return project_config


def compute_and_persist_to_filesystem_data_context_id(
    project_config: DataContextConfig,
    base_directory: str,
    runtime_environment: dict = None,
):
    store_config: dict = {"base_directory": base_directory}
    store_backend_obj = build_tuple_filesystem_store_backend(**store_config)
    project_config.anonymous_usage_statistics.data_context_id = compute_and_persist_data_context_id(
        store_backend=store_backend_obj,
        project_config=project_config,
        runtime_environment=runtime_environment,
    )


def compute_and_persist_to_s3_data_context_id(
    project_config: DataContextConfig,
    bucket: str = None,
    prefix: str = None,
    runtime_environment: dict = None,
    **kwargs,
):
    store_config: dict = {
        "bucket": bucket,
        "prefix": prefix,
    }
    store_config.update(**kwargs)
    s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
    project_config.anonymous_usage_statistics.data_context_id = compute_and_persist_data_context_id(
        store_backend=s3_store_backend_obj,
        project_config=project_config,
        runtime_environment=runtime_environment,
    )


def compute_and_persist_data_context_id(
    store_backend,
    project_config: Union[DataContextConfig, None] = None,
    runtime_environment: dict = None,
) -> Union[str, None]:
    if project_config is None:
        return None

    store_config: dict
    expectations_store_backend_obj = None

    expectations_store_exists: bool = False

    data_context_id_from_expectations_store: Union[str, None] = None

    data_context_id_from_id_store: Union[str, None] = find_data_context_id(
        store_backend=store_backend, overwrite_existing=False,
    )

    if project_config.expectations_store_name is not None:
        if (
            project_config.stores.get(project_config.expectations_store_name)
            is not None
        ):
            expectations_store_exists = True
            store_config = project_config.stores[
                project_config.expectations_store_name
            ]["store_backend"]
            expectations_store_backend_obj = build_store_from_config(
                store_config=store_config, runtime_environment=runtime_environment
            )
            data_context_id_from_expectations_store = find_data_context_id(
                store_backend=expectations_store_backend_obj, overwrite_existing=False,
            )

    data_context_id: Union[str, None]

    if expectations_store_exists:
        if data_context_id_from_expectations_store is None:
            if data_context_id_from_id_store is None:
                data_context_id = (
                    project_config.anonymous_usage_statistics.data_context_id
                )
            else:
                data_context_id = data_context_id_from_id_store
            # noinspection PyUnusedLocal
            data_context_id_stored_into_expectations_store_backend: str = find_or_create_data_context_id(
                store_backend=expectations_store_backend_obj,
                data_context_id=data_context_id,
                overwrite_existing=True,
            )
        else:
            if data_context_id_from_id_store is None:
                data_context_id = data_context_id_from_expectations_store
            else:
                data_context_id = data_context_id_from_id_store
                if not (data_context_id_from_expectations_store == data_context_id):
                    # noinspection PyUnusedLocal
                    data_context_id_stored_into_expectations_store_backend: str = find_or_create_data_context_id(
                        store_backend=expectations_store_backend_obj,
                        data_context_id=data_context_id,
                        overwrite_existing=True,
                    )
        if not (
            data_context_id_from_id_store is None
            or store_backend.config == expectations_store_backend_obj.config
        ):
            delete_data_context_id(store_backend=store_backend,)
    else:
        if data_context_id_from_id_store is None:
            data_context_id = project_config.anonymous_usage_statistics.data_context_id
            # noinspection PyUnusedLocal
            data_context_id_stored_into_store_backend: str = find_or_create_data_context_id(
                store_backend=store_backend,
                data_context_id=data_context_id,
                overwrite_existing=True,
            )
        else:
            data_context_id = data_context_id_from_id_store

    return data_context_id


def find_or_create_data_context_id(
    store_backend=None, data_context_id: str = None, overwrite_existing: bool = False,
) -> Union[str, None]:
    if store_backend is None:
        raise ge_exceptions.DataContextError(
            f"""The find_or_create_data_context_id method requires a valid store_backend reference.
            """
        )

    stored_data_context_id: Union[str, None] = find_data_context_id(
        store_backend=store_backend, overwrite_existing=overwrite_existing,
    )
    if overwrite_existing or stored_data_context_id is None:
        ge_id_config_store = build_configuration_store(
            configuration_class=DataContextIdentificationConfig,
            store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
            store_backend=store_backend,
            overwrite_existing=True,
        )
        ge_id_config = DataContextIdentificationConfig(data_context_id=data_context_id)
        ge_id_config_store.save_configuration(configuration=ge_id_config)
        stored_data_context_id = ge_id_config.data_context_id
        validate_identification_config(identification_config=ge_id_config)

    return stored_data_context_id


def find_data_context_id(
    store_backend=None, overwrite_existing: bool = False,
) -> Union[str, None]:
    if store_backend is None:
        raise ge_exceptions.DataContextError(
            f"""The find_data_context_id method requires a valid store_backend reference.
              """
        )
    ge_id_config_store = build_configuration_store(
        configuration_class=DataContextIdentificationConfig,
        store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
        store_backend=store_backend,
        overwrite_existing=overwrite_existing,
    )

    ge_id_config: Union[DataContextIdentificationConfig, None]
    try:
        # noinspection PyTypeChecker
        ge_id_config = ge_id_config_store.load_configuration()
    except ge_exceptions.ConfigNotFoundError:
        ge_id_config = None

    if ge_id_config is None:
        return None

    validate_identification_config(identification_config=ge_id_config)

    return ge_id_config.data_context_id


def delete_data_context_id(store_backend=None):
    if store_backend is None:
        raise ge_exceptions.DataContextError(
            f"""The delete_data_context_id method requires a valid store_backend reference.
            """
        )
    ge_id_config_store = build_configuration_store(
        configuration_class=DataContextIdentificationConfig,
        store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
        store_backend=store_backend,
        overwrite_existing=True,
    )
    ge_id_config_store.delete_configuration()


def create_minimal_project_config(
    usage_statistics_enabled: bool = True,
) -> DataContextConfig:
    project_yaml: str = get_templated_yaml(
        j2_template_name="data_context_minimal_project_template.j2",
        usage_statistics_enabled=usage_statistics_enabled,
    )
    project_config_dict_from_yaml: CommentedMap = yaml.load(project_yaml)
    try:
        # noinspection PyTypeChecker
        return DataContextConfig.from_commented_map(project_config_dict_from_yaml)
    except ge_exceptions.InvalidDataContextConfigError:
        # Just to be explicit about what we intended to catch
        raise
