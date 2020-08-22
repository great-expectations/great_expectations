# import copy
# # import logging
# from functools import wraps
# from inspect import getfullargspec
# from typing import Callable, Union
#
# from marshmallow import ValidationError
# from ruamel.yaml import YAML
# from ruamel.yaml.comments import CommentedMap
#
# import great_expectations as ge
# import great_expectations.exceptions as ge_exceptions
# from great_expectations.data_context.store import (
#     ConfigurationStore,
#     Store,
#     StoreBackend,
# )
# TODO: <Alex>ALEX</Alex>
# from great_expectations.data_context.store.util import (
#     build_store_from_config,
#     build_tuple_filesystem_store_backend,
#     build_tuple_s3_store_backend,
#     build_configuration_store
# )
# from great_expectations.data_context.util import (
#     build_store_from_config,
#     # build_tuple_filesystem_store_backend,
#     # build_tuple_s3_store_backend,
#     # build_configuration_store
# )
# from great_expectations.data_context.templates import get_templated_yaml
# from great_expectations.data_context.types.base import (
#     DataContextConfig,
#     DataContextIdentificationConfig,
#     dataContextConfigSchema,
#     dataContextIdentificationConfigSchema,
# )
# from great_expectations.datasource import Datasource
# from great_expectations.util import filter_properties_dict
# from great_expectations.validation_operators import ValidationOperator

# yaml = YAML()
# yaml.indent(mapping=2, sequence=4, offset=2)
# yaml.default_flow_style = False

# GE_IDENTIFICATION_CONFIGURATION_STORE_NAME: str = "anon_data_context_id"
# GE_PROJECT_CONFIGURATION_STORE_NAME: str = "great_expectations"

# logger = logging.getLogger(__name__)


# # noinspection PyMethodParameters
# def create_using_s3_backend(func: Callable = None,) -> Callable:
#     """
#     A decorator for loading or creating data context with S3 serving as the backend store for all
#     application-level stores (Expectation Suites, Validations, Evaluation Parameters, and Data Docs).
#     """
#
#     @wraps(func)
#     def initialize_using_s3_backend_wrapped_method(**kwargs):
#         kwargs_callee: dict
#
#         func_callee: Callable = create_s3_backend_project_config
#         # noinspection SpellCheckingInspection
#         argspec: list = getfullargspec(func_callee)[0]
#         kwargs_callee = filter_properties_dict(
#             properties=kwargs, keep_fields=argspec, clean_empty=False, inplace=False
#         )
#         working_project_config_info: dict = func_callee(**kwargs_callee)
#
#         project_config_store = working_project_config_info["project_config_store"]
#         project_config_created: bool = working_project_config_info["project_config_created"]
#
#         if project_config_created:
#             kwargs_callee = copy.deepcopy(kwargs)
#             kwargs_callee.update({"project_config_store": project_config_store})
#             return func(**kwargs_callee)
#
#         return project_config_store.load_configuration()
#
#     return initialize_using_s3_backend_wrapped_method
#
#
# def create_s3_backend_project_config(
#     expectations_store_bucket: str,
#     expectations_store_prefix: str,
#     expectations_store_name: str,
#     expectations_store_kwargs: dict = None,
#     project_config_bucket: str = None,
#     project_config_prefix: str = None,
#     project_config_kwargs: dict = None,
#     # TODO: <Alex>ALEX</Alex>
#     # runtime_environment: Union[dict, None] = None,
#     overwrite_existing: bool = False,
#     usage_statistics_enabled: bool = True,
# ):
#     if expectations_store_kwargs is None:
#         expectations_store_kwargs = {}
#     ge_id_config_bucket: str = expectations_store_bucket
#     ge_id_config_prefix: str = expectations_store_prefix
#     ge_id_config_kwargs: dict = expectations_store_kwargs
#     if project_config_bucket is None:
#         project_config_bucket = expectations_store_bucket
#     if project_config_prefix is None:
#         project_config_prefix = expectations_store_prefix
#     if project_config_kwargs is None:
#         project_config_kwargs = expectations_store_kwargs
#
#     store_config: dict = {
#         "bucket": project_config_bucket,
#         "prefix": project_config_prefix,
#     }
#     store_config.update(**project_config_kwargs)
#     # TODO: <Alex>ALEX</Alex>
#     # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(**store_config)
#     s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
#     # TODO: <Alex>ALEX</Alex>
#     # project_config_store: ConfigurationStore = build_configuration_store(
#     project_config_store = build_configuration_store(
#         configuration_class=DataContextConfig,
#         store_name=GE_PROJECT_CONFIGURATION_STORE_NAME,
#         store_backend=s3_store_backend_obj,
#         overwrite_existing=True,
#     )
#
#     project_config: Union[DataContextConfig, None]
#
#     try:
#         # noinspection PyTypeChecker
#         project_config = project_config_store.load_configuration()
#     except ge_exceptions.ConfigNotFoundError:
#         project_config = None
#
#     found_existing_project_config: bool
#
#     if project_config is None:
#         found_existing_project_config = False
#         project_config = create_minimal_project_config(
#             usage_statistics_enabled=usage_statistics_enabled
#         )
#         # project_config_store.save_configuration(configuration=project_config)
#     else:
#         found_existing_project_config = True
#
#     project_config_created: bool
#     if overwrite_existing:
#         project_config = create_minimal_project_config(
#             usage_statistics_enabled=usage_statistics_enabled
#         )
#         # TODO: <Alex>ALEX</Alex>
#         # project_config_store.save_configuration(configuration=project_config)
#         project_config_created = True
#     else:
#         project_config_created = not found_existing_project_config
#
#     validate_project_config(project_config=project_config)
#
#     # TODO: <Alex>ALEX</Alex>
#     # working_data_context: Union[
#     #     ge.data_context.data_context.DataContext, None
#     # ] = ge.data_context.data_context.DataContext(
#     #     project_config_in_backend_store=True,
#     #     project_config_store=project_config_store,
#     #     project_config=project_config,
#     #     context_root_dir=None,
#     #     runtime_environment=runtime_environment,
#     #     usage_statistics_enabled=usage_statistics_enabled,
#     # )
#
#     # compute_and_persist_to_s3_data_context_id(
#     #     project_config=working_data_context.get_project_config(),
#     #     bucket=ge_id_config_bucket,
#     #     prefix=ge_id_config_prefix,
#     #     runtime_environment=None,
#     #     **ge_id_config_kwargs,
#     # )
#     # TODO: <Alex>ALEX</Alex>
#     compute_and_persist_to_s3_data_context_id(
#         project_config=project_config,
#         bucket=ge_id_config_bucket,
#         prefix=ge_id_config_prefix,
#         runtime_environment=None,
#         **ge_id_config_kwargs,
#     )
#
#     store_config: dict = {
#         "bucket": expectations_store_bucket,
#         "prefix": expectations_store_prefix,
#     }
#     store_config.update(**expectations_store_kwargs)
#     # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(**store_config)
#     # noinspection PyUnusedLocal
#     # expectations_store_obj: Store = working_data_context.add_expectations_store(
#     #     name=expectations_store_name, store_backend=s3_store_backend_obj
#     # )
#     # working_data_context.set_expectations_store_name(
#     #     expectations_store_name=expectations_store_name
#     # )
#     # TODO: <Alex>ALEX</Alex>
#     # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(**store_config)
#     s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
#     # noinspection PyUnusedLocal
#     # TODO: <Alex>ALEX</Alex>
#     # expectations_store_obj: Store = project_config.add_expectations_store(
#     expectations_store_obj = project_config.add_expectations_store(
#         name=expectations_store_name, store_backend=s3_store_backend_obj
#     )
#     # project_config.set_expectations_store_name(
#     #     expectations_store_name=expectations_store_name
#     # )
#     project_config.expectations_store_name = expectations_store_name
#     # TODO: <Alex>ALEX</Alex>
#
#     # return {
#     #     "data_context": working_data_context,
#     #     "data_context_created": data_context_created,
#     # }
#     # project_config_store.save_configuration(configuration=project_config)
#     # return {
#     #     "project_config": project_config,
#     #     "project_config_created": project_config_created,
#     # }
#
#     project_config_store.save_configuration(configuration=project_config)
#
#     return {
#         "project_config_store": project_config_store,
#         "project_config_created": project_config_created,
#     }
#
#
# # noinspection pyargumentlist
# @create_using_s3_backend
# def create_standard_s3_backend_project_config(**kwargs,):
#     func_callee: Callable = build_s3_backend_project_config
#     # noinspection SpellCheckingInspection
#     argspec: list = getfullargspec(func_callee)[0]
#     filter_properties_dict(
#         properties=kwargs, keep_fields=argspec, clean_empty=False, inplace=True
#     )
#     return func_callee(**kwargs)
#
#
# # noinspection SpellCheckingInspection
# def build_s3_backend_project_config(
#     datasource_type: str,
#     validations_store_bucket: str = None,
#     validations_store_prefix: str = None,
#     data_docs_store_bucket: str = None,
#     data_docs_store_prefix: str = None,
#     validations_store_name: str = None,
#     data_docs_site_name: str = "data_docs_site",
#     validations_store_kwargs: dict = None,
#     data_docs_store_kwargs: dict = None,
#     slack_webhook: str = None,
#     show_how_to_buttons: bool = True,
#     show_cta_footer: bool = True,
#     include_profiling: bool = True,
#     project_config_store=None,
# ):
#     if project_config_store is None:
#         raise ge_exceptions.DataContextError(
#             f"""The build_s3_backend_project_config method requires a valid project_config_store reference.
#             """
#         )
#     if validations_store_bucket is None:
#         validations_store_bucket = ""
#     if validations_store_prefix is None:
#         validations_store_prefix = ""
#     if validations_store_kwargs is None:
#         validations_store_kwargs = {}
#     if data_docs_store_bucket is None:
#         data_docs_store_bucket = ""
#     if data_docs_store_prefix is None:
#         data_docs_store_prefix = ""
#     if data_docs_store_kwargs is None:
#         data_docs_store_kwargs = {}
#
#     project_config = project_config_store.load_configuration()
#
#     # Add the Data Source (Spark Dataframe and Pandas Dataframe data sources types are currently implemented).
#     if datasource_type == "spark":
#         spark_df_datasource_name: str = "s3_files_spark_datasource"
#         # noinspection PyUnusedLocal
#         spark_df_datasource: Datasource = project_config.add_spark_df_datasource(
#             name=spark_df_datasource_name
#         )
#     elif datasource_type == "pandas":
#         pandas_datasource_name: str = "s3_files_pandas_datasource"
#         # noinspection PyUnusedLocal
#         pandas_datasource: Datasource = project_config.add_pandas_datasource(
#             name=pandas_datasource_name
#         )
#     else:
#         raise ge_exceptions.DataContextError(
#             f"""
# Only "spark" and "pandas" are currently supported as datasource types when "aws" is the specified backend ecosystem
# ("{datasource_type}" is not currently supported).
#             """
#         )
#
#     # Create the Validations Store:
#     # First, allocated the backend store (to be used for storing Validations in the JSON format);
#     if validations_store_bucket:
#         store_config: dict = {
#             "bucket": validations_store_bucket,
#             "prefix": validations_store_prefix,
#         }
#         store_config.update(**validations_store_kwargs)
#         # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(
#         # TODO: <Alex>ALEX</Alex>
#         s3_store_backend_obj = build_tuple_s3_store_backend(
#             **store_config
#         )
#         # Second, add the Validations Store that will use the AWS S3 backend store, allocated in the previous step.
#         # noinspection PyUnusedLocal
#         # validations_store_obj: Store = data_context.add_validation_store(
#         # TODO: <Alex>ALEX</Alex>
#         validations_store_obj = project_config.add_validation_store(
#             name=validations_store_name, store_backend=s3_store_backend_obj
#         )
#         # Third, set the name of the Validations Store added to be the one used by Great Expectations (required).
#         # data_context.set_validations_store_name(
#         #     validations_store_name=validations_store_name
#         # )
#         # TODO: <Alex>ALEX</Alex>
#         project_config.validations_store_name = validations_store_name
#
#     # Create the Evaluation Parameters Store (no arguments means an In-Memory backend store and the default name).
#     # noinspection PyUnusedLocal
#     # evaluation_parameters_store_obj: Store = data_context.add_evaluation_parameters_store()
#     # TODO: <Alex>ALEX</Alex>
#     evaluation_parameters_store_obj = project_config.add_evaluation_parameters_store()
#     # Set the default Evaluation Parameters Store just added to be the one used by Great Expectations (required).
#     project_config.set_evaluation_parameter_store_name()
#
#     # Add the Action List Operator for data validation (satisfies the needs of a wide variety of applications).
#     # noinspection PyUnusedLocal
#     action_list_validation_operator: ValidationOperator = project_config.add_action_list_validation_operator(
#         name="action_list_operator", slack_webhook=slack_webhook
#     )
#
#     if data_docs_store_bucket:
#         # Create the Data Docs Site:
#         # First, allocated the backend store (to be used for storing Data Docs in the HTML/CSS format);
#         store_config: dict = {
#             "bucket": data_docs_store_bucket,
#             "prefix": data_docs_store_prefix,
#         }
#         store_config.update(**data_docs_store_kwargs)
#         # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(
#         # TODO: <Alex>ALEX</Alex>
#         s3_store_backend_obj = build_tuple_s3_store_backend(
#             **store_config
#         )
#         # Second, add the Data Docs Site that will use the AWS S3 backend store, allocated in the previous step.
#         # noinspection PyUnusedLocal
#         data_docs_site_dict: dict = project_config.add_data_docs_site(
#             name=data_docs_site_name,
#             store_backend=s3_store_backend_obj,
#             show_how_to_buttons=show_how_to_buttons,
#             show_cta_footer=show_cta_footer,
#             include_profiling=include_profiling,
#         )
#
#     project_config_store.save_configuration(configuration=project_config)
#
#     return project_config
#
#
# def compute_and_persist_to_filesystem_data_context_id(
#     project_config: DataContextConfig,
#     base_directory: str,
#     runtime_environment: dict = None,
# ):
#     store_config: dict = {"base_directory": base_directory}
#     store_backend_obj = build_tuple_filesystem_store_backend(**store_config)
#     project_config.anonymous_usage_statistics.data_context_id = compute_and_persist_data_context_id(
#         store_backend=store_backend_obj,
#         project_config=project_config,
#         runtime_environment=runtime_environment,
#     )
#
#
# def compute_and_persist_to_s3_data_context_id(
#     project_config: DataContextConfig,
#     bucket: str = None,
#     prefix: str = None,
#     runtime_environment: dict = None,
#     **kwargs,
# ):
#     store_config: dict = {
#         "bucket": bucket,
#         "prefix": prefix,
#     }
#     store_config.update(**kwargs)
#     # s3_store_backend_obj: StoreBackend = build_tuple_s3_store_backend(**store_config)
#     # TODO: <Alex>ALEX</Alex>
#     s3_store_backend_obj = build_tuple_s3_store_backend(**store_config)
#     project_config.anonymous_usage_statistics.data_context_id = compute_and_persist_data_context_id(
#         store_backend=s3_store_backend_obj,
#         project_config=project_config,
#         runtime_environment=runtime_environment,
#     )
#
#
# def compute_and_persist_data_context_id(
#     # store_backend: StoreBackend,
#     # TODO: <Alex>ALEX</Alex>
#     store_backend,
#     project_config: Union[DataContextConfig, None] = None,
#     runtime_environment: dict = None,
# ) -> Union[str, None]:
#     if project_config is None:
#         return None
#
#     store_config: dict
#     # expectations_store_backend_obj: Union[StoreBackend, None] = None
#     # TODO: <Alex>ALEX</Alex>
#
#     expectations_store_exists: bool = False
#
#     data_context_id_from_expectations_store: Union[str, None] = None
#
#     data_context_id_from_id_store: Union[str, None] = find_data_context_id(
#         store_backend=store_backend, overwrite_existing=False,
#     )
#
#     if project_config.expectations_store_name is not None:
#         if (
#             project_config.stores.get(project_config.expectations_store_name)
#             is not None
#         ):
#             expectations_store_exists = True
#             store_config = project_config.stores[
#                 project_config.expectations_store_name
#             ]["store_backend"]
#             expectations_store_backend_obj = build_store_from_config(
#                 store_config=store_config, runtime_environment=runtime_environment
#             )
#             data_context_id_from_expectations_store = find_data_context_id(
#                 store_backend=expectations_store_backend_obj, overwrite_existing=False,
#             )
#
#     data_context_id: Union[str, None]
#
#     if expectations_store_exists:
#         if data_context_id_from_expectations_store is None:
#             if data_context_id_from_id_store is None:
#                 data_context_id = (
#                     project_config.anonymous_usage_statistics.data_context_id
#                 )
#             else:
#                 data_context_id = data_context_id_from_id_store
#             # noinspection PyUnusedLocal
#             data_context_id_stored_into_expectations_store_backend: str = find_or_create_data_context_id(
#                 store_backend=expectations_store_backend_obj,
#                 data_context_id=data_context_id,
#                 overwrite_existing=True,
#             )
#         else:
#             if data_context_id_from_id_store is None:
#                 data_context_id = data_context_id_from_expectations_store
#             else:
#                 data_context_id = data_context_id_from_id_store
#                 if not (data_context_id_from_expectations_store == data_context_id):
#                     # noinspection PyUnusedLocal
#                     data_context_id_stored_into_expectations_store_backend: str = find_or_create_data_context_id(
#                         store_backend=expectations_store_backend_obj,
#                         data_context_id=data_context_id,
#                         overwrite_existing=True,
#                     )
#         if not (
#             data_context_id_from_id_store is None
#             or store_backend.config == expectations_store_backend_obj.config
#         ):
#             delete_data_context_id(store_backend=store_backend,)
#     else:
#         if data_context_id_from_id_store is None:
#             data_context_id = project_config.anonymous_usage_statistics.data_context_id
#             # noinspection PyUnusedLocal
#             data_context_id_stored_into_store_backend: str = find_or_create_data_context_id(
#                 store_backend=store_backend,
#                 data_context_id=data_context_id,
#                 overwrite_existing=True,
#             )
#         else:
#             data_context_id = data_context_id_from_id_store
#
#     return data_context_id
#

# TODO: <Alex>ALEX</Alex>
# def find_or_create_data_context_id(
#     # store_backend: StoreBackend = None,
#     # TODO: <Alex>ALEX</Alex>
#     store_backend = None,
#     data_context_id: str = None,
#     overwrite_existing: bool = False,
# ) -> Union[str, None]:
#     if store_backend is None:
#         raise ge_exceptions.DataContextError(
#             f"""The find_or_create_data_context_id method requires a valid store_backend reference.
#             """
#         )
#
#     stored_data_context_id: Union[str, None] = find_data_context_id(
#         store_backend=store_backend, overwrite_existing=overwrite_existing,
#     )
#     if overwrite_existing or stored_data_context_id is None:
#         # TODO: <Alex>ALEX</Alex>
#         # ge_id_config_store: ConfigurationStore = build_configuration_store(
#         ge_id_config_store = build_configuration_store(
#             configuration_class=DataContextIdentificationConfig,
#             store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
#             store_backend=store_backend,
#             overwrite_existing=True,
#         )
#         ge_id_config = DataContextIdentificationConfig(data_context_id=data_context_id)
#         ge_id_config_store.save_configuration(configuration=ge_id_config)
#         stored_data_context_id = ge_id_config.data_context_id
#         validate_identification_config(identification_config=ge_id_config)
#
#     return stored_data_context_id
#

# def find_data_context_id(
#     # store_backend: StoreBackend = None, overwrite_existing: bool = False,
#     # TODO: <Alex>ALEX</Alex>
#     store_backend = None, overwrite_existing: bool = False,
# ) -> Union[str, None]:
#     if store_backend is None:
#         raise ge_exceptions.DataContextError(
#             f"""The find_data_context_id method requires a valid store_backend reference.
#               """
#         )
#     # ge_id_config_store: ConfigurationStore = build_configuration_store(
#     # TODO: <Alex>ALEX</Alex>
#     ge_id_config_store = build_configuration_store(
#         configuration_class=DataContextIdentificationConfig,
#         store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
#         store_backend=store_backend,
#         overwrite_existing=overwrite_existing,
#     )
#
#     ge_id_config: Union[DataContextIdentificationConfig, None]
#     try:
#         # noinspection PyTypeChecker
#         ge_id_config = ge_id_config_store.load_configuration()
#     except ge_exceptions.ConfigNotFoundError:
#         ge_id_config = None
#
#     if ge_id_config is None:
#         return None
#
#     validate_identification_config(identification_config=ge_id_config)
#
#     return ge_id_config.data_context_id


# # def delete_data_context_id(store_backend: StoreBackend = None,):
# # TODO: <Alex>ALEX</Alex>
# def delete_data_context_id(store_backend = None,):
#     if store_backend is None:
#         raise ge_exceptions.DataContextError(
#             f"""The delete_data_context_id method requires a valid store_backend reference.
#             """
#         )
#     # ge_id_config_store: ConfigurationStore = build_configuration_store(
#     # TODO: <Alex>ALEX</Alex>
#     ge_id_config_store = build_configuration_store(
#         configuration_class=DataContextIdentificationConfig,
#         store_name=GE_IDENTIFICATION_CONFIGURATION_STORE_NAME,
#         store_backend=store_backend,
#         overwrite_existing=True,
#     )
#     ge_id_config_store.delete_configuration()


# def create_minimal_data_context(
#     runtime_environment: Union[dict, None] = None,
#     usage_statistics_enabled: bool = True,
# ):
#     project_config: DataContextConfig = create_minimal_project_config(
#         usage_statistics_enabled=usage_statistics_enabled
#     )
#     return ge.data_context.data_context.DataContext(
#         project_config_in_backend_store=True,
#         project_config_store=None,
#         project_config=project_config,
#         context_root_dir=None,
#         runtime_environment=runtime_environment,
#         usage_statistics_enabled=usage_statistics_enabled,
#     )


# def create_minimal_project_config(
#     usage_statistics_enabled: bool = True,
# ) -> DataContextConfig:
#     project_yaml: str = get_templated_yaml(
#         j2_template_name="data_context_minimal_project_template.j2",
#         usage_statistics_enabled=usage_statistics_enabled,
#     )
#     project_config_dict_from_yaml: CommentedMap = yaml.load(project_yaml)
#     try:
#         # noinspection PyTypeChecker
#         return DataContextConfig.from_commented_map(project_config_dict_from_yaml)
#     except ge_exceptions.InvalidDataContextConfigError:
#         # Just to be explicit about what we intended to catch
#         raise


# TODO: <Alex>ALEX</Alex>
# def validate_identification_config(identification_config):
#     if isinstance(identification_config, DataContextIdentificationConfig):
#         return True
#     try:
#         dataContextIdentificationConfigSchema.load(identification_config)
#     except ValidationError:
#         raise
#     return True
#
#
# def validate_project_config(project_config):
#     if isinstance(project_config, DataContextConfig):
#         return True
#     try:
#         dataContextConfigSchema.load(project_config)
#     except ValidationError:
#         raise
#     return True


# TODO: <Alex>ALEX</Alex>
# def build_configuration_store(
#     store_name: str,
#     store_backend: StoreBackend,
#     *,
#     module_name: str = "great_expectations.data_context.store",
#     class_name: str = "ConfigurationStore",
#     **kwargs,
# ) -> ConfigurationStore:
#     logger.debug(
#         f"Starting data_context/store/util.py#build_configuration_store for store_name {store_name}"
#     )
#     if store_backend is not None:
#         store_backend = store_backend.config
#     store_config: dict = {
#         "store_name": store_name,
#         "module_name": module_name,
#         "class_name": class_name,
#         "store_backend": store_backend,
#     }
#     store_config.update(**kwargs)
#     # noinspection PyTypeChecker
#     configuration_store: ConfigurationStore = build_store_from_config(
#         store_config=store_config, module_name=module_name, runtime_environment=None,
#     )
#     return configuration_store

# TODO: <Alex>ALEX</Alex>
import logging
from typing import Union

# import great_expectations.exceptions as ge_exceptions
# from great_expectations.data_context.store import Store, StoreBackend, ConfigurationStore
from great_expectations.data_context.util import instantiate_class_from_config

# logger = logging.getLogger(__name__)

#
# TODO: <Alex>ALEX</Alex>
# def build_store_from_config(
#     store_config: dict = None,
#     module_name: str = "great_expectations.data_context.store",
#     runtime_environment: dict = None,
# ):
#     if store_config is None or module_name is None:
#         return None
#
#     try:
#         config_defaults: dict = {"module_name": module_name}
#         new_store = instantiate_class_from_config(
#             config=store_config,
#             runtime_environment=runtime_environment,
#             config_defaults=config_defaults,
#         )
#     except ge_exceptions.DataContextError as e:
#         new_store = None
#         logger.critical(f"Error {e} occurred while attempting to instantiate a store.")
#     if not new_store:
#         class_name: str = store_config.get("class_name")
#         raise ge_exceptions.ClassInstantiationError(
#             module_name=module_name, package_name=None, class_name=class_name,
#         )
#     return new_store


# def build_in_memory_store_backend(
#     module_name: str = "great_expectations.data_context.store",
#     class_name: str = "InMemoryStoreBackend",
#     **kwargs,
# ):
#     logger.debug(f"Starting data_context/store/util.py#build_in_memory_store_backend")
#     store_config: dict = {"module_name": module_name, "class_name": class_name}
#     store_config.update(**kwargs)
#     return build_store_from_config(
#         store_config=store_config, module_name=module_name, runtime_environment=None,
#     )
#
#
# def build_tuple_filesystem_store_backend(
#     base_directory: str,
#     *,
#     module_name: str = "great_expectations.data_context.store",
#     class_name: str = "TupleFilesystemStoreBackend",
#     **kwargs,
# ):
#     logger.debug(
#         f"Starting data_context/store/util.py#build_tuple_filesystem_store_backend"
#     )
#     store_config: dict = {
#         "module_name": module_name,
#         "class_name": class_name,
#         "base_directory": base_directory,
#     }
#     store_config.update(**kwargs)
#     return build_store_from_config(
#         store_config=store_config, module_name=module_name, runtime_environment=None,
#     )
#
#
# def build_tuple_s3_store_backend(
#     bucket: str,
#     *,
#     module_name: str = "great_expectations.data_context.store",
#     class_name: str = "TupleS3StoreBackend",
#     **kwargs,
# ):
#     logger.debug(f"Starting data_context/store/util.py#build_tuple_s3_store_backend")
#     store_config: dict = {
#         "module_name": module_name,
#         "class_name": class_name,
#         "bucket": bucket,
#     }
#     store_config.update(**kwargs)
#     return build_store_from_config(
#         store_config=store_config, module_name=module_name, runtime_environment=None,
#     )
#
#
# def build_configuration_store(
#     store_name: str,
#     # store_backend: StoreBackend,
#     # TODO: <Alex>ALEX</Alex>
#     store_backend,
#     *,
#     module_name: str = "great_expectations.data_context.store",
#     class_name: str = "ConfigurationStore",
#     **kwargs,
# ):
#     logger.debug(
#         f"Starting data_context/store/util.py#build_configuration_store for store_name {store_name}"
#     )
#     if store_backend is not None:
#         store_backend = store_backend.config
#     store_config: dict = {
#         "store_name": store_name,
#         "module_name": module_name,
#         "class_name": class_name,
#         "store_backend": store_backend,
#     }
#     store_config.update(**kwargs)
#     # noinspection PyTypeChecker
#     configuration_store = build_store_from_config(
#         store_config=store_config, module_name=module_name, runtime_environment=None,
#     )
#     return configuration_store
