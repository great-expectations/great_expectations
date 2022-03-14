import logging
from hashlib import md5
from typing import List, Optional, Tuple

from great_expectations.util import load_class

logger = logging.getLogger(__name__)


class Anonymizer:
    """Anonymize string names in an optionally-consistent way."""

    # Any class that starts with this __module__ is considered a "core" object
    CORE_GE_OBJECT_MODULE_PREFIX = "great_expectations"

    def __init__(self, salt: Optional[str] = None) -> None:
        if salt is not None and not isinstance(salt, str):
            logger.error("invalid salt: must provide a string. Setting a random salt.")
            salt = None
        if salt is None:
            import secrets

            self._salt = secrets.token_hex(8)
        else:
            self._salt = salt

    @property
    def salt(self) -> str:
        return self._salt

    def anonymize(self, string_: Optional[str]) -> Optional[str]:
        if string_ is None:
            return None

        if not isinstance(string_, str):
            raise TypeError(
                f"""The type of the "string_" argument must be a string (Python "str").  The type given is
"{str(type(string_))}", which is illegal.
            """
            )

        salted = self._salt + string_
        return md5(salted.encode("utf-8")).hexdigest()

    def anonymize_object_info(
        self,
        anonymized_info_dict: dict,
        object_: Optional[object] = None,
        object_class: Optional[type] = None,
        object_config: Optional[dict] = None,
        runtime_environment: Optional[dict] = None,
    ) -> dict:
        """Given an object, anonymize relevant fields and return result as a dictionary.

        Args:
            anonymized_info_dict: The payload object to hydrate with anonymized values.
            object_: The specific object to anonymize.
            object_class: The class of the specific object to anonymize.
            object_config: The dictionary configuration of the specific object to anonymize.
            runtime_environment: A dictionary containing relevant runtime information (like class_name and module_name)

        Returns:
            The anonymized_info_dict that's been populated with anonymized values.

        Raises:
            AssertionError if no object_, object_class, or object_config is provided.
        """
        assert (
            object_ or object_class or object_config
        ), "Must pass either object_ or object_class or object_config."

        if runtime_environment is None:
            runtime_environment = {}

        object_class_name: Optional[str] = None
        object_module_name: Optional[str] = None
        try:
            if object_class is None and object_ is not None:
                object_class = object_.__class__
            elif object_class is None and object_config is not None:
                object_class_name = object_config.get("class_name")
                object_module_name = object_config.get(
                    "module_name"
                ) or runtime_environment.get("module_name")
                object_class = load_class(object_class_name, object_module_name)

            object_class_name = object_class.__name__
            object_module_name = object_class.__module__
            parents: Tuple[type, ...] = object_class.__bases__

            if Anonymizer._is_core_great_expectations_class(object_module_name):
                anonymized_info_dict["parent_class"] = object_class_name
            else:

                # Chetan - 20220311 - If we can't identify the class in question, we iterate through the parents.
                # While GE rarely utilizes multiple inheritance when defining core objects (as of v0.14.10),
                # it is important to recognize that this is possibility.
                #
                # In the presence of multiple valid parents, we generate a comma-delimited list.

                parent_class_list: List[str] = []

                parent_class: type
                for parent_class in parents:
                    parent_module_name: str = parent_class.__module__
                    if Anonymizer._is_core_great_expectations_class(parent_module_name):
                        parent_class_list.append(parent_class.__name__)

                if parent_class_list:
                    concatenated_parent_classes: str = ",".join(
                        cls for cls in parent_class_list
                    )
                    anonymized_info_dict["parent_class"] = concatenated_parent_classes
                    anonymized_info_dict["anonymized_class"] = self.anonymize(
                        object_class_name
                    )

            # Catch-all to prevent edge cases from slipping past
            if not anonymized_info_dict.get("parent_class"):
                anonymized_info_dict["parent_class"] = "__not_recognized__"
                anonymized_info_dict["anonymized_class"] = self.anonymize(
                    object_class_name
                )

        except AttributeError:
            anonymized_info_dict["parent_class"] = "__not_recognized__"
            anonymized_info_dict["anonymized_class"] = self.anonymize(object_class_name)

        return anonymized_info_dict

    @staticmethod
    def _is_core_great_expectations_class(class_name: str) -> bool:
        return class_name.startswith(Anonymizer.CORE_GE_OBJECT_MODULE_PREFIX)

    @staticmethod
    def get_parent_class(
        classes_to_check: Optional[List[type]] = None,
        object_: Optional[object] = None,
        object_class: Optional[type] = None,
        object_config: Optional[dict] = None,
    ) -> Optional[str]:
        """Check if the parent class is a subclass of any core GE class.

        These anonymizers define and provide an optional list of core GE classes_to_check.
        If not provided, the object's inheritance hierarchy is traversed.

        Args:
            classes_to_check: An optinal list of candidate parent classes to iterate through.
            object_: The specific object to analyze.
            object_class: The class of the specific object to analyze.
            object_config: The dictionary configuration of the specific object to analyze.

        Returns:
            The name of the parent class found, or None if no parent class was found.

        Raises:
            AssertionError if no object_, object_class, or object_config is provided.
        """
        assert (
            object_ or object_class or object_config
        ), "Must pass either object_ or object_class or object_config."
        try:
            if object_class is None and object_ is not None:
                object_class = object_.__class__
            elif object_class is None and object_config is not None:
                object_class_name = object_config.get("class_name")
                object_module_name = object_config.get("module_name")
                object_class = load_class(object_class_name, object_module_name)

            object_class_name = object_class.__name__
            object_module_name = object_class.__module__

            # Utilize candidate list if provided.
            if classes_to_check:
                for class_to_check in classes_to_check:
                    if issubclass(object_class, class_to_check):
                        return class_to_check.__name__
                return None

            # Otherwise, iterate through parents in inheritance hierarchy.
            parents: Tuple[type, ...] = object_class.__bases__
            parent_class: type
            for parent_class in parents:
                parent_module_name: str = parent_class.__module__
                if Anonymizer._is_core_great_expectations_class(parent_module_name):
                    return parent_class.__name__

        except AttributeError:
            pass

        return None

    def anonymize_action_info(self, action_name, action_obj=None, action_config=None):
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(action_name),
        }

        self.anonymize_object_info(
            object_=action_obj,
            object_config=action_config,
            anonymized_info_dict=anonymized_info_dict,
            runtime_environment={"module_name": "great_expectations.checkpoint"},
        )

        return anonymized_info_dict

    def anonymize_validation_operator_info(
        self, validation_operator_name, validation_operator_obj
    ):
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(validation_operator_name)
        }
        actions_dict: dict = validation_operator_obj.actions

        anonymized_info_dict.update(
            self.anonymize_object_info(
                object_=validation_operator_obj,
                anonymized_info_dict=anonymized_info_dict,
            )
        )

        if actions_dict:
            anonymized_info_dict["anonymized_action_list"] = [
                self.anonymize_action_info(
                    action_name=action_name, action_obj=action_obj
                )
                for action_name, action_obj in actions_dict.items()
            ]

        return anonymized_info_dict

    def anonymize_batch_kwargs(self, batch_kwargs):
        ge_batch_kwarg_keys = [
            "datasource",
            "reader_method",
            "reader_options",
            "path",
            "s3",
            "dataset",
            "PandasInMemoryDF",
            "ge_batch_id",
            "query",
            "table",
            "SparkDFRef",
            "limit",
            "query_parameters",
            "offset",
            "snowflake_transient_table",
            "bigquery_temp_table",
            "data_asset_name",
        ]

        anonymized_batch_kwarg_keys = []
        for batch_kwarg_key in batch_kwargs.keys():
            if batch_kwarg_key in ge_batch_kwarg_keys:
                anonymized_batch_kwarg_keys.append(batch_kwarg_key)
            else:
                anonymized_batch_kwarg_keys.append(self.anonymize(batch_kwarg_key))

        return anonymized_batch_kwarg_keys

    def anonymize_batch_info(self, batch):
        from great_expectations.data_asset import DataAsset
        from great_expectations.validator.validator import Validator

        batch_kwargs = {}
        expectation_suite_name = ""
        datasource_name = ""
        if isinstance(batch, tuple):
            batch_kwargs = batch[0]
            expectation_suite_name = batch[1]
            datasource_name = batch_kwargs.get("datasource")
        if isinstance(batch, DataAsset):
            batch_kwargs = batch.batch_kwargs
            expectation_suite_name = batch.expectation_suite_name
            datasource_name = batch_kwargs.get("datasource")
        if isinstance(batch, Validator):
            expectation_suite_name = batch.expectation_suite_name
            datasource_name = batch.active_batch_definition.datasource_name

        anonymized_info_dict = {}

        if batch_kwargs:
            anonymized_info_dict[
                "anonymized_batch_kwarg_keys"
            ] = self.anonymize_batch_kwargs(batch_kwargs)
        else:
            anonymized_info_dict["anonymized_batch_kwarg_keys"] = []
        if expectation_suite_name:
            anonymized_info_dict["anonymized_expectation_suite_name"] = self.anonymize(
                expectation_suite_name
            )
        else:
            anonymized_info_dict["anonymized_expectation_suite_name"] = "__not_found__"
        if datasource_name:
            anonymized_info_dict["anonymized_datasource_name"] = self.anonymize(
                datasource_name
            )
        else:
            anonymized_info_dict["anonymized_datasource_name"] = "__not_found__"

        return anonymized_info_dict

    def anonymize_site_builder_info(self, site_builder_config):
        class_name = site_builder_config.get("class_name")
        module_name = site_builder_config.get("module_name")
        if module_name is None:
            module_name = "great_expectations.render.renderer.site_builder"

        anonymized_info_dict = {}
        self.anonymize_object_info(
            object_config={"class_name": class_name, "module_name": module_name},
            anonymized_info_dict=anonymized_info_dict,
        )

        return anonymized_info_dict

    def anonymize_store_backend_info(
        self, store_backend_obj=None, store_backend_object_config=None
    ):
        assert (
            store_backend_obj or store_backend_object_config
        ), "Must pass store_backend_obj or store_backend_object_config."
        anonymized_info_dict = {}
        if store_backend_obj is not None:
            self.anonymize_object_info(
                object_=store_backend_obj,
                anonymized_info_dict=anonymized_info_dict,
            )
        else:
            class_name = store_backend_object_config.get("class_name")
            module_name = store_backend_object_config.get("module_name")
            if module_name is None:
                module_name = "great_expectations.data_context.store"
            self.anonymize_object_info(
                object_config={"class_name": class_name, "module_name": module_name},
                anonymized_info_dict=anonymized_info_dict,
            )
        return anonymized_info_dict

    def anonymize_data_docs_site_info(self, site_name, site_config):
        site_config_module_name = site_config.get("module_name")
        if site_config_module_name is None:
            site_config[
                "module_name"
            ] = "great_expectations.render.renderer.site_builder"

        anonymized_info_dict = self.anonymize_site_builder_info(
            site_builder_config=site_config,
        )
        anonymized_info_dict["anonymized_name"] = self.anonymize(site_name)

        store_backend_config = site_config.get("store_backend")
        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self.anonymize_store_backend_info(
            store_backend_object_config=store_backend_config
        )
        site_index_builder_config = site_config.get("site_index_builder")
        anonymized_site_index_builder = self.anonymize_site_builder_info(
            site_builder_config=site_index_builder_config
        )
        # Note AJB-20201218 show_cta_footer was removed in v 0.9.9 via PR #1249
        if "show_cta_footer" in site_index_builder_config:
            anonymized_site_index_builder[
                "show_cta_footer"
            ] = site_index_builder_config.get("show_cta_footer")
        anonymized_info_dict[
            "anonymized_site_index_builder"
        ] = anonymized_site_index_builder

        return anonymized_info_dict

    def anonymize_store_info(self, store_name, store_obj):
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(store_name)
        store_backend_obj = store_obj.store_backend

        self.anonymize_object_info(
            object_=store_obj,
            anonymized_info_dict=anonymized_info_dict,
        )

        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self.anonymize_store_backend_info(store_backend_obj=store_backend_obj)

        return anonymized_info_dict

    def anonymize_profiler_info(self, name: str, config: dict) -> dict:
        anonymized_info_dict: dict = {
            "anonymized_name": self.anonymize(name),
        }
        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=config,
        )
        return anonymized_info_dict

    def anonymize_execution_engine_info(self, name, config):
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self.anonymize(name)

        from great_expectations.data_context.types.base import (
            ExecutionEngineConfig,
            executionEngineConfigSchema,
        )

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        execution_engine_config: ExecutionEngineConfig = (
            executionEngineConfigSchema.load(config)
        )
        execution_engine_config_dict: dict = executionEngineConfigSchema.dump(
            execution_engine_config
        )

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=execution_engine_config_dict,
        )

        return anonymized_info_dict

    def anonymize_data_connector_info(self, name, config):
        anonymized_info_dict = {
            "anonymized_name": self.anonymize(name),
        }

        # Roundtrip through schema validation to remove any illegal fields add/or restore any missing fields.
        from great_expectations.data_context.types.base import (
            DataConnectorConfig,
            dataConnectorConfigSchema,
        )

        data_connector_config: DataConnectorConfig = dataConnectorConfigSchema.load(
            config
        )
        data_connector_config_dict: dict = dataConnectorConfigSchema.dump(
            data_connector_config
        )

        self.anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=data_connector_config_dict,
        )

        return anonymized_info_dict
