import copy
import logging
from abc import ABC, abstractmethod, abstractstaticmethod
from hashlib import md5
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from great_expectations.core.usage_statistics.util import (
    aggregate_all_core_expectation_types,
)
from great_expectations.util import deep_filter_properties_iterable, load_class

from great_expectations.core.usage_statistics.anonymizers.types.base import (  # isort:skip
    GETTING_STARTED_DATASOURCE_NAME,
    GETTING_STARTED_EXPECTATION_SUITE_NAME,
    GETTING_STARTED_CHECKPOINT_NAME,
    BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS,
    BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS,
    DATA_CONNECTOR_QUERY_KEYS,
    RUNTIME_PARAMETERS_KEYS,
    BATCH_SPEC_PASSTHROUGH_KEYS,
    BATCH_REQUEST_FLATTENED_KEYS,
)


logger = logging.getLogger(__name__)


class BaseAnonymizer(ABC):

    # Any class that starts with this __module__ is considered a "core" object
    CORE_GE_OBJECT_MODULE_PREFIX = "great_expectations"

    CORE_GE_EXPECTATION_TYPES = aggregate_all_core_expectation_types()

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

    @abstractmethod
    def anonymize(self, obj: object, **kwargs) -> dict:
        raise NotImplementedError

    @abstractstaticmethod
    def can_handle(obj: object, **kwargs) -> bool:
        raise NotImplementedError

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
                if BaseAnonymizer._is_core_great_expectations_class(parent_module_name):
                    return parent_class.__name__

        except AttributeError:
            pass

        return None

    def _anonymize_string(self, string_: Optional[str]) -> Optional[str]:
        """Obsfuscates a given string using an MD5 hash.

        Utilized to anonymize user-specific/sensitive strings in usage statistics payloads.

        Args:
            string_ (Optional[str]): The input string to anonymize.

        Returns:
            The MD5 hash of the input string. If an input of None is provided, the string is untouched.

        Raises:
            TypeError if input string does not adhere to type signature Optional[str].
        """
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

    def _anonymize_object_info(
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

            if self._is_core_great_expectations_class(object_module_name):
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
                    if BaseAnonymizer._is_core_great_expectations_class(
                        parent_module_name
                    ):
                        parent_class_list.append(parent_class.__name__)

                if parent_class_list:
                    concatenated_parent_classes: str = ",".join(
                        cls for cls in parent_class_list
                    )
                    anonymized_info_dict["parent_class"] = concatenated_parent_classes
                    anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                        object_class_name
                    )

            # Catch-all to prevent edge cases from slipping past
            if not anonymized_info_dict.get("parent_class"):
                anonymized_info_dict["parent_class"] = "__not_recognized__"
                anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                    object_class_name
                )

        except AttributeError:
            anonymized_info_dict["parent_class"] = "__not_recognized__"
            anonymized_info_dict["anonymized_class"] = self._anonymize_string(
                object_class_name
            )

        return anonymized_info_dict

    @staticmethod
    def _is_core_great_expectations_class(class_name: str) -> bool:
        return class_name.startswith(BaseAnonymizer.CORE_GE_OBJECT_MODULE_PREFIX)

    def _anonymize_validation_operator_info(
        self,
        validation_operator_name: str,
        validation_operator_obj: object,
    ) -> dict:
        """Anonymize ValidationOperator objs from the 'great_expectations.validation_operators' module.

        Args:
            validation_operator_name (str): The name of the operator.
            validation_operator_obj (object): An instance of the operator base class or one of its children.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict: dict = {
            "anonymized_name": self._anonymize_string(validation_operator_name)
        }
        actions_dict: dict = validation_operator_obj.actions

        anonymized_info_dict.update(
            self._anonymize_object_info(
                object_=validation_operator_obj,
                anonymized_info_dict=anonymized_info_dict,
            )
        )

        if actions_dict:
            anonymized_info_dict["anonymized_action_list"] = [
                self._anonymize_action_info(
                    action_name=action_name, action_obj=action_obj
                )
                for action_name, action_obj in actions_dict.items()
            ]

        return anonymized_info_dict

    def _anonymize_batch_kwargs(self, batch_kwargs: dict) -> List[str]:
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
                anonymized_batch_kwarg_keys.append(
                    self._anonymize_string(batch_kwarg_key)
                )

        return anonymized_batch_kwarg_keys

    def _anonymize_batch_info(
        self, batch: Union[Tuple[dict, str], "DataAsset", "Validator"]  # noqa: F821
    ) -> dict:
        """Anonymize Batch objs - can be derived from a variety of types.

        Args:
            batch (Union[Tuple[dict, str], "DataAsset", "Validator"]): The object used to
            obtain relevant details such as 'batch_kwargs' and 'expectation_suite_name'.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
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
            ] = self._anonymize_batch_kwargs(batch_kwargs)
        else:
            anonymized_info_dict["anonymized_batch_kwarg_keys"] = []
        if expectation_suite_name:
            anonymized_info_dict[
                "anonymized_expectation_suite_name"
            ] = self._anonymize_string(expectation_suite_name)
        else:
            anonymized_info_dict["anonymized_expectation_suite_name"] = "__not_found__"
        if datasource_name:
            anonymized_info_dict["anonymized_datasource_name"] = self._anonymize_string(
                datasource_name
            )
        else:
            anonymized_info_dict["anonymized_datasource_name"] = "__not_found__"

        return anonymized_info_dict

    def _anonymize_site_builder_info(self, site_builder_config: dict) -> dict:
        class_name = site_builder_config.get("class_name")
        module_name = site_builder_config.get("module_name")
        if module_name is None:
            module_name = "great_expectations.render.renderer.site_builder"

        anonymized_info_dict = {}
        self._anonymize_object_info(
            object_config={"class_name": class_name, "module_name": module_name},
            anonymized_info_dict=anonymized_info_dict,
        )

        return anonymized_info_dict

    def _anonymize_store_backend_info(
        self,
        store_backend_obj: Optional[object] = None,
        store_backend_object_config: Optional[dict] = None,
    ) -> dict:
        assert (
            store_backend_obj or store_backend_object_config
        ), "Must pass store_backend_obj or store_backend_object_config."
        anonymized_info_dict = {}
        if store_backend_obj is not None:
            self._anonymize_object_info(
                object_=store_backend_obj,
                anonymized_info_dict=anonymized_info_dict,
            )
        else:
            class_name = store_backend_object_config.get("class_name")
            module_name = store_backend_object_config.get("module_name")
            if module_name is None:
                module_name = "great_expectations.data_context.store"
            self._anonymize_object_info(
                object_config={"class_name": class_name, "module_name": module_name},
                anonymized_info_dict=anonymized_info_dict,
            )
        return anonymized_info_dict

    def _anonymize_data_docs_site_info(self, site_name: str, site_config: dict) -> dict:
        """Anonymize details around a DataDocs depolyment.

        Args:
            site_name (str): The name of the DataDocs site.
            site_config (dict): The dictionary configuration for the site.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        site_config_module_name = site_config.get("module_name")
        if site_config_module_name is None:
            site_config[
                "module_name"
            ] = "great_expectations.render.renderer.site_builder"

        anonymized_info_dict = self._anonymize_site_builder_info(
            site_builder_config=site_config,
        )
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(site_name)

        store_backend_config = site_config.get("store_backend")
        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self._anonymize_store_backend_info(
            store_backend_object_config=store_backend_config
        )
        site_index_builder_config = site_config.get("site_index_builder")
        anonymized_site_index_builder = self._anonymize_site_builder_info(
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

    def _anonymize_store_info(self, store_name: str, store_obj: object) -> dict:
        """Anonymize Store objs from the 'great_expectations.data_context.store' module.

        Args:
            store_name (str): The name of the store.
            store_obj (object): An instance of the Store base class or one of its children.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict = {}
        anonymized_info_dict["anonymized_name"] = self._anonymize_string(store_name)
        store_backend_obj = store_obj.store_backend

        self._anonymize_object_info(
            object_=store_obj,
            anonymized_info_dict=anonymized_info_dict,
        )

        anonymized_info_dict[
            "anonymized_store_backend"
        ] = self._anonymize_store_backend_info(store_backend_obj=store_backend_obj)

        return anonymized_info_dict

    def _anonymize_data_connector_info(self, name: str, config: dict) -> dict:
        """Anonymize DataConnector objs from the 'great_expectations.datasource.data_connector' module.

        Args:
            name (str): The name of the connector.
            config (dict): The dictionary configuration corresponding to the connector.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict = {
            "anonymized_name": self._anonymize_string(name),
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

        self._anonymize_object_info(
            anonymized_info_dict=anonymized_info_dict,
            object_config=data_connector_config_dict,
        )

        return anonymized_info_dict

    def _anonymize_expectation_suite_info(
        self, expectation_suite: "ExpectationSuite"  # noqa: F821
    ) -> dict:
        """Anonymize ExpectationSuite objs from the 'great_expectations.core' module.

        Args:
            expectation_suite ("ExpectationSuite"): An instance of ExpectationSuite.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_info_dict = {}
        anonymized_expectation_counts = list()

        expectations = expectation_suite.expectations
        expectation_types = [
            expectation.expectation_type for expectation in expectations
        ]
        for expectation_type in set(expectation_types):
            expectation_info = {"count": expectation_types.count(expectation_type)}
            self._anonymize_expectation(expectation_type, expectation_info)
            anonymized_expectation_counts.append(expectation_info)

        anonymized_info_dict["anonymized_name"] = self._anonymize_string(
            expectation_suite.expectation_suite_name
        )
        anonymized_info_dict["expectation_count"] = len(expectations)
        anonymized_info_dict[
            "anonymized_expectation_counts"
        ] = anonymized_expectation_counts

        return anonymized_info_dict

    def _anonymize_expectation(
        self, expectation_type: Optional[str], info_dict: dict
    ) -> None:
        """Anonymize Expectation objs from 'great_expectations.expectations'.

        Args:
            expectation_type (Optional[str]): The string name of the Expectation.
            info_dict (dict): A dictionary to update within this function.
        """
        if expectation_type in self.CORE_GE_EXPECTATION_TYPES:
            info_dict["expectation_type"] = expectation_type
        else:
            info_dict["anonymized_expectation_type"] = self._anonymize_string(
                expectation_type
            )

    def _anonymize_batch_request(
        self, *args, **kwargs
    ) -> Optional[Dict[str, List[str]]]:
        """Construct a BatchRequest from given args and anonymize user-specificy details.

            *args: Used to instantiate a BatchRequest.
            **kwargs: Used to instantiate a BatchRequest.

        Returns:
            An anonymized dictionary payload that obfuscates user-specific details.
        """
        anonymized_batch_request_properties_dict: Optional[Dict[str, List[str]]] = None

        # noinspection PyBroadException
        try:
            from great_expectations.core.batch import (
                BatchRequest,
                get_batch_request_from_acceptable_arguments,
                standardize_batch_request_display_ordering,
            )

            batch_request: BatchRequest = get_batch_request_from_acceptable_arguments(
                *args, **kwargs
            )
            batch_request_dict: dict = batch_request.to_json_dict()

            anonymized_batch_request_dict: Optional[
                Union[str, dict]
            ] = self._anonymize_batch_request_properties(source=batch_request_dict)
            anonymized_batch_request_dict = standardize_batch_request_display_ordering(
                batch_request=anonymized_batch_request_dict
            )
            deep_filter_properties_iterable(
                properties=anonymized_batch_request_dict,
                clean_falsy=True,
                inplace=True,
            )

            anonymized_batch_request_required_top_level_properties: dict = {}
            batch_request_optional_top_level_keys: List[str] = []
            batch_spec_passthrough_keys: List[str] = []
            data_connector_query_keys: List[str] = []
            runtime_parameters_keys: List[str] = []

            anonymized_batch_request_properties_dict = {
                "anonymized_batch_request_required_top_level_properties": (
                    anonymized_batch_request_required_top_level_properties
                ),
                "batch_request_optional_top_level_keys": batch_request_optional_top_level_keys,
                "batch_spec_passthrough_keys": batch_spec_passthrough_keys,
                "runtime_parameters_keys": runtime_parameters_keys,
                "data_connector_query_keys": data_connector_query_keys,
            }
            self._build_anonymized_batch_request(
                destination=anonymized_batch_request_properties_dict,
                source=anonymized_batch_request_dict,
            )
            deep_filter_properties_iterable(
                properties=anonymized_batch_request_properties_dict,
                clean_falsy=True,
                inplace=True,
            )
            batch_request_optional_top_level_keys.sort()
            batch_spec_passthrough_keys.sort()
            data_connector_query_keys.sort()
            runtime_parameters_keys.sort()

        except Exception:
            logger.debug(
                "anonymize_batch_request: Unable to create anonymized_batch_request payload field"
            )

        return anonymized_batch_request_properties_dict

    def _anonymize_batch_request_properties(
        self, source: Optional[Any] = None
    ) -> Optional[Union[str, dict]]:
        if source is None:
            return None

        if isinstance(source, str) and source in BATCH_REQUEST_FLATTENED_KEYS:
            return source

        if isinstance(source, dict):
            source_copy: dict = copy.deepcopy(source)
            anonymized_keys: Set[str] = set()

            key: str
            value: Any
            for key, value in source.items():
                if key in BATCH_REQUEST_FLATTENED_KEYS:
                    if self._is_getting_started_keyword(value=value):
                        source_copy[key] = value
                    else:
                        source_copy[key] = self._anonymize_batch_request_properties(
                            source=value
                        )
                else:
                    anonymized_key: str = self._anonymize_string(key)
                    source_copy[
                        anonymized_key
                    ] = self._anonymize_batch_request_properties(source=value)
                    anonymized_keys.add(key)

            for key in anonymized_keys:
                source_copy.pop(key)

            return source_copy

        return self._anonymize_string(str(source))

    def _build_anonymized_batch_request(
        self,
        destination: Optional[Dict[str, Union[Dict[str, str], List[str]]]],
        source: Optional[Any] = None,
    ) -> None:
        if isinstance(source, dict):
            key: str
            value: Any
            for key, value in source.items():
                if key in BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS:
                    destination[
                        "anonymized_batch_request_required_top_level_properties"
                    ][f"anonymized_{key}"] = value
                elif key in BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS:
                    destination["batch_request_optional_top_level_keys"].append(key)
                elif key in BATCH_SPEC_PASSTHROUGH_KEYS:
                    destination["batch_spec_passthrough_keys"].append(key)
                elif key in DATA_CONNECTOR_QUERY_KEYS:
                    destination["data_connector_query_keys"].append(key)
                elif key in RUNTIME_PARAMETERS_KEYS:
                    destination["runtime_parameters_keys"].append(key)
                else:
                    pass

                self._build_anonymized_batch_request(
                    destination=destination, source=value
                )

    @staticmethod
    def _is_getting_started_keyword(value: str) -> bool:
        return value in [
            GETTING_STARTED_DATASOURCE_NAME,
            GETTING_STARTED_EXPECTATION_SUITE_NAME,
            GETTING_STARTED_CHECKPOINT_NAME,
        ]
