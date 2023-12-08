import copy
import logging
from typing import Any, Dict, List, Optional, Set, Union

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    BATCH_REQUEST_FLATTENED_KEYS,
    BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS,
    BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS,
    BATCH_SPEC_PASSTHROUGH_KEYS,
    DATA_CONNECTOR_QUERY_KEYS,
    GETTING_STARTED_CHECKPOINT_NAME,
    GETTING_STARTED_DATASOURCE_NAME,
    GETTING_STARTED_EXPECTATION_SUITE_NAME,
    RUNTIME_PARAMETERS_KEYS,
)
from great_expectations.util import deep_filter_properties_iterable

logger = logging.getLogger(__name__)


class BatchRequestAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(
        self, obj: Optional[object] = None, **kwargs
    ) -> Dict[str, Union[dict, List[str]]]:
        anonymized_batch_request_properties_dict: Dict[str, Union[dict, List[str]]] = {}

        # noinspection PyBroadException
        try:
            from great_expectations.core.batch import (
                BatchRequest,
                get_batch_request_from_acceptable_arguments,
                standardize_batch_request_display_ordering,
            )

            batch_request: BatchRequest = get_batch_request_from_acceptable_arguments(
                **kwargs
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

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

        attrs: Set[str] = BatchRequest.include_field_names.union(
            RuntimeBatchRequest.include_field_names
        )
        for kwarg in kwargs:
            if kwarg in attrs or kwarg == "batch_request":
                return True

        return False
