import copy
from typing import Any, Dict, List, Optional, Set, Union

from great_expectations.core.batch import (
    BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS,
    BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS,
    DATA_CONNECTOR_QUERY_KEYS,
    RUNTIME_PARAMETERS_KEYS,
    RECOGNIZED_BATCH_SPEC_PASSTHROUGH_KEYS,
    BATCH_REQUEST_FLATTENED_KEYS,
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer
from great_expectations.util import filter_properties_dict


class BatchRequestAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

        self._batch_request_optional_top_level_keys = []
        self._data_connector_query_keys = []
        self._runtime_parameters_keys = []
        self._batch_spec_passthrough_keys = []
        self._anonymized_batch_request_keys = {
            "batch_request_optional_top_level_keys": self._batch_request_optional_top_level_keys,
            "data_connector_query_keys": self._data_connector_query_keys,
            "runtime_parameters_keys": self._batch_spec_passthrough_keys,
            "recognized_batch_spec_passthrough_keys": [],
        }

    def anonymize_batch_request(self, *args, **kwargs) -> Dict[str, Union[str, Dict[str, Any]]]:
        batch_request: Union[
            BatchRequest, RuntimeBatchRequest
        ] = get_batch_request_from_acceptable_arguments(*args, **kwargs)
        batch_request_dict: dict = batch_request.to_json_dict()
        anonymized_batch_request_dict: Optional[
            Union[Any, dict]
        ] = self._anonymize_batch_request_properties(source=batch_request_dict)
        filter_properties_dict(
            properties=anonymized_batch_request_dict,
            delete_fields=list(BATCH_REQUEST_REQUIRED_TOP_LEVEL_KEYS),
            clean_falsy=True,
            keep_falsy_numerics=True,
            inplace=True,
        )
        self._delete_extraneous_batch_request_properties(
            anonymized_batch_request_dict=anonymized_batch_request_dict,
        )
        anonymized_batch_request: Dict[str, Union[str, Dict[str, Any]]]
        self._build_anonymized_batch_request(
            destination=anonymized_batch_request, source=anonymized_batch_request_dict
        )
        return anonymized_batch_request

    @staticmethod
    def _delete_extraneous_batch_request_properties(cls, anonymized_batch_request_dict: Optional[Union[Any, dict]]):
        key: str
        value: Any
        for key, value in anonymized_batch_request_dict.items():
            if isinstance(value, dict):
                filter_properties_dict(
                    properties=value,
                    clean_falsy=True,
                    keep_falsy_numerics=True,
                    inplace=True,
                )
                cls._delete_extraneous_batch_request_properties(anonymized_batch_request_dict=value)


    def _anonymize_batch_request_properties(
        self, source: Optional[Any] = None
    ) -> Optional[Union[Any, dict]]:
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
                    source_copy[key] = self._anonymize_batch_request_properties(
                        source=value
                    )
                else:
                    anonymized_key: str = self.anonymize(key)
                    source_copy[
                        anonymized_key
                    ] = self._anonymize_batch_request_properties(source=value)
                    anonymized_keys.add(key)

            for key in anonymized_keys:
                source_copy.pop(key)

            return source_copy

        return self.anonymize(str(source))

    def _build_anonymized_batch_request(
        self, destination: Dict[str, Any], source: Optional[Any] = None
    ):
        if isinstance(source, dict):
            key: str
            value: Any
            for key, value in source.items():
                if key in BATCH_REQUEST_OPTIONAL_TOP_LEVEL_KEYS:
                    destination.append(key)
                else:
                    anonymized_keys: List[str] = []
                    if key in DATA_CONNECTOR_QUERY_KEYS:
                        anonymized_data_connector_query_keys: Set[str] =
                        destination.append({"anonymized_data_connector_query_keys": anonymized_keys})
                    elif key in RUNTIME_PARAMETERS_KEYS:
                        anonymized_keys: List[Union[str, dict]] = []
                        destination.append({"anonymized_runtime_parameters_keys": anonymized_keys})
                    elif key in RECOGNIZED_BATCH_SPEC_PASSTHROUGH_KEYS:
                        anonymized_keys: List[Union[str, dict]] = []
                        destination.append({"anonymized_recognized_batch_spec_passthrough_keys": anonymized_keys})
                    else:
                        pass

                self._build_anonymized_batch_request(
                    destination=anonymized_keys, source=value
                )

                if key in BATCH_REQUEST_FLATTENED_KEYS:
                    if isinstance(value, dict):
                        anonymized_keys: List[Union[str, dict]] = []
                        destination.append({key: anonymized_keys})
                        self._build_anonymized_batch_request(
                            destination=anonymized_keys, source=value
                        )
                    else:
                        destination.append(key)
