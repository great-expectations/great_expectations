from typing import Any, List, Optional, Union

from great_expectations.core.batch import (
    BATCH_REQUEST_INSTANTIATION_KEYS,
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_from_acceptable_arguments,
)
from great_expectations.core.usage_statistics.anonymizers.anonymizer import Anonymizer


class BatchRequestAnonymizer(Anonymizer):
    def __init__(self, salt=None):
        super().__init__(salt=salt)

    # TODO: <Alex>ALEX</Alex>
    def _anonymize_batch_request_dictionary_properties(
        self, source: Optional[Any] = None
    ) -> Optional[Union[Any, dict]]:
        if source is None:
            return None

        if isinstance(source, dict):
            filter_properties_dict(properties=source, inplace=True)
            key: str
            value: Any
            for key, value in source.items():
                source[key] = self._anonymize_batch_request_dictionary_properties(source=value)
        else:
            

        return source

    def _convert_dictionaries_to_parameter_nodes(
            self, source: Optional[Any] = None
    ) -> Optional[Union[Any, ParameterNode]]:
        if source is None:
            return None

        if isinstance(source, dict):
            if not isinstance(source, ParameterNode):
                filter_properties_dict(properties=source, inplace=True)
                source = ParameterNode(source)
            key: str
            value: Any
            for key, value in source.items():
                source[key] = self._convert_dictionaries_to_parameter_nodes(
                    source=value
                )

        return source

    def anonymize_batch_request(self, **kwargs) -> List[Union[str, dict]]:
        batch_request: Union[BatchRequest, RuntimeBatchRequest] = get_batch_request_from_acceptable_arguments(**kwargs)
        batch_request_dict: dict = batch_request.to_json_dict()
        # TODO: <Alex>ALEX</Alex>
        # batch_request_keys: List[str] = list(batch_request_dict.keys())
        # TODO: <Alex>ALEX</Alex>

        anonymized_batch_request: List[Union[str, dict]] = []

        key: str
        value: Any
        for key, value in batch_request_dict.keys():
            if isinstance(value, dict):





        for batch_request_key in kwargs:
            if batch_request_key in batch_request_keys:
                anonymized_batch_request_keys.append(batch_request_key)
            else:
                if "batch_spec_passthrough" in batch_request_dict:
                    if batch_request_key
                if (batch_request_key in batch_request_keys) or (("batch_spec_passthrough" in batch_request_dict) and (batch_request_key in batch_request_dict["batch_spec_passthrough"])):
                    anonymized_batch_request_keys.append(batch_request_key)
                elif (batch_request_key in BATCH_REQUEST_INSTANTIATION_KEYS) and (("batch_spec_passthrough" in batch_request_dict) and (batch_request_key in batch_request_dict["batch_spec_passthrough"])):
                    anonymized_batch_request_keys.append(self.anonymize(batch_request_key))
                else:
                    anonymized_batch_request_keys.append(self.anonymize(batch_request_key))


        batch_spec_passthrough: Optional[dict] = batch_request_dict.pop("batch_spec_passthrough", None)

        for batch_request_key in batch_request_dict.keys():
            if batch_request_key in BATCH_REQUEST_INSTANTIATION_KEYS:
                anonymized_batch_request_keys.append(batch_request_key)
            else:
                anonymized_batch_request_keys.append(self.anonymize(batch_request_key))

        return anonymized_batch_request_keys

    def anonymize_batch_info(self, batch):
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

        anonymized_info_dict = {}

        if batch_kwargs:
            anonymized_info_dict[
                "anonymized_batch_kwarg_keys"
            ] = self._batch_kwargs_anonymizer.anonymize_batch_kwargs(batch_kwargs)
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
