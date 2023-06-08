from typing import Any, List, Optional, Tuple, Union

from great_expectations.core.usage_statistics.anonymizers.base import BaseAnonymizer


class BatchAnonymizer(BaseAnonymizer):
    def __init__(
        self,
        aggregate_anonymizer: "Anonymizer",  # noqa: F821
        salt: Optional[str] = None,
    ) -> None:
        super().__init__(salt=salt)

        self._aggregate_anonymizer = aggregate_anonymizer

    def anonymize(
        self,
        obj: Union[Tuple[dict, str], "DataAsset", "Validator"],  # noqa: F821
        **kwargs,
    ) -> Any:
        from great_expectations.data_asset import DataAsset
        from great_expectations.validator.validator import Validator

        batch = obj
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

    def can_handle(self, obj: Optional[object] = None, **kwargs) -> bool:
        from great_expectations.data_asset.data_asset import DataAsset
        from great_expectations.validator.validator import Validator

        if obj is None:
            return False

        return isinstance(obj, (Validator, DataAsset)) or (
            isinstance(obj, tuple) and len(obj) == 2  # noqa: PLR2004
        )
