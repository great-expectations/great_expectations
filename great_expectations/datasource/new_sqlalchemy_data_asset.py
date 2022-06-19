from typing import List, Optional
from typing_extensions import Self
from great_expectations.core.batch import Batch
from great_expectations.datasource.base_data_asset import BaseDataAsset
from great_expectations.datasource.misc_types import NewConfiguredBatchRequest
from great_expectations.validator.validator import Validator


class ConfiguredNewSqlAlchemyDataAsset(BaseDataAsset):
    
    def __init__(
        self,
        datasource,  # Should be of type: V15Datasource
        name: str,
        batch_identifiers: List[str],
        table_name: str,
        schema_name: Optional[str],
    ) -> None:
        super().__init__(
            datasource=datasource,
            name=name,
            batch_identifiers=batch_identifiers,
        )

        self._table_name = table_name
        self._schema_name = schema_name


    def update_configuration(self, **kwargs) -> Self:
        raise NotImplementedError
        
    def list_batches(self) -> List[NewConfiguredBatchRequest]:
        raise NotImplementedError

    def get_batch(self, *args, **kwargs) -> Batch:
        raise NotImplementedError

    def get_batches(self, *args, **kwargs) -> List[Batch]:
        raise NotImplementedError

    def get_batch_request(self, *args, **kwargs) -> NewConfiguredBatchRequest:
        raise NotImplementedError

    def get_validator(self, *args, **kwargs) -> Validator:
        raise NotImplementedError