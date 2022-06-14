import logging
from typing import List, Optional, Dict

# from great_expectations.datasource.pandas_reader_datasource import PandasReaderDatasource #!!! This causes a circular import

logger = logging.getLogger(__name__)

class BatchIdentifierException(BaseException):
    # !!! Do we want to create a class for this? Is this the right name and inheritance?
    pass

class BatchIdentifiers(dict):
    pass

class RuntimeParameters(dict):
    pass

class NewBatchRequest:
    # !!! This prolly isn't the right name for this class

    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        batch_identifiers: Optional[BatchIdentifiers] = None,
        runtime_parameters: Optional[RuntimeParameters] = None,
    ) -> None:
        self._datasource_name = datasource_name
        self._data_asset_name = data_asset_name

        self._batch_identifiers = batch_identifiers
        self._runtime_parameters = runtime_parameters

    @property
    def datasource_name(self) -> str:
        return self._datasource_name

    @property
    def data_asset_name(self) -> str:
        return self._data_asset_name

    @property
    def batch_identifiers(self) -> str:
        return self._batch_identifiers

    @property
    def runtime_parameters(self) -> str:
        return self._runtime_parameters

    def __str__(self) -> str:
        # !!! This isn't right---very slapdash
        return f"""{self.datasource_name},{self.data_asset_name},{self._batch_identifiers},{self._runtime_parameters}"""

    def __eq__(self, other) -> bool:
        return all([
           self.datasource_name == other.datasource_name,
           self.data_asset_name == other.data_asset_name,
           self.batch_identifiers == other.batch_identifiers,
           self.runtime_parameters == other.runtime_parameters,
        ])


class BaseDataAsset:

    def __init__(
        self,
        datasource, #Should be of type: V15Datasource
        name: str,
        batch_identifiers: List[str]
    ) -> None:
        self._datasource = datasource
        self._name = name
        self._batch_identifiers = batch_identifiers

    @property
    def name(self) -> str:
        return self._name

    @property
    def batch_identifiers(self) -> List[str]:
        return self._batch_identifiers


class PandasReaderDataAsset(BaseDataAsset):

    def __init__(
        self,
        datasource, #Should be of type: PandasReaderDatasource,
        name: str,
        batch_identifiers: List[str],
        method: str,
        base_directory: str,
        regex: str,
    ) -> None:
        self._method = method
        self._base_directory = base_directory
        self._regex = regex

        super().__init__(
            datasource=datasource,
            name=name,
            batch_identifiers=batch_identifiers
        )
    
    def get_batch_request(self, *batch_identifier_args, **batch_identifier_kwargs) -> NewBatchRequest:

        batch_identifiers = self._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args,
            batch_identifier_kwargs,
        )

        print(type(batch_identifiers))

        return NewBatchRequest(
            datasource_name=self._datasource.name,
            data_asset_name=self._name,
            batch_identifiers=batch_identifiers
        )

    def get_validator(self, *batch_identifier_kargs, **batch_identifier_kwargs):
        # return self._datasource()
        pass

    def _generate_batch_identifiers_from_args_and_kwargs(
        self,
        batch_identifier_args : List[str],
        batch_identifier_kwargs : Dict,
    ) -> BatchIdentifiers:

        if len(batch_identifier_args) > len(self._batch_identifiers):
            raise BatchIdentifierException(f"Expected no more than {len(self._batch_identifiers)} batch_identifiers. Got {len(batch_identifier_args)} instead.")

        unknown_keys = set(batch_identifier_kwargs.keys()).difference(self._batch_identifiers)
        if unknown_keys != set({}):
            raise BatchIdentifierException(f"Unknown BatchIdentifier keys : {unknown_keys}")

        arg_dict = dict(zip(self.batch_identifiers, batch_identifier_args))

        overlapping_keys = set(arg_dict).intersection(batch_identifier_kwargs.keys())
        if overlapping_keys != set():
            raise BatchIdentifierException(f"Duplicate BatchIdentifier keys: {unknown_keys}")
        
        batch_identifier_dict = {**arg_dict, **batch_identifier_kwargs}

        missing_keys = set(self._batch_identifiers).difference(batch_identifier_dict.keys())
        if missing_keys != set({}):
            raise BatchIdentifierException(f"Missing BatchIdentifier keys : {missing_keys}")

        return BatchIdentifiers(**batch_identifier_dict)