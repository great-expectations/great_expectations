from dataclasses import dataclass
import logging
from typing import List, Dict
from great_expectations.core.batch import BatchRequest

from great_expectations.validator.validator import Validator
from great_expectations.datasource.base_data_asset import (
    BaseDataAsset,
    NewBatchRequest,
    BatchIdentifierException,
    BatchIdentifiers,
)
# from great_expectations.datasource.pandas_reader_datasource import PandasReaderDatasource # !!! This creates a circular import

logger = logging.getLogger(__name__)

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

        return NewBatchRequest(
            datasource_name=self._datasource.name,
            data_asset_name=self._name,
            batch_identifiers=batch_identifiers
        )

    def get_validator(self, *batch_identifier_kargs, **batch_identifier_kwargs):
        batch_request = self.get_batch_request(
            *batch_identifier_kargs,
            **batch_identifier_kwargs,
        )
        # !!! The current implementation of this method is a hack to get a basic prototype working
        self._datasource.data_connectors["configured_data_connector"]._base_directory = self._base_directory

        batch = self._datasource.get_single_batch_from_batch_request(
            batch_request=BatchRequest(
                datasource_name=self._datasource.name,
                data_connector_name="configured_data_connector",
                data_asset_name="default_data_asset",
                data_connector_query={
                    "batch_filter_parameters": batch_request.batch_identifiers
                },
            )
        )
        validator = Validator(
            execution_engine=self._datasource.execution_engine,
            expectation_suite=None,#expectation_suite,
            batches=[batch],
        )

        return validator

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

    def __str__(self):
        # !!! We should figure out a convention for __str__ifying objects, and apply it across the codebase
        return f"""great_expectations.datasource.pandas_reader_data_asset.PandasReaderDataAsset object :
    datasource:        {self._datasource}
    name:              {self._name}
    batch_identifiers: {self._batch_identifiers}
    method:            {self._method}
    base_directory:    {self._base_directory}
    regex:             {self._regex}
"""

    def __eq__(self, other) -> bool:
        # !!! I'm not sure if this is a good implementation of __eq__, but I had to do something to get `assert A == B` in tests working.

        return all([
            self._datasource == other._datasource,
            self._name == other._name,
            self._batch_identifiers == other._batch_identifiers,
            self._method == other._method,
            self._base_directory == other._base_directory,
            self._regex == other._regex,
        ])
