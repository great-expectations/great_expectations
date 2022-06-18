import glob
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

from typing_extensions import Self

from great_expectations.core.batch import Batch, BatchRequest
from great_expectations.datasource.base_data_asset import BaseDataAsset
from great_expectations.datasource.misc_types import (
    BatchIdentifierException,
    BatchSpecPassthrough,
    DataConnectorQuery,
    NewBatchRequestBase,
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.data_connector.util import (
    get_filesystem_one_level_directory_glob_path_list,
)
from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)


class ConfiguredPandasDataAsset(BaseDataAsset):
    def __init__(
        self,
        datasource,  # Should be of type: ConfiguredPandasDatasource,
        name: str,
        batch_identifiers: List[str] = ["filename"],
        method: str = "read_csv",
        base_directory: str = "",
        regex: str = "(.*)",
    ) -> None:

        # !!! Check to make sure this is a valid configuration of parameters.
        # !!! For example, if base_directory is Null, method and regex should be Null as well.
        # !!! Also, if base_directory is not Null, method and regex should be populated as well. defaults are okay.
        # !!! Also, if regex is not Null, the number of groups should be exactly equal to the number of parameters in batch_identifiers

        self._method = method
        self._base_directory = base_directory
        self._regex = regex

        super().__init__(
            datasource=datasource, name=name, batch_identifiers=batch_identifiers
        )

    def update_configuration(
        self,
        name: Optional[str] = None,
        batch_identifiers: Optional[List[str]] = None,
        method: Optional[str] = None,
        base_directory: Optional[str] = None,
        regex: Optional[str] = None,
    ) -> Self:

        # !!! Check to make sure this is a valid configuration of parameters.
        # !!! For example, if base_directory is Null, method and regex should be Null as well.
        # !!! Also, if base_directory is not Null, method and regex should be populated as well. defaults are okay.
        # !!! Also, if regex is not Null, the number of groups should be exactly equal to the number of parameters in batch_identifiers

        if name != None:
            self._datasource.rename_asset(self._name, name)

        if batch_identifiers != None:
            self._batch_identifiers = batch_identifiers

        if method != None:
            self._method = method

        if base_directory != None:
            self._base_directory = base_directory

        if regex != None:
            self._regex = regex

        return self

    def get_batch_request(
        self, *batch_identifier_args, **batch_identifier_kwargs
    ) -> NewBatchRequestBase:

        batch_identifiers = self._generate_batch_identifiers_from_args_and_kwargs(
            batch_identifier_args,
            batch_identifier_kwargs,
        )

        #!!! Need to handle batch_spec passthrough parameters here
        batch_request = NewConfiguredBatchRequest(
            datasource_name=self._datasource.name,
            data_asset_name=self._name,
            data_connector_query=batch_identifiers,
            batch_spec_passthrough=BatchSpecPassthrough(),
            # batch_identifiers=batch_identifiers,
        )

        return batch_request

    def get_validator(
        self, *batch_identifier_args, **batch_identifier_kwargs
    ) -> Validator:
        batch_request = self.get_batch_request(
            *batch_identifier_args,
            **batch_identifier_kwargs,
        )

        validator = self._datasource.get_validator(batch_request)

        return validator

    #!!! This currently returns a Validator, not a Batch
    def get_batch(self, *batch_identifier_args, **batch_identifier_kwargs) -> Validator:
        return self.get_validator(*batch_identifier_args, **batch_identifier_kwargs)

    #!!! Not sure what this method signature should be
    def get_batches(
        self, *batch_identifier_args, **batch_identifier_kwargs
    ) -> Validator:
        raise NotImplementedError

    def list_batches(self) -> List[BatchRequest]:
        asset_paths = get_filesystem_one_level_directory_glob_path_list(
            self.base_directory, "*"
        )
        return asset_paths

    @property
    def batches(self) -> List[Batch]:
        #!!! OMG inefficient
        # return [self.get_batch(b) for b in self.list_batches()]
        # return []
        return self.list_batches()

    def _generate_batch_identifiers_from_args_and_kwargs(
        self,
        batch_identifier_args: List[str],
        batch_identifier_kwargs: Dict,
    ) -> DataConnectorQuery:

        if len(batch_identifier_args) > len(self._batch_identifiers):
            raise BatchIdentifierException(
                f"Expected no more than {len(self._batch_identifiers)} batch_identifiers. Got {len(batch_identifier_args)} instead."
            )

        unknown_keys = set(batch_identifier_kwargs.keys()).difference(
            self._batch_identifiers
        )
        if unknown_keys != set({}):
            raise BatchIdentifierException(
                f"Unknown BatchIdentifier keys : {unknown_keys}"
            )

        arg_dict = dict(zip(self.batch_identifiers, batch_identifier_args))

        overlapping_keys = set(arg_dict).intersection(batch_identifier_kwargs.keys())
        if overlapping_keys != set():
            raise BatchIdentifierException(
                f"Duplicate BatchIdentifier keys: {unknown_keys}"
            )

        batch_identifier_dict = {**arg_dict, **batch_identifier_kwargs}

        missing_keys = set(self._batch_identifiers).difference(
            batch_identifier_dict.keys()
        )
        if missing_keys != set({}):
            raise BatchIdentifierException(
                f"Missing BatchIdentifier keys : {missing_keys}"
            )

        return DataConnectorQuery(**batch_identifier_dict)

    def _get_data_asset_paths(self):
        # glob_config = self._get_data_asset_config(data_asset_name)
        # return glob.glob(os.path.join(self.base_directory, glob_config["glob"]))
        # return glob.glob(os.path.join(self.base_directory, "*.csv"), recursive=True)
        return glob.glob(self.base_directory)

    def __str__(self):
        # !!! We should figure out a convention for __str__ifying objects, and apply it across the codebase
        return f"""great_expectations.datasource.configured_pandas_data_asset.py.ConfiguredPandasDataAsset object :
    datasource:        {self._datasource}
    name:              {self._name}
    batch_identifiers: {self._batch_identifiers}
    method:            {self._method}
    base_directory:    {self._base_directory}
    regex:             {self._regex}
"""

    def __eq__(self, other) -> bool:
        # !!! I'm not sure if this is a good implementation of __eq__, but I had to do something to get `assert A == B` in tests working.

        return all(
            [
                self._datasource == other._datasource,
                self._name == other._name,
                self._batch_identifiers == other._batch_identifiers,
                self._method == other._method,
                self._base_directory == other._base_directory,
                self._regex == other._regex,
            ]
        )

    @property
    def method(self) -> str:
        return self._method

    @property
    def base_directory(self) -> str:
        return self._base_directory

    @property
    def regex(self) -> str:
        return self._regex