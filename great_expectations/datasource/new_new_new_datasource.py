#!!! Rename this
from typing import Dict, List
from great_expectations.core.batch import Batch
from great_expectations.datasource.base_data_asset import BaseDataAsset
from great_expectations.datasource.misc_types import NewBatchRequestBase
from great_expectations.types import DictDot
from great_expectations.validator.validator import Validator


class NewNewNewDatasource:

    def __init__(
        self,
        name,
    ):
        #!!! Trying this on for size
        # experimental-v0.15.1
        # warnings.warn(
        #     "\n================================================================================\n" \
        #     "NewNewNewDatasource is an experimental feature of Great Expectations\n" \
        #     "You should consider the API to be unstable.\n" \
        #     "\n" \
        #     "You can disable this warning by calling: \n" \
        #     "from great_expectations.warnings import GxExperimentalWarning\n" \
        #     "warnings.simplefilter(action=\"ignore\", category=GxExperimentalWarning)\n" \
        #     "\n" \
        #     "If you have questions or feedback, please chime in at\n" \
        #     "https://github.com/great-expectations/great_expectations/discussions/DISCUSSION-ID-GOES-HERE\n" \
        #     "================================================================================\n",
        #     GxExperimentalWarning,
        # )

        self._name = name
        self._assets = DictDot()

    def add_asset(self, name:str, *args, **kwargs) -> BaseDataAsset:
        raise NotImplementedError

    def rename_asset(self, old_name: str, new_name: str) -> None:
        if not isinstance(new_name, str):
            raise TypeError(f"new_name must be of type str, not {type(new_name)}.")

        if new_name in self._assets.keys():
            raise KeyError(f"An asset named {new_name} already exists.")

        self._assets[new_name] = self._assets.pop(old_name)
        asset = self._assets[new_name]
        asset.set_name(new_name)

        return asset

    def list_asset_names(self) -> List[str]:
        return list(self.assets.keys())

    def get_batch(self, *args, **kwargs) -> Batch:
        raise NotImplementedError

    def get_validator(self, *args, **kwargs) -> Validator:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def assets(self) -> Dict[str, BaseDataAsset]:
        return self._assets
