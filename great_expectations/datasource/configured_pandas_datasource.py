import os
import re
import warnings
from typing import Any, Dict, List, Tuple

import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.id_dict import IDDict
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.misc_types import (
    NewConfiguredBatchRequest,
)
from great_expectations.datasource.data_connector.util import (
    convert_batch_identifiers_to_data_reference_string_using_regex,
)
from great_expectations.datasource.new_new_new_datasource import NewNewNewDatasource
from great_expectations.datasource.configured_pandas_data_asset import ConfiguredPandasDataAsset
from great_expectations.marshmallow__shade.fields import Bool
from great_expectations.types import DictDot
from great_expectations.validator.validator import Validator

#!!! Keep this? It disables some annoying pandas warnings.
warnings.simplefilter(action="ignore", category=FutureWarning)
warnings.simplefilter(action="ignore", category=pd.errors.ParserWarning)

#!!! Factor this out to somewhere nicer
class GxExperimentalWarning(Warning):
    pass


class ConfiguredPandasDatasource(NewNewNewDatasource):
    """ """

    def __init__(
        self,
        name,
    ):
        #!!! Trying this on for size
        # experimental-v0.15.1
        # warnings.warn(
        #     "\n================================================================================\n" \
        #     "ConfiguredPandasDatasource is an experimental feature of Great Expectations\n" \
        #     "You should consider the API to be unstable.\n" \
        #     "If you have questions or feedback, please chime in at\n" \
        #     "https://github.com/great-expectations/great_expectations/discussions/DISCUSSION-ID-GOES-HERE\n" \
        #     "================================================================================\n",
        #     GxExperimentalWarning,
        # )
        self._name = name
        self._assets = DictDot()

        self._execution_engine = instantiate_class_from_config(
            config={
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            runtime_environment={"concurrency": None},
            config_defaults={"module_name": "great_expectations.execution_engine"},
        )

    def add_asset(
        self,
        name: str,
        base_directory: str = "",
        method: str = "read_csv",
        regex: str = "(.*)",
        batch_identifiers: str = ["filename"],
    ) -> ConfiguredPandasDataAsset:

        new_asset = ConfiguredPandasDataAsset(
            datasource=self,
            name=name,
            method=method,
            base_directory=base_directory,
            regex=regex,
            batch_identifiers=batch_identifiers,
        )

        self._assets[name] = new_asset

        return new_asset

    def list_asset_names(self) -> List[str]:
        return list(self.assets.keys())

    def get_batch(self, batch_request: NewConfiguredBatchRequest) -> Batch:
        asset = self.assets[batch_request.data_asset_name]

        func = getattr(pd, asset.method)

        filename = convert_batch_identifiers_to_data_reference_string_using_regex(
            batch_identifiers=IDDict(**batch_request.batch_identifiers),
            regex_pattern=asset.regex,
            group_names=asset.batch_identifiers,
            # data_asset_name= self.name,
        )
        primary_arg = os.path.join(
            asset.base_directory,
            filename,
        )

        # !!! How do we handle non-serializable elements like `con` for sql?

        args = batch_request.passthrough_parameters.get("args", [])
        kwargs = batch_request.passthrough_parameters.get("kwargs", {})

        df = func(primary_arg, *args, **kwargs)

        batch = Batch(
            data=df,
            batch_request=batch_request,
        )

        return batch

    def get_validator(self, batch_request: NewConfiguredBatchRequest) -> Batch:
        batch = self.get_batch(batch_request)
        return Validator(
            execution_engine=self._execution_engine,
            expectation_suite=None,  # expectation_suite,
            batches=[batch],
        )

    def rename_asset(self, old_name: str, new_name: str) -> None:
        if not isinstance(new_name, str):
            raise TypeError(f"new_name must be of type str, not {type(new_name)}.")

        if new_name in self._assets.keys():
            raise KeyError(f"An asset named {new_name} already exists.")

        self._assets[new_name] = self._assets.pop(old_name)
        asset = self._assets[new_name]
        asset.set_name(new_name)

        return asset

    @property
    def assets(self) -> Dict[str, ConfiguredPandasDataAsset]:
        return self._assets

    @property
    def name(self) -> str:
        return self._name

    def _decide_whether_to_use_variable_as_identifier(self, var):
        #!!! This is brittle. Almost certainly needs fleshing out.
        if not isinstance(var, str):
            return False

        # Does the string contain any whitespace?
        return re.search("\s", var) == None

    def _remove_excluded_arguments(
        self,
        arguments_excluded_from_runtime_parameters: Dict[str, int],
        args: List[Any],
        kwargs: Dict[str, Any],
    ) -> Tuple[List[Any], Dict[str, Any]]:
        remove_indices = []
        for arg_name, index in arguments_excluded_from_runtime_parameters.items():
            kwargs.pop(arg_name, None)
            remove_indices.append(index - 1)

        args = [i for j, i in enumerate(args) if j not in remove_indices]

        return args, kwargs
