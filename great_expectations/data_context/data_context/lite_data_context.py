import warnings
from typing import Dict

from great_expectations.types.base import DotDict
from great_expectations.data_context.data_context.base_data_context import BaseDataContext
from great_expectations.datasource import BaseDatasource
from great_expectations.data_context.types.base import (
    DataContextConfig,
    InMemoryStoreBackendDefaults,
)

#!!! Factor this out to somewhere nicer
class GxExperimentalWarning(Warning):
    pass

class LiteDataContext(BaseDataContext):
    #!!! Rather than start from a config, it would be better to programmatically instantiate this datasource in __init__. That will allow other configs to be passed in.
    default_context_config: DataContextConfig = DataContextConfig(
        datasources={
            "default_pandas_reader": {
                "class_name": "PandasReaderDatasource",
                "module_name": "great_expectations.datasource",
            },
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        checkpoint_store_name="checkpoint_store",
        store_backend_defaults=InMemoryStoreBackendDefaults(),
    )

    def __init__(
        self,
        project_config=default_context_config,
        context_root_dir=None,
        runtime_environment=None,
        ge_cloud_mode=False,
        ge_cloud_config=None,
    ) -> BaseDataContext:

        #!!! Trying this on for size...
        # experimental-v0.15.1
        # warnings.warn(
        #     "\n================================================================================\n" \
        #     "LiteDataContext is an experimental feature of Great Expectations.\n" \
        #     "You should consider the API to be unstable.\n" \
        #     "If you have questions or feedback, please chime in at\n" \
        #     "https://github.com/great-expectations/great_expectations/discussions/DISCUSSION-ID-GOES-HERE" \
        #     "\n================================================================================\n",
        #     GxExperimentalWarning,
        # )

        super().__init__(
            project_config=project_config,
            context_root_dir=context_root_dir,
            runtime_environment=runtime_environment,
            ge_cloud_mode=ge_cloud_mode,
            ge_cloud_config=ge_cloud_config,
        )

    @property
    def datasources(self) -> Dict[str, BaseDatasource]:
        """A single holder for all Datasources in this context"""
        return DotDict(self._cached_datasources)

    @property
    def sources(self) -> Dict[str, BaseDatasource]:
        """An alias for self.datasources
        
        This method is purely for convenience.
        
        Several other objects and methods also start with "data",
        so it's often nicer to be able to type "so<TAB>" to autocomplete.
        """
        return self.datasources
