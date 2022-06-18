import warnings
from typing import Dict

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    GeCloudConfig,
    InMemoryStoreBackendDefaults,
)
from great_expectations.datasource import BaseDatasource
from great_expectations.warnings import GxExperimentalWarning
from great_expectations.datasource.new_new_new_datasource import NewNewNewDatasource
from great_expectations.types.base import DotDict
from great_expectations.util import load_class


class LiteDataContext(BaseDataContext):
    #!!! Rather than start from a config, it would be better to programmatically instantiate this datasource in __init__. That will allow other configs to be passed in.
    default_context_config: DataContextConfig = DataContextConfig(
        datasources={
            "runtime_pandas": {
                "class_name": "RuntimePandasDatasource",
                "module_name": "great_expectations.datasource.runtime_pandas_datasource",
            },
            "configured_pandas": {
                "class_name": "ConfiguredPandasDatasource",
                "module_name": "great_expectations.datasource.configured_pandas_datasource",
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
        project_config: DataContextConfig=default_context_config,
        context_root_dir:str=None,
        runtime_environment:str=None,
        ge_cloud_mode:bool=False,
        ge_cloud_config:GeCloudConfig=None,
    ) -> BaseDataContext:

        #!!! Trying this on for size...
        # experimental-v0.15.1
        # warnings.warn(
        #     "\n================================================================================\n" \
        #     "LiteDataContext is an experimental feature of Great Expectations.\n" \
        #     "You should consider the API to be unstable.\n" \
        #     "\n" \
        #     "You can disable this warning by calling: \n" \
        #     "from great_expectations.warnings import GxExperimentalWarning\n" \
        #     "warnings.simplefilter(action=\"ignore\", category=GxExperimentalWarning)\n" \
        #     "\n" \
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

    def fancy_add_datasource(
        self,
        name: str,
        module_name: str,
        class_name: str,
        **kwargs,
    ) -> NewNewNewDatasource:
        class_ = load_class(class_name=class_name, module_name=module_name)
        self._cached_datasources[name] = class_(name=name, **kwargs)
