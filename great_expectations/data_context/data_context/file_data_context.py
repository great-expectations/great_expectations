import logging
import pathlib
from typing import Mapping, Optional, Union, cast

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as gx_exceptions
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
    FileDataContextVariables,
)
from great_expectations.data_context.store.store import Store
from great_expectations.data_context.types.base import (
    DataContextConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)
from great_expectations.exceptions.exceptions import ConfigNotFoundError

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class FileDataContext(SerializableDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    def __init__(
        self,
        project_config: Optional[Union[DataContextConfig, Mapping]] = None,
        context_root_dir: Optional[str] = None,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        self._context_root_directory = self._init_context_root_directory(
            context_root_dir
        )
        self._project_config = self._init_project_config(project_config=project_config)

        super().__init__(
            context_root_dir=self._context_root_directory,
            runtime_environment=runtime_environment,
        )

    def _init_context_root_directory(self, context_root_dir: Optional[str]) -> str:
        if not context_root_dir:
            context_root_dir = FileDataContext.find_context_yml_file()
            # If we still can't find one, panic since a FileDataContext is dependent on a local project config
            if not context_root_dir:
                raise ValueError(
                    "A FileDataContext relies on the presence of a local great_expectations.yml project config"
                )
        return context_root_dir

    def _init_project_config(
        self,
        project_config: Optional[Union[DataContextConfig, Mapping]],
    ) -> DataContextConfig:
        if project_config:
            # If a mapping was provided, convert to a rich DataContextConfig for downstream usage
            project_config = FileDataContext.get_or_create_data_context_config(
                project_config
            )
        else:
            project_config = FileDataContext._load_file_backed_project_config(
                context_root_directory=self._context_root_directory,
            )

        return self._apply_global_config_overrides(config=project_config)

    def _init_datasource_store(self) -> None:
        from great_expectations.data_context.store.datasource_store import (
            DatasourceStore,
        )

        store_name: str = "datasource_store"  # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_backend: dict = {
            "class_name": "InlineStoreBackend",
            "resource_type": DataContextVariableSchema.DATASOURCES,
        }
        runtime_environment: dict = {
            "root_directory": self.root_directory,
            "data_context": self,
            # By passing this value in our runtime_environment,
            # we ensure that the same exact context (memory address and all) is supplied to the Store backend
        }

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            runtime_environment=runtime_environment,
            serializer=YAMLReadyDictDatasourceConfigSerializer(
                schema=datasourceConfigSchema
            ),
        )
        self._datasource_store = datasource_store

    @property
    def root_directory(self) -> Optional[str]:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.

        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it

        """
        return self._context_root_directory

    def _init_variables(self) -> FileDataContextVariables:
        variables = FileDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
            data_context=self,
        )
        return variables

    def add_store(self, store_name: str, store_config: dict) -> Optional[Store]:
        """
        See parent `AbstractDataContext.add_store()` for more information.

        """
        store = super().add_store(store_name=store_name, store_config=store_config)
        self._save_project_config()
        return store

    @classmethod
    def _load_file_backed_project_config(
        cls,
        context_root_directory: str,
    ) -> DataContextConfig:
        path_to_yml = pathlib.Path(context_root_directory, cls.GX_YML)
        try:
            with open(path_to_yml) as data:
                config_commented_map_from_yaml = yaml.load(data)

        except DuplicateKeyError:
            raise gx_exceptions.InvalidConfigurationYamlError(
                "Error: duplicate key found in project YAML file."
            )
        except YAMLError as err:
            raise gx_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                    err
                )
            )
        except OSError:
            raise gx_exceptions.ConfigNotFoundError()

        try:
            return cast(
                DataContextConfig,
                DataContextConfig.from_commented_map(
                    commented_map=config_commented_map_from_yaml
                ),
            )
        except gx_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise
