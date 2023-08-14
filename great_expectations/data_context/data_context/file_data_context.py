from __future__ import annotations

import logging
import pathlib
from typing import TYPE_CHECKING, Mapping, Optional, Union

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as gx_exceptions
from great_expectations.core._docs_decorators import public_api
from great_expectations.data_context.data_context.serializable_data_context import (
    SerializableDataContext,
)
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
    FileDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    datasourceConfigSchema,
)
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)
from great_expectations.datasource.fluent.config import GxConfig

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues, PathStr
    from great_expectations.core.config_provider import _ConfigurationProvider
    from great_expectations.data_context.store.datasource_store import DatasourceStore

logger = logging.getLogger(__name__)
yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


@public_api
class FileDataContext(SerializableDataContext):
    """Subclass of AbstractDataContext that contains functionality necessary to work in a filesystem-backed environment."""

    def __init__(
        self,
        project_config: Optional[DataContextConfig] = None,
        context_root_dir: Optional[PathStr] = None,
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
        self._scaffold_project()

        self._project_config = self._init_project_config(project_config)
        super().__init__(
            context_root_dir=self._context_root_directory,
            runtime_environment=runtime_environment,
        )

    def _init_context_root_directory(self, context_root_dir: Optional[PathStr]) -> str:
        if isinstance(context_root_dir, pathlib.Path):
            context_root_dir = str(context_root_dir)

        if not context_root_dir:
            context_root_dir = FileDataContext.find_context_root_dir()
            if not context_root_dir:
                raise ValueError(
                    "A FileDataContext relies on the presence of a local great_expectations.yml project config"
                )

        return context_root_dir

    def _scaffold_project(self) -> None:
        """Prepare a `great_expectations` directory with all necessary subdirectories.
        If one already exists, no-op.
        """
        if self.is_project_scaffolded(self._context_root_directory):
            return

        # GX makes an important distinction between project directory and context directory.
        # The former corresponds to the root of the user's project while the latter
        # encapsulates any config (in the form of a great_expectations/ directory).
        project_root_dir = pathlib.Path(self._context_root_directory).parent
        self._scaffold(
            project_root_dir=project_root_dir,
        )

    def _init_project_config(
        self, project_config: Optional[Union[DataContextConfig, Mapping]]
    ) -> DataContextConfig:
        if project_config:
            project_config = FileDataContext.get_or_create_data_context_config(
                project_config
            )
        else:
            project_config = FileDataContext._load_file_backed_project_config(
                context_root_directory=self._context_root_directory,
            )
        return self._apply_global_config_overrides(config=project_config)

    def _init_datasource_store(self) -> DatasourceStore:
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
        return datasource_store

    def _init_variables(self) -> FileDataContextVariables:
        variables = FileDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
            data_context=self,
        )
        return variables

    def _save_project_config(self, _fds_datasource=None) -> None:
        """
        See parent 'AbstractDataContext._save_project_config()` for more information.

        Explicitly override base class implementation to retain legacy behavior.
        """
        config_filepath = pathlib.Path(self.root_directory, self.GX_YML)

        logger.debug(
            f"Starting DataContext._save_project_config; attempting to update {config_filepath}"
        )

        try:
            with open(config_filepath, "w") as outfile:
                fluent_datasources = self._synchronize_fluent_datasources()
                if fluent_datasources:
                    self.fluent_config.update_datasources(
                        datasources=fluent_datasources
                    )
                    logger.info(
                        f"Saving {len(self.fluent_config.datasources)} Fluent Datasources to {config_filepath}"
                    )
                    fluent_json_dict: dict[
                        str, JSONValues
                    ] = self.fluent_config._json_dict()
                    fluent_json_dict = (
                        self.fluent_config._exclude_name_fields_from_fluent_datasources(
                            config=fluent_json_dict
                        )
                    )
                    self.config._commented_map.update(fluent_json_dict)

                self.config.to_yaml(outfile)
        except PermissionError as e:
            logger.warning(f"Could not save project config to disk: {e}")

    @classmethod
    def _load_file_backed_project_config(
        cls,
        context_root_directory: PathStr,
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
            return DataContextConfig.from_commented_map(
                commented_map=config_commented_map_from_yaml
            )
        except gx_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise

    def _load_fluent_config(self, config_provider: _ConfigurationProvider) -> GxConfig:
        logger.info(f"{type(self).__name__} loading fluent config")
        if not self.root_directory:
            logger.warning("`root_directory` not set, cannot load fluent config")
        else:
            path_to_fluent_yaml = pathlib.Path(self.root_directory) / self.GX_YML
            if path_to_fluent_yaml.exists():
                gx_config = GxConfig.parse_yaml(path_to_fluent_yaml, _allow_empty=True)

                for datasource in gx_config.datasources:
                    datasource._data_context = self

                return gx_config
            logger.info(f"no fluent config at {path_to_fluent_yaml.absolute()}")
        return GxConfig(fluent_datasources=[])
