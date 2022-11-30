import logging
import os
from typing import Optional

from ruamel.yaml import YAML, YAMLError
from ruamel.yaml.constructor import DuplicateKeyError

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.data_context._serializable_data_context import (
    _SerializableDataContext,
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

logger = logging.getLogger(__name__)

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False


class FileDataContext(_SerializableDataContext):
    """
    Extends AbstractDataContext, contains only functionality necessary to hydrate state from disk.

    TODO: Most of the functionality in DataContext will be refactored into this class, and the current DataContext
    class will exist only for backwards-compatibility reasons.
    """

    GE_YML = "great_expectations.yml"

    def __init__(
        self,
        project_config: Optional[DataContextConfig],
        context_root_dir: str,
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
        self._context_root_directory = context_root_dir

        project_config = project_config or self._load_project_config()
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(runtime_environment=runtime_environment)

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
            data_context=self,  # type: ignore[arg-type]
        )
        return variables

    def _load_project_config(self):
        """
        Reads the project configuration from the project configuration file.
        The file may contain ${SOME_VARIABLE} variables - see self.project_config_with_variables_substituted
        for how these are substituted.

        For Data Contexts in GE Cloud mode, a user-specific template is retrieved from the Cloud API
        - see CloudDataContext.retrieve_data_context_config_from_ge_cloud for more details.

        :return: the configuration object read from the file or template
        """
        path_to_yml = os.path.join(self._context_root_directory, self.GE_YML)
        try:
            with open(path_to_yml) as data:
                config_commented_map_from_yaml = yaml.load(data)

        except DuplicateKeyError:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Error: duplicate key found in project YAML file."
            )
        except YAMLError as err:
            raise ge_exceptions.InvalidConfigurationYamlError(
                "Your configuration file is not a valid yml file likely due to a yml syntax error:\n\n{}".format(
                    err
                )
            )
        except OSError:
            raise ge_exceptions.ConfigNotFoundError()

        try:
            return DataContextConfig.from_commented_map(
                commented_map=config_commented_map_from_yaml
            )
        except ge_exceptions.InvalidDataContextConfigError:
            # Just to be explicit about what we intended to catch
            raise
