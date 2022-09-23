import logging
from typing import Mapping, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
    FileDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)
from great_expectations.datasource.datasource_serializer import (
    YAMLReadyDictDatasourceConfigSerializer,
)

logger = logging.getLogger(__name__)


class _BridgeFileDataContext(AbstractDataContext):
    """
    # NOTE: <DataContextRefactor>  09232022

    The _BridgeFileDataContext was created as part of the DataContextRefactor work:
        - It is most closely related to the FileDataContext, but intends to preserve behavior that already exists in
        BaseDataContext.

    More specifically, we currently allow BaseDataContext to have both
        - project_config : an in-memory configuration
        - context_root_dir : path on the filesystem that contains our `great_expectations.yml` file.

    And there are also tests for BaseDataContext that test that `project_config` can override the config in
    `context_root_dir`.

    As part of the DataContextRefactor, we are intending to split the responsibilities of BaseDataContext (and DataContext)
    between FileDataContext and EphemeralDataContext.

    FileDataContext will only consume configurations from `context_root_dir`
    EphemeralDataContext will only consume configurations from the in-memory `project_config`.

    _BridgeFileDataContext is intended to "bridge" and preserve current behavior while we complete the migration into
    FileDataContext and EphemeralDataContext.

    Therefore it is only instantiated from BaseDataContext (using the `from_base_data_context` flag) and will
    give an DataContext error if a user tries to instantiate it on its own.
    """

    GE_YML = "great_expectations.yml"

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: str,
        from_base_data_context: bool = False,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """FileDataContext constructor

        Args:
            project_config (DataContextConfig):  Config for current DataContext
            context_root_dir (Optional[str]): location to look for the ``great_expectations.yml`` file. If None,
                searches for the file based on conventions for project subdirectories.
            from_base_data_context (bool): Flag to ensure that `_BridgeFileDataContext` is not instantiated on its own.
                The only path that we support instantiating is through BaseDataContext.
                Will give a DataContextError otherwise.
            runtime_environment (Optional[dict]): a dictionary of config variables that override both those set in
                config_variables.yml and the environment
        """
        if not from_base_data_context:
            raise ge_exceptions.DataContextError(
                "_BridgeFileDataContext should not be instantiated on it's own"
            )

        self._context_root_directory = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        self._variables: FileDataContextVariables = self._init_variables()
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

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: Optional[bool] = None,
        **kwargs: Optional[dict],
    ) -> None:
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: The suite to save.
            expectation_suite_name: The name of this Expectation Suite. If no name is provided, the name will be read
                from the suite.
            overwrite_existing: Whether to overwrite the suite if it already exists.
            include_rendered_content: Whether to save the prescriptive rendered content for each expectation.

        Returns:
            None
        """
        if expectation_suite_name is None:
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite.expectation_suite_name
            )
        else:
            expectation_suite.expectation_suite_name = expectation_suite_name
            key = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if (
            self.expectations_store.has_key(key)  # noqa: W601
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        self._evaluation_parameter_dependencies_compiled = False
        include_rendered_content = (
            self._determine_if_expectation_suite_include_rendered_content(
                include_rendered_content=include_rendered_content
            )
        )
        if include_rendered_content:
            expectation_suite.render()
        return self.expectations_store.set(key, expectation_suite, **kwargs)  # type: ignore[arg-type]

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
            data_context=self,  # type: ignore[arg-type]
        )
        return variables
