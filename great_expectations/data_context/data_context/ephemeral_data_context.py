import logging
from typing import Optional

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.serializer import DictConfigSerializer
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    EphemeralDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
)

logger = logging.getLogger(__name__)


class EphemeralDataContext(AbstractDataContext):
    """
    Will contain functionality to create DataContext at runtime (ie. passed in config object or from stores). Users will
    be able to use EphemeralDataContext for having a temporary or in-memory DataContext

    TODO: Most of the BaseDataContext code will be migrated to this class, which will continue to exist for backwards
    compatibility reasons.
    """

    def __init__(
        self,
        project_config: DataContextConfig,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """EphemeralDataContext constructor

        project_config: config for in-memory EphemeralDataContext
        runtime_environment: a dictionary of config variables tha
                override both those set in config_variables.yml and the environment

        """
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> EphemeralDataContextVariables:
        variables = EphemeralDataContextVariables(
            config=self._project_config,
            config_provider=self.config_provider,
        )
        return variables

    def _init_datasource_store(self) -> None:
        from great_expectations.data_context.store.datasource_store import (
            DatasourceStore,
        )

        store_name: str = "datasource_store"  # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_backend: dict = {"class_name": "InMemoryStoreBackend"}

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            serializer=DictConfigSerializer(schema=datasourceConfigSchema),
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
            self.expectations_store.has_key(key)  # noqa: @601
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
        return self.expectations_store.set(key, expectation_suite, **kwargs)
