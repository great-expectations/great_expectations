import logging
from typing import Any, Callable, Dict, List, Mapping, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import Batch, BatchRequestBase
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.usage_statistics import (
    get_batch_list_usage_statistics,
    usage_statistics_enabled_method,
)
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    EphemeralDataContextVariables,
)
from great_expectations.data_context.types.base import DataContextConfig
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
        project_config: Union[DataContextConfig, Mapping],
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
        self._config_variables: dict = self._load_config_variables()
        self._variables: EphemeralDataContextVariables = self._init_variables()
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> EphemeralDataContextVariables:
        variables = EphemeralDataContextVariables(
            config=self._project_config,
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
        )
        self._datasource_store = datasource_store

    def save_expectation_suite(  # type: ignore[override]
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        include_rendered_content: bool = False,
        **kwargs: Dict[str, Any],
    ):
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
        if include_rendered_content:
            expectation_suite.render()
        return self.expectations_store.set(key, expectation_suite, **kwargs)

    @usage_statistics_enabled_method(
        event_name=UsageStatsEvents.DATA_CONTEXT_GET_BATCH_LIST.value,
        args_payload_fn=get_batch_list_usage_statistics,
    )
    def get_batch_list(
        self,
        datasource_name: Optional[str] = None,
        data_connector_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        *,
        batch_request: Optional[BatchRequestBase] = None,
        batch_data: Optional[Any] = None,
        data_connector_query: Optional[dict] = None,
        batch_identifiers: Optional[dict] = None,
        limit: Optional[int] = None,
        index: Optional[Union[int, list, tuple, slice, str]] = None,
        custom_filter_function: Optional[Callable] = None,
        sampling_method: Optional[str] = None,
        sampling_kwargs: Optional[dict] = None,
        splitter_method: Optional[str] = None,
        splitter_kwargs: Optional[dict] = None,
        runtime_parameters: Optional[dict] = None,
        query: Optional[str] = None,
        path: Optional[str] = None,
        batch_filter_parameters: Optional[dict] = None,
        batch_spec_passthrough: Optional[dict] = None,
        **kwargs,
    ) -> List[Batch]:
        """Get the list of zero or more batches, based on a variety of flexible input types.
        This method applies only to the new (V3) Datasource schema.

        Args:
            batch_request

            datasource_name
            data_connector_name
            data_asset_name

            batch_request
            batch_data
            query
            path
            runtime_parameters
            data_connector_query
            batch_identifiers
            batch_filter_parameters

            limit
            index
            custom_filter_function

            sampling_method
            sampling_kwargs

            splitter_method
            splitter_kwargs

            batch_spec_passthrough

            **kwargs

        Returns:
            (Batch) The requested batch

        `get_batch` is the main user-facing API for getting batches.
        In contrast to virtually all other methods in the class, it does not require typed or nested inputs.
        Instead, this method is intended to help the user pick the right parameters

        This method attempts to return any number of batches, including an empty list.
        """
        return super().get_batch_list(
            datasource_name=datasource_name,
            data_connector_name=data_connector_name,
            data_asset_name=data_asset_name,
            batch_request=batch_request,
            batch_data=batch_data,
            data_connector_query=data_connector_query,
            batch_identifiers=batch_identifiers,
            limit=limit,
            index=index,
            custom_filter_function=custom_filter_function,
            sampling_method=sampling_method,
            sampling_kwargs=sampling_kwargs,
            splitter_method=splitter_method,
            splitter_kwargs=splitter_kwargs,
            runtime_parameters=runtime_parameters,
            query=query,
            path=path,
            batch_filter_parameters=batch_filter_parameters,
            batch_spec_passthrough=batch_spec_passthrough,
            **kwargs,
        )
