import logging
from typing import Mapping, Optional, Union

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
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
        self._config_variables = self._load_config_variables()
        super().__init__(runtime_environment=runtime_environment)

    def _init_variables(self) -> EphemeralDataContextVariables:
        raise NotImplementedError

    def _save_project_config_to_disk(self) -> None:
        """Since EphemeralDataContext does not have config as a file, display logging message instead"""
        raise ge_exceptions.DataContextError(
            "EphemeralDataContext has been asked to save project_config to file,"
            "which is illegal. Please check your config and try again."
        )

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        **kwargs,
    ):
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite

            overwrite_existing: bool setting whether to overwrite existing ExpectationSuite

        Returns:
            None
        """
        if expectation_suite_name is None:
            key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite.expectation_suite_name
            )
        else:
            expectation_suite.expectation_suite_name = expectation_suite_name
            key: ExpectationSuiteIdentifier = ExpectationSuiteIdentifier(
                expectation_suite_name=expectation_suite_name
            )
        if self.expectations_store.has_key(key) and not overwrite_existing:
            raise ge_exceptions.DataContextError(
                "expectation_suite with name {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(
                    expectation_suite_name
                )
            )
        self._evaluation_parameter_dependencies_compiled = False
        return self.expectations_store.set(key, expectation_suite, **kwargs)
