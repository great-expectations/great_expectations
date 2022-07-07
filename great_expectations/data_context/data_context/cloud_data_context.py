import logging
from typing import List, Mapping, Optional, Union, cast

import great_expectations.exceptions as ge_exceptions
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context_variables import (
    CloudDataContextVariables,
)
from great_expectations.data_context.types.base import (
    DEFAULT_USAGE_STATISTICS_URL,
    DataContextConfig,
    DataContextConfigDefaults,
    GeCloudConfig,
)
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.data_context.util import substitute_all_config_variables

logger = logging.getLogger(__name__)


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        context_root_dir: str,
        ge_cloud_config: GeCloudConfig,
        runtime_environment: Optional[dict] = None,
    ) -> None:
        """
        CloudDataContext constructor

        Args:
            project_config (DataContextConfig): config for CloudDataContext
            runtime_environment (dict):  a dictionary of config variables that override both those set in
                config_variables.yml and the environment
            ge_cloud_config (GeCloudConfig): GeCloudConfig corresponding to current CloudDataContext
        """
        self._ge_cloud_mode = True  # property needed for backward compatibility
        self._ge_cloud_config = ge_cloud_config
        self._context_root_directory = context_root_dir
        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )
        super().__init__(
            runtime_environment=runtime_environment,
        )

    def list_expectation_suite_names(self) -> List[str]:
        """
        Lists the available expectation suite names. If in ge_cloud_mode, a list of
        GE Cloud ids is returned instead.
        """
        return [suite_key.ge_cloud_id for suite_key in self.list_expectation_suites()]

    @property
    def ge_cloud_config(self) -> Optional[GeCloudConfig]:
        return self._ge_cloud_config

    @property
    def ge_cloud_mode(self) -> bool:
        return self._ge_cloud_mode

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """

        # if in ge_cloud_mode, use ge_cloud_organization_id
        return self.ge_cloud_config.organization_id

    def _save_project_config(self) -> None:
        """Save the current project to disk."""
        logger.debug(
            "ge_cloud_mode detected - skipping DataContext._save_project_config"
        )
        return None

    def get_config_with_variables_substituted(
        self, config: Optional[DataContextConfig] = None
    ) -> DataContextConfig:
        """
        Substitute vars in config of form ${var} or $(var) with values found in the following places,
        in order of precedence: ge_cloud_config (for Data Contexts in GE Cloud mode), runtime_environment,
        environment variables, config_variables, or ge_cloud_config_variable_defaults (allows certain variables to
        be optional in GE Cloud mode).
        """
        if not config:
            config = self.config

        substitutions: dict = self._determine_substitutions()

        ge_cloud_config_variable_defaults = {
            "plugins_directory": self._normalize_absolute_or_relative_path(
                path=DataContextConfigDefaults.DEFAULT_PLUGINS_DIRECTORY.value
            ),
            "usage_statistics_url": DEFAULT_USAGE_STATISTICS_URL,
        }
        for config_variable, value in ge_cloud_config_variable_defaults.items():
            if substitutions.get(config_variable) is None:
                logger.info(
                    f'Config variable "{config_variable}" was not found in environment or global config ('
                    f'{self.GLOBAL_CONFIG_PATHS}). Using default value "{value}" instead. If you would '
                    f"like to "
                    f"use a different value, please specify it in an environment variable or in a "
                    f"great_expectations.conf file located at one of the above paths, in a section named "
                    f'"ge_cloud_config".'
                )
                substitutions[config_variable] = value

        return DataContextConfig(
            **substitute_all_config_variables(
                config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
            )
        )

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite
        Returns:
            None
        """
        key: GeCloudIdentifier = GeCloudIdentifier(
            resource_type="expectation_suite",
            ge_cloud_id=ge_cloud_id
            if ge_cloud_id is not None
            else str(expectation_suite.ge_cloud_id),
        )
        if self.expectations_store.has_key(key) and not overwrite_existing:
            raise ge_exceptions.DataContextError(
                f"expectation_suite with GE Cloud ID {ge_cloud_id} already exists. "
                f"If you would like to overwrite this expectation_suite, set overwrite_existing=True."
            )
        self._evaluation_parameter_dependencies_compiled = False
        self.expectations_store.set(key, expectation_suite, **kwargs)

    def get_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ) -> ExpectationSuite:
        """Get an Expectation Suite by name or GE Cloud ID
        Args:
            expectation_suite_name (str): the name for the Expectation Suite
            ge_cloud_id (str): the GE Cloud ID for the Expectation Suite

        Returns:
            expectation_suite
        """
        key: GeCloudIdentifier = GeCloudIdentifier(
            resource_type="expectation_suite", ge_cloud_id=ge_cloud_id
        )
        if self.expectations_store.has_key(key):
            expectations_schema_dict: dict = cast(
                dict, self.expectations_store.get(key)
            )
            # create the ExpectationSuite from constructor
            return ExpectationSuite(**expectations_schema_dict, data_context=self)

        else:
            raise ge_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        ge_cloud_id: Optional[str] = None,
        **kwargs,
    ) -> ExpectationSuite:
        """Build a new expectation suite and save it into the data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create
            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name
                already exists.
            ge_cloud_id (str): the GE Cloud ID for the Expectation Suite

        Returns:
            A new (empty) expectation suite.
        """
        if not isinstance(overwrite_existing, bool):
            raise ValueError("Parameter overwrite_existing must be of type BOOL")

        expectation_suite: ExpectationSuite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name, data_context=self
        )
        key: GeCloudIdentifier = GeCloudIdentifier(
            resource_type="expectation_suite", ge_cloud_id=ge_cloud_id
        )
        if self.expectations_store.has_key(key) and not overwrite_existing:
            raise ge_exceptions.DataContextError(
                "expectation_suite with GE Cloud ID {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(ge_cloud_id)
            )
        self.expectations_store.set(key, expectation_suite, **kwargs)
        return expectation_suite

    @property
    def root_directory(self) -> Optional[str]:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.

        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it

        """
        return self._context_root_directory
