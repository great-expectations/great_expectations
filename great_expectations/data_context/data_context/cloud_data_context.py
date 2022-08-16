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
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import (
    DEFAULT_USAGE_STATISTICS_URL,
    DataContextConfig,
    DataContextConfigDefaults,
    DatasourceConfig,
    GeCloudConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.refs import GeCloudResourceRef
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier
from great_expectations.data_context.util import substitute_all_config_variables
from great_expectations.datasource import Datasource

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
        self._variables = self._init_variables()
        super().__init__(
            runtime_environment=runtime_environment,
        )

    def _init_datasource_store(self) -> None:
        from great_expectations.data_context.store.datasource_store import (
            DatasourceStore,
        )

        store_name: str = "datasource_store"  # Never explicitly referenced but adheres
        # to the convention set by other internal Stores
        store_backend: dict = {"class_name": "GeCloudStoreBackend"}
        runtime_environment: dict = {
            "root_directory": self.root_directory,
            "ge_cloud_credentials": self.ge_cloud_config.to_dict(),
            "ge_cloud_resource_type": GeCloudRESTResource.DATASOURCE,
            "ge_cloud_base_url": self.ge_cloud_config.base_url,
        }

        datasource_store = DatasourceStore(
            store_name=store_name,
            store_backend=store_backend,
            runtime_environment=runtime_environment,
        )
        self._datasource_store = datasource_store

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
        ge_cloud_base_url: str = self._ge_cloud_config.base_url
        ge_cloud_organization_id: str = self._ge_cloud_config.organization_id
        ge_cloud_access_token: str = self._ge_cloud_config.access_token

        variables = CloudDataContextVariables(
            config=self._project_config,
            ge_cloud_base_url=ge_cloud_base_url,
            ge_cloud_organization_id=ge_cloud_organization_id,
            ge_cloud_access_token=ge_cloud_access_token,
        )
        return variables

    def _construct_data_context_id(self) -> str:
        """
        Choose the id of the currently-configured expectations store, if available and a persistent store.
        If not, it should choose the id stored in DataContextConfig.
        Returns:
            UUID to use as the data_context_id
        """

        # if in ge_cloud_mode, use ge_cloud_organization_id
        return self.ge_cloud_config.organization_id

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

    def create_expectation_suite(
        self,
        expectation_suite_name: str,
        overwrite_existing: bool = False,
        ge_cloud_id: Optional[str] = None,
        **kwargs: Optional[dict],
    ) -> ExpectationSuite:
        """Build a new expectation suite and save it into the data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create
            overwrite_existing (boolean): Whether to overwrite expectation suite if expectation suite with given name
                already exists.

        Returns:
            A new (empty) expectation suite.
        """
        if not isinstance(overwrite_existing, bool):
            raise ValueError("Parameter overwrite_existing must be of type BOOL")

        expectation_suite = ExpectationSuite(
            expectation_suite_name=expectation_suite_name, data_context=self
        )
        key = GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=ge_cloud_id,
        )
        if (
            self.expectations_store.has_key(key)  # noqa: W601
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                "expectation_suite with GE Cloud ID {} already exists. If you would like to overwrite this "
                "expectation_suite, set overwrite_existing=True.".format(ge_cloud_id)
            )
        self.expectations_store.set(key, expectation_suite, **kwargs)
        return expectation_suite

    def delete_expectation_suite(
        self,
        expectation_suite_name: Optional[str] = None,
        ge_cloud_id: Optional[str] = None,
    ):
        """Delete specified expectation suite from data_context expectation store.

        Args:
            expectation_suite_name: The name of the expectation_suite to create

        Returns:
            True for Success and False for Failure.
        """
        key = GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=ge_cloud_id,
        )
        if not self.expectations_store.has_key(key):  # noqa: W601
            raise ge_exceptions.DataContextError(
                f"expectation_suite with id {ge_cloud_id} does not exist."
            )
        else:
            self.expectations_store.remove_key(key)
            return True

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
        key = GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=ge_cloud_id,
        )
        if self.expectations_store.has_key(key):  # noqa: W601
            expectations_schema_dict: dict = cast(
                dict, self.expectations_store.get(key)
            )
            # create the ExpectationSuite from constructor
            return ExpectationSuite(**expectations_schema_dict, data_context=self)

        else:
            raise ge_exceptions.DataContextError(
                f"expectation_suite {expectation_suite_name} not found"
            )

    def save_expectation_suite(
        self,
        expectation_suite: ExpectationSuite,
        expectation_suite_name: Optional[str] = None,
        overwrite_existing: bool = True,
        ge_cloud_id: Optional[str] = None,
        **kwargs: Optional[dict],
    ) -> None:
        """Save the provided expectation suite into the DataContext.

        Args:
            expectation_suite: the suite to save
            expectation_suite_name: the name of this expectation suite. If no name is provided the name will \
                be read from the suite
            ge_cloud_id: cloud id for saving expectation suite
            overwrite_existing: should I over-write the Suite if it already exists?
        Returns:
            None
        """
        key = GeCloudIdentifier(
            resource_type=GeCloudRESTResource.EXPECTATION_SUITE,
            ge_cloud_id=ge_cloud_id
            if ge_cloud_id is not None
            else str(expectation_suite.ge_cloud_id),
        )
        if (
            self.expectations_store.has_key(key)  # noqa: W601
            and not overwrite_existing
        ):
            raise ge_exceptions.DataContextError(
                f"expectation_suite with GE Cloud ID {ge_cloud_id} already exists. "
                f"If you would like to overwrite this expectation_suite, set overwrite_existing=True."
            )
        self._evaluation_parameter_dependencies_compiled = False
        self.expectations_store.set(key, expectation_suite, **kwargs)

    @property
    def root_directory(self) -> Optional[str]:
        """The root directory for configuration objects in the data context; the location in which
        ``great_expectations.yml`` is located.

        Why does this exist in AbstractDataContext? CloudDataContext and FileDataContext both use it

        """
        return self._context_root_directory

    def _instantiate_datasource_from_config_and_update_project_config(
        self,
        name: str,
        config: dict,
        initialize: bool = True,
        save_changes: bool = False,
    ) -> Optional[Datasource]:
        """Instantiate datasource and optionally persist datasource config to store and/or initialize datasource for use.

        Args:
            name: Desired name for the datasource.
            config: Config for the datasource.
            initialize: Whether to initialize the datasource or return None.
            save_changes: Whether to save the datasource config to the configured Datasource store.

        Returns:
            If initialize=True return an instantiated Datasource object, else None.
        """

        datasource_config: DatasourceConfig = datasourceConfigSchema.load(config)

        if save_changes:

            datasource_config["name"] = name
            resource_ref: GeCloudResourceRef = self._datasource_store.create(
                datasource_config
            )
            datasource_config.id_ = resource_ref.ge_cloud_id

        self.config.datasources[name] = datasource_config

        # Config must be persisted with ${VARIABLES} syntax but hydrated at time of use
        substitutions: dict = self._determine_substitutions()
        config: dict = dict(datasourceConfigSchema.dump(datasource_config))

        substituted_config_dict: dict = substitute_all_config_variables(
            config, substitutions, self.DOLLAR_SIGN_ESCAPE_STRING
        )

        # Round trip through schema validation and config creation to ensure "id_" is present
        #
        # Chetan - 20220804 - This logic is utilized with other id-enabled objects and should
        # be refactored to into the config/schema. Also, downstream methods should be refactored
        # to accept the config object (as opposed to a dict).
        substituted_config = DatasourceConfig(
            **datasourceConfigSchema.load(substituted_config_dict)
        )
        schema_validated_substituted_config_dict = substituted_config.to_json_dict()

        datasource: Optional[Datasource] = None
        if initialize:
            try:
                datasource = self._instantiate_datasource_from_config(
                    name=name, config=schema_validated_substituted_config_dict
                )
                self._cached_datasources[name] = datasource
            except ge_exceptions.DatasourceInitializationError as e:
                # Do not keep configuration that could not be instantiated.
                if save_changes:
                    self._datasource_store.delete(datasource_config)
                # If the DatasourceStore uses an InlineStoreBackend, the config may already be updated
                self.config.datasources.pop(name, None)
                raise e

        return datasource

    def add_checkpoint(
        self,
        name: str,
        config_version: Optional[Union[int, float]] = None,
        template_name: Optional[str] = None,
        module_name: Optional[str] = None,
        class_name: Optional[str] = None,
        run_name_template: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        batch_request: Optional[dict] = None,
        action_list: Optional[List[dict]] = None,
        evaluation_parameters: Optional[dict] = None,
        runtime_configuration: Optional[dict] = None,
        validations: Optional[List[dict]] = None,
        profilers: Optional[List[dict]] = None,
        # Next two fields are for LegacyCheckpoint configuration
        validation_operator_name: Optional[str] = None,
        batches: Optional[List[dict]] = None,
        # the following four arguments are used by SimpleCheckpoint
        site_names: Optional[Union[str, List[str]]] = None,
        slack_webhook: Optional[str] = None,
        notify_on: Optional[str] = None,
        notify_with: Optional[Union[str, List[str]]] = None,
        ge_cloud_id: Optional[str] = None,
        expectation_suite_ge_cloud_id: Optional[str] = None,
        default_validation_id: Optional[str] = None,
    ) -> "Checkpoint":  # noqa: F821
        """
        See `AbstractDataContext.add_checkpoint` for more information.
        """

        from great_expectations.checkpoint.checkpoint import Checkpoint

        checkpoint: Checkpoint = Checkpoint.construct_from_config_args(
            data_context=self,
            checkpoint_store_name=self.checkpoint_store_name,
            name=name,
            config_version=config_version,
            template_name=template_name,
            module_name=module_name,
            class_name=class_name,
            run_name_template=run_name_template,
            expectation_suite_name=expectation_suite_name,
            batch_request=batch_request,
            action_list=action_list,
            evaluation_parameters=evaluation_parameters,
            runtime_configuration=runtime_configuration,
            validations=validations,
            profilers=profilers,
            # Next two fields are for LegacyCheckpoint configuration
            validation_operator_name=validation_operator_name,
            batches=batches,
            # the following four arguments are used by SimpleCheckpoint
            site_names=site_names,
            slack_webhook=slack_webhook,
            notify_on=notify_on,
            notify_with=notify_with,
            ge_cloud_id=ge_cloud_id,
            expectation_suite_ge_cloud_id=expectation_suite_ge_cloud_id,
            default_validation_id=default_validation_id,
        )

        checkpoint_config = self.checkpoint_store.create(
            checkpoint_config=checkpoint.config
        )

        checkpoint = Checkpoint.instantiate_from_config_with_runtime_args(
            checkpoint_config=checkpoint_config, data_context=self
        )
        return checkpoint
