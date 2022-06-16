import logging
from typing import Mapping, Optional, Union

from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.types.base import DataContextConfig, GeCloudConfig
from great_expectations.data_context.types.data_context_variables import (
    CloudDataContextVariables,
)

logger = logging.getLogger(__name__)


class CloudDataContext(AbstractDataContext):
    """
    Subclass of AbstractDataContext that contains functionality necessary to hydrate state from cloud
    """

    def __init__(
        self,
        project_config: Union[DataContextConfig, Mapping],
        runtime_environment: Optional[dict] = None,
        ge_cloud_mode: bool = False,
        ge_cloud_config: Optional[GeCloudConfig] = None,
    ) -> None:
        self._ge_cloud_mode = ge_cloud_mode
        self._ge_cloud_config = ge_cloud_config
        self.runtime_environment = runtime_environment or {}

        self._project_config = self._apply_global_config_overrides(
            config=project_config
        )

    # def _apply_global_config_overrides(
    #     self, config: DataContextConfig
    # ) -> DataContextConfig:
    #     """ """
    #     # check for global usage_statistics opt out
    #     validation_errors: dict = {}
    #     config_with_global_config_overrides: DataContextConfig = copy.deepcopy(config)
    #
    #     if self._check_global_usage_statistics_opt_out():
    #         logger.info(
    #             "Usage statistics is disabled globally. Applying override to project_config."
    #         )
    #         config_with_global_config_overrides.anonymous_usage_statistics.enabled = (
    #             False
    #         )
    #
    #     # check for global data_context_id
    #     global_data_context_id = self._get_global_config_value(
    #         environment_variable="GE_DATA_CONTEXT_ID",
    #         conf_file_section="anonymous_usage_statistics",  # i want to keep this for now
    #         conf_file_option="data_context_id",  # same with this
    #     )
    #     if global_data_context_id:
    #         data_context_id_errors = anonymizedUsageStatisticsSchema.validate(
    #             {"data_context_id": global_data_context_id}
    #         )
    #         if not data_context_id_errors:
    #             logger.info(
    #                 "data_context_id is defined globally. Applying override to project_config."
    #             )
    #             config_with_global_config_overrides.anonymous_usage_statistics.data_context_id = (
    #                 global_data_context_id
    #             )
    #         else:
    #             validation_errors.update(data_context_id_errors)
    #     # check for global usage_statistics url
    #     global_usage_statistics_url = self._get_global_config_value(
    #         environment_variable="GE_USAGE_STATISTICS_URL",
    #         conf_file_section="anonymous_usage_statistics",
    #         conf_file_option="usage_statistics_url",
    #     )
    #     if global_usage_statistics_url:
    #         usage_statistics_url_errors = anonymizedUsageStatisticsSchema.validate(
    #             {"usage_statistics_url": global_usage_statistics_url}
    #         )
    #         if not usage_statistics_url_errors:
    #             logger.info(
    #                 "usage_statistics_url is defined globally. Applying override to project_config."
    #             )
    #             config_with_global_config_overrides.anonymous_usage_statistics.usage_statistics_url = (
    #                 global_usage_statistics_url
    #             )
    #         else:
    #             validation_errors.update(usage_statistics_url_errors)
    #     if validation_errors:
    #         logger.warning(
    #             "The following globally-defined config variables failed validation:\n{}\n\n"
    #             "Please fix the variables if you would like to apply global values to project_config.".format(
    #                 json.dumps(validation_errors, indent=2)
    #             )
    #         )
    #     return config_with_global_config_overrides

    def _init_variables(self) -> CloudDataContextVariables:
        raise NotImplementedError
