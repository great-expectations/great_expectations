from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilePathDataConnector,
)


class ConfiguredAssetAzureDataConnector(ConfiguredAssetFilePathDataConnector):
    def __init__(self):
        pass

    def build_batch_spec(self):
        pass

    def _get_data_reference_list_for_asset(self):
        pass

    def _get_full_file_path(self):
        pass
